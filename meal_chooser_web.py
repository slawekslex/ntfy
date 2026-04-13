#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import re
import sys
import time
import unicodedata
from collections import defaultdict
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock, local
from typing import Dict, Iterator, List, Tuple
from urllib.parse import urlparse

import requests

from flask import Flask, Response, abort, jsonify, redirect, render_template, request, session, url_for

from ntfy_meals_lib import (
    DELIVERY_EXPANSIONS,
    NtfyClient,
    apply_optimal_plan_via_api,
    build_rows_by_meal,
    build_chooser_payload,
    choose_delivery_diet_id,
    choices_from_indices,
    fetch_delivery_context,
    fetch_image_bytes,
    load_json_config,
    load_env_file,
    nutrition_aggregates_by_name,
    read_caps_from_config,
    validate_date,
)

_log = logging.getLogger(__name__)


def _resolve_log_level() -> int:
    raw_level = (os.environ.get("MEAL_CHOOSER_LOG_LEVEL") or "INFO").strip().upper()
    return getattr(logging, raw_level, logging.INFO)


def _configure_logging() -> int:
    """Configure app logging so INFO progress logs are visible by default."""
    level = _resolve_log_level()
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
    else:
        root_logger.setLevel(level)
    logging.getLogger(__name__).setLevel(level)
    logging.getLogger("lifekid_menu").setLevel(level)
    return level


def _timing_enabled() -> bool:
    return (os.environ.get("MEAL_CHOOSER_TIMING") or "").strip().lower() in ("1", "true", "yes")


class TimingAgg:
    """Thread-safe per-request timing totals (enable with MEAL_CHOOSER_TIMING=1)."""

    __slots__ = ("_lock", "_totals", "_counts")

    def __init__(self) -> None:
        self._lock = Lock()
        self._totals: dict[str, float] = defaultdict(float)
        self._counts: dict[str, int] = defaultdict(int)

    def add(self, label: str, seconds: float) -> None:
        with self._lock:
            self._totals[label] += seconds
            self._counts[label] += 1

    def emit(self, *, prefix: str) -> None:
        if not self._totals:
            return
        lines = sorted(self._totals.items(), key=lambda kv: kv[1], reverse=True)
        parts = [f"{lab} {sec * 1000:.1f}ms total (n={self._counts[lab]})" for lab, sec in lines]
        line = (
            f"{prefix}: {' | '.join(parts)} "
            "(per-label totals sum worker time; compare nela.route.thread_pool for wall clock)"
        )
        if _log.isEnabledFor(logging.INFO):
            _log.info("%s", line)
        else:
            print(line, file=sys.stderr, flush=True)


@contextmanager
def _time_block(label: str, agg: TimingAgg | None) -> Iterator[None]:
    if agg is None:
        yield
        return
    start = time.perf_counter()
    try:
        yield
    finally:
        agg.add(label, time.perf_counter() - start)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the NTFY meal chooser web UI.")
    parser.add_argument("--date", required=True, help="Date in format YYYY-MM-DD (e.g. 2026-04-11)")
    parser.add_argument("--diet-name", default="Slex", help="Diet display name prefix, default: Slex")
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to JSON config file with protein_cap_g and fiber_cap_g.",
    )
    parser.add_argument(
        "--cookies",
        default=None,
        help="Raw cookie header string. If omitted, NTFY_COOKIES env var is used.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind the local web server to.")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind the local web server to.")
    return parser.parse_args()


def selected_name_from_rows(rows: List[dict]) -> str | None:
    for row in rows:
        name = row.get("name")
        if row.get("selected") and name:
            return str(name)
    for row in rows:
        name = row.get("name")
        if name:
            return str(name)
    return None


def nela_obiad_name(delivery_payload: dict | None) -> str | None:
    if not delivery_payload or not delivery_payload.get("results"):
        return None
    _, rows_by_meal = build_rows_by_meal(delivery_payload)
    for meal_label, rows in rows_by_meal.items():
        if meal_label.lower().startswith("obiad"):
            return selected_name_from_rows(rows)
    return None


def select_page_first_meal_name(delivery_payload: dict | None) -> str | None:
    if not delivery_payload or not delivery_payload.get("results"):
        return None

    includes = delivery_payload.get("includes", {})
    products = {product["id"]: product for product in includes.get("simple_products", [])}
    for item in includes.get("delivery_items", []):
        if item.get("related_item_type") != "ITEM":
            continue
        if not item.get("is_simple_product_selected_by_user"):
            continue
        product_name = (products.get(item.get("simple_product_id")) or {}).get("name")
        if product_name:
            return str(product_name)
    return None


def select_first_meal_name(delivery_payload: dict | None) -> str | None:
    if not delivery_payload or not delivery_payload.get("results"):
        return None
    _, rows_by_meal = build_rows_by_meal(delivery_payload)
    for rows in rows_by_meal.values():
        name = selected_name_from_rows(rows)
        if name:
            return name
    return None


def obiad_meal_from_rows_by_meal(rows_by_meal: Dict[str, List[dict]]) -> tuple[str | None, List[dict]]:
    for meal_label, rows in rows_by_meal.items():
        if meal_label.lower().startswith("obiad") and rows:
            return meal_label, rows
    return None, []


_PL_WEEKDAYS = (
    "poniedziałek",
    "wtorek",
    "środa",
    "czwartek",
    "piątek",
    "sobota",
    "niedziela",
)


def polish_weekday_and_display_date(date_str: str) -> tuple[str, str] | None:
    try:
        validate_date(date_str)
    except ValueError:
        return None
    day = datetime.strptime(date_str, "%Y-%m-%d").date()
    return _PL_WEEKDAYS[day.weekday()], day.strftime("%d.%m.%Y")


def nela_livekid_favourite_product_id(date_str: str) -> str:
    """Synthetic id for LiveKid cards on nela_opcje so favourites API can target them (per viewed date)."""
    return f"livekid:{date_str}"


def nela_default_start_date_str() -> str:
    """First day of the /nela window when ?start= is omitted (local server calendar date)."""
    return datetime.now().date().isoformat()


def nela_favourites_file_path() -> Path:
    return Path(__file__).resolve().parent / ".data" / "nela_meal_favourites.json"


# Fuzzy meal-name match: NTFY simple_product_id is usually stable for a catalog SKU, but names are
# the durable fallback if ids rotate or duplicate. LLM matching is avoided here (latency, cost).
_NELA_FAV_NAME_FUZZY_MIN_LEN = 14
_NELA_FAV_NAME_FUZZY_RATIO = 0.9


def normalize_meal_name_for_match(name: str) -> str:
    text = unicodedata.normalize("NFKC", name or "")
    text = text.casefold()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def meal_names_fuzzy_match(a: str, b: str) -> bool:
    na = normalize_meal_name_for_match(a)
    nb = normalize_meal_name_for_match(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    if min(len(na), len(nb)) < _NELA_FAV_NAME_FUZZY_MIN_LEN:
        return False
    return SequenceMatcher(None, na, nb).ratio() >= _NELA_FAV_NAME_FUZZY_RATIO


def load_nela_favourite_entries(path: Path) -> List[dict]:
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    version = raw.get("version", 1)
    if version == 1:
        ids = raw.get("simple_product_ids")
        if not isinstance(ids, list):
            return []
        return [
            {"simple_product_id": str(x), "meal_name": ""}
            for x in ids
            if x is not None and str(x).strip() != ""
        ]
    favourites = raw.get("favourites")
    if not isinstance(favourites, list):
        return []
    out: List[dict] = []
    for item in favourites:
        if not isinstance(item, dict):
            continue
        pid_raw = item.get("simple_product_id")
        pid = str(pid_raw).strip() if pid_raw is not None else ""
        mname = str(item.get("meal_name") or "").strip()
        if pid or mname:
            out.append({"simple_product_id": pid, "meal_name": mname})
    return out


def save_nela_favourite_entries(path: Path, entries: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 2,
        "favourites": sorted(
            entries,
            key=lambda e: (e.get("meal_name") or "", e.get("simple_product_id") or ""),
        ),
    }
    tmp_path = path.parent / f"{path.name}.tmp"
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def nela_meal_favourite_matches(opt: dict, entries: List[dict]) -> bool:
    pid = (opt.get("simple_product_id") or "").strip() or None
    name = (opt.get("name") or "").strip()
    for entry in entries:
        eid = (entry.get("simple_product_id") or "").strip() or None
        ename = (entry.get("meal_name") or "").strip()
        if pid and eid and pid == eid:
            return True
        if name and ename and meal_names_fuzzy_match(ename, name):
            return True
    return False


def add_nela_favourite_entry(entries: List[dict], *, product_id: str, meal_name: str) -> None:
    product_id = (product_id or "").strip()
    meal_name = (meal_name or "").strip()
    entries[:] = [
        e
        for e in entries
        if (e.get("simple_product_id") or "").strip() != product_id
        and not (meal_name and meal_names_fuzzy_match(e.get("meal_name") or "", meal_name))
    ]
    entries.append({"simple_product_id": product_id, "meal_name": meal_name})


def remove_nela_favourite_entry(entries: List[dict], *, product_id: str, meal_name: str) -> None:
    product_id = (product_id or "").strip()
    meal_name = (meal_name or "").strip()
    before = len(entries)
    entries[:] = [e for e in entries if (e.get("simple_product_id") or "").strip() != product_id]
    if len(entries) < before:
        return
    if meal_name:
        entries[:] = [e for e in entries if not meal_names_fuzzy_match(e.get("meal_name") or "", meal_name)]


def load_nela_favourite_product_ids(path: Path) -> set[str]:
    """Backward-compatible helper: ids only (tests / diagnostics)."""
    return {e["simple_product_id"] for e in load_nela_favourite_entries(path) if e.get("simple_product_id")}


def livekid_kid_id(env_vars: Dict[str, str]) -> int | None:
    token = os.getenv("LIVEKID_BEARER_TOKEN") or env_vars.get("LIVEKID_BEARER_TOKEN")
    if not token:
        return None
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8")
        claims = json.loads(decoded)
    except (OSError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return None
    kid = claims.get("kid")
    if kid is None:
        return None
    try:
        return int(kid)
    except (TypeError, ValueError):
        return None


def livekid_has_obiad(presence_payload: dict | None) -> bool:
    if not presence_payload:
        return False
    day_value = str(presence_payload.get("day") or presence_payload.get("date") or "").strip()
    if day_value:
        try:
            if datetime.strptime(day_value, "%Y-%m-%d").weekday() >= 5:
                return False
        except ValueError:
            pass
    meals = presence_payload.get("meals") or []
    return any(str(meal.get("name") or "").strip().lower() == "obiad" for meal in meals)


def format_livekid_menu(menu_payload: dict | None) -> str:
    if not menu_payload or menu_payload.get("status") == "missing":
        return "Brak Menu"
    parts = [str(menu_payload.get("zupa") or "").strip(), str(menu_payload.get("drugie") or "").strip()]
    parts = [part for part in parts if part]
    return " | ".join(parts) if parts else "Brak Menu"


def livekid_meal_name_from_payloads(presence_payload: dict | None, menu_payload: dict | None) -> str | None:
    if not livekid_has_obiad(presence_payload):
        return None
    return format_livekid_menu(menu_payload)


def create_app(args: argparse.Namespace, *, nela_favourites_path: Path | None = None) -> Flask:
    app = Flask(__name__)
    configured_level = _configure_logging()
    app.logger.setLevel(configured_level)
    _log.info("Logging configured at level=%s", logging.getLevelName(configured_level))
    favourites_path = nela_favourites_path or nela_favourites_file_path()
    env_vars = load_env_file()
    auth_password = os.getenv("APP_PASSWORD") or env_vars.get("APP_PASSWORD") or os.getenv("PASSWORD") or env_vars.get("PASSWORD")
    if not auth_password:
        raise ValueError("Missing APP_PASSWORD or PASSWORD in .env for app login.")
    secret_source = os.getenv("FLASK_SECRET_KEY") or env_vars.get("FLASK_SECRET_KEY") or auth_password
    app.config.update(
        SECRET_KEY=hashlib.sha256(f"ntfy-auth:{secret_source}".encode("utf-8")).hexdigest(),
        PERMANENT_SESSION_LIFETIME=timedelta(days=36500),
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
    )
    image_cache: Dict[str, Tuple[bytes, str]] = {}
    chooser_cache: Dict[str, dict] = {}
    context_cache: Dict[str, dict] = {}
    livekid_cache: Dict[str, str | None] = {}
    ntfy_nela_cache: Dict[str, str] = {}
    cache_lock = Lock()
    nela_favourites_lock = Lock()
    nela_worker_state = local()

    def requested_path() -> str:
        query = request.query_string.decode("utf-8")
        return f"{request.path}?{query}" if query else request.path

    def safe_redirect_target(raw_target: str | None) -> str:
        if not raw_target:
            return url_for("overview")
        parsed = urlparse(raw_target)
        if parsed.scheme or parsed.netloc:
            return url_for("overview")
        if not raw_target.startswith("/"):
            return url_for("overview")
        return raw_target

    @app.before_request
    def require_login() -> Response | None:
        if request.endpoint in {"login", "login_post", "logout", "static"}:
            return None
        if session.get("authenticated"):
            session.permanent = True
            return None
        if request.path.startswith("/api/"):
            return jsonify({"error": "Authentication required."}), 401
        return redirect(url_for("login", next=requested_path()))

    @app.get("/login")
    def login() -> str | Response:
        if session.get("authenticated"):
            return redirect(safe_redirect_target(request.args.get("next")))
        return render_template("login.html", error_message=None, next_url=request.args.get("next", ""))

    @app.post("/login")
    def login_post() -> str | Response:
        submitted_password = request.form.get("password", "")
        next_url = request.form.get("next", "")
        if submitted_password != auth_password:
            return render_template(
                "login.html",
                error_message="Wrong password.",
                next_url=next_url,
            ), 401
        session.clear()
        session["authenticated"] = True
        session.permanent = True
        return redirect(safe_redirect_target(next_url))

    @app.post("/logout")
    def logout() -> Response:
        session.clear()
        return redirect(url_for("login"))

    def clear_all_caches() -> None:
        _log.info("NELA flow: clearing in-memory caches (keeping LiveKid disk caches).")
        with cache_lock:
            image_cache.clear()
            chooser_cache.clear()
            context_cache.clear()
            livekid_cache.clear()
            ntfy_nela_cache.clear()

    def with_image_urls(chooser_payload: dict) -> dict:
        for meal in chooser_payload["meals"]:
            for option in meal["options"]:
                image_id = option.get("imageId")
                option["imageUrl"] = f"/images/{image_id}" if image_id else None
        return chooser_payload

    def get_context_for_date(date_str: str) -> dict:
        cached = context_cache.get(date_str)
        if cached is not None:
            cached["client"].ensure_authenticated()
            return cached
        context = fetch_delivery_context(
            date=date_str,
            diet_name=args.diet_name,
            config_path=args.config,
            explicit_cookie_str=args.cookies,
        )
        context_cache[date_str] = context
        return context

    def invalidate_date_cache(date_str: str) -> None:
        context_cache.pop(date_str, None)
        chooser_keys = [key for key in chooser_cache if key.startswith(f"{date_str}|")]
        for key in chooser_keys:
            chooser_cache.pop(key, None)

    def parse_positive_target(raw_value: str | None, default_value: float, label: str) -> float:
        if raw_value is None or raw_value == "":
            return float(default_value)
        try:
            parsed = float(raw_value)
        except ValueError as exc:
            raise ValueError(f"{label} target must be a number.") from exc
        if parsed <= 0:
            raise ValueError(f"{label} target must be positive.")
        return parsed

    def get_chooser_payload_for_targets(date_str: str, protein_target_g: float, fiber_target_g: float) -> dict:
        cache_key = f"{date_str}|{protein_target_g:.3f}|{fiber_target_g:.3f}"
        cached = chooser_cache.get(cache_key)
        if cached is not None:
            return cached
        context = get_context_for_date(date_str)
        chooser_payload = build_chooser_payload(
            date=date_str,
            diet_name=args.diet_name,
            rows_by_meal=context["rows_by_meal"],
            protein_cap_g=protein_target_g,
            fiber_cap_g=fiber_target_g,
        )
        chooser_cache[cache_key] = with_image_urls(chooser_payload)
        return chooser_cache[cache_key]

    def adjacent_dates(date_str: str) -> tuple[str, str]:
        current = datetime.strptime(date_str, "%Y-%m-%d").date()
        return (
            (current - timedelta(days=1)).isoformat(),
            (current + timedelta(days=1)).isoformat(),
        )

    def fetch_delivery_payload_for_diet(client: NtfyClient, date_str: str, delivery_diet_id: int | None) -> dict | None:
        if delivery_diet_id is None:
            return None
        return client.get_data(
            path=f"users/{client.user_id}/deliveries",
            params={
                "date": date_str,
                "delivery_diet_id": str(delivery_diet_id),
                "status__in": "TO-BE-REALIZED,REALIZED",
                "aggregate_by__in": "nutritional_data:date",
                "expansions__in": DELIVERY_EXPANSIONS,
            },
        )

    def fetch_delivery_payload_for_date(client: NtfyClient, date_str: str) -> dict:
        return client.get_data(
            path=f"users/{client.user_id}/deliveries",
            params={
                "date": date_str,
                "status__in": "TO-BE-REALIZED,REALIZED",
                "aggregate_by__in": "nutritional_data:date",
                "expansions__in": DELIVERY_EXPANSIONS,
            },
        )

    def fetch_livekid_presence(date_str: str) -> dict | None:
        kid_id = livekid_kid_id(env_vars)
        token = os.getenv("LIVEKID_BEARER_TOKEN") or env_vars.get("LIVEKID_BEARER_TOKEN")
        if not kid_id or not token:
            return None
        response = requests.get(
            f"https://pl.api.api-livekid-prod.com/v1/presence/{date_str}/{kid_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "meal-chooser-web/1.0",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def fetch_livekid_menu(date_str: str) -> dict | None:
        from lifekid_menu import get_menu_for_day

        return get_menu_for_day(date_str, env_path=".env")

    def livekid_meal_name_for_day(
        date_str: str,
        timing_agg: TimingAgg | None,
        *,
        prefetched_presence: dict[str, dict | None] | None = None,
    ) -> str | None:
        with cache_lock:
            if date_str in livekid_cache:
                return livekid_cache[date_str]
        if prefetched_presence is not None and date_str in prefetched_presence:
            presence_payload = prefetched_presence[date_str]
        else:
            with _time_block("livekid.fetch_presence", timing_agg):
                presence_payload = fetch_livekid_presence(date_str)
        if not livekid_has_obiad(presence_payload):
            with cache_lock:
                livekid_cache[date_str] = None
            return None
        with _time_block("livekid.fetch_menu", timing_agg):
            menu_payload = fetch_livekid_menu(date_str)
        meal_name = livekid_meal_name_from_payloads(presence_payload, menu_payload)
        with cache_lock:
            livekid_cache[date_str] = meal_name
        return meal_name

    def ntfy_meal_name_for_day(
        client: NtfyClient,
        date_str: str,
        *,
        nela_delivery_diet_id: int | None,
        select_delivery_diet_id: int | None,
        timing_agg: TimingAgg | None,
    ) -> str:
        with cache_lock:
            cached = ntfy_nela_cache.get(date_str)
        if cached is not None:
            return cached

        with _time_block("ntfy.deliveries_nela_diet", timing_agg):
            nela_payload = fetch_delivery_payload_for_diet(client, date_str, nela_delivery_diet_id)
        with _time_block("ntfy.parse_nela_obiad", timing_agg):
            ntfy_meal = nela_obiad_name(nela_payload)
        if ntfy_meal is None and select_delivery_diet_id is not None:
            with _time_block("ntfy.deliveries_select_diet", timing_agg):
                select_payload = fetch_delivery_payload_for_diet(client, date_str, select_delivery_diet_id)
            with _time_block("ntfy.parse_select_first_meal", timing_agg):
                ntfy_meal = select_first_meal_name(select_payload)
        if ntfy_meal is None:
            with _time_block("ntfy.deliveries_by_date", timing_agg):
                date_payload = fetch_delivery_payload_for_date(client, date_str)
            with _time_block("ntfy.parse_select_page_first", timing_agg):
                ntfy_meal = select_page_first_meal_name(date_payload)

        meal_name = ntfy_meal or "-"
        with cache_lock:
            ntfy_nela_cache[date_str] = meal_name
        return meal_name

    def build_nela_overview_row(
        date_str: str,
        *,
        nela_delivery_diet_id: int | None,
        select_delivery_diet_id: int | None,
        cookie_str: str,
        timing_agg: TimingAgg | None,
        livekid_presence_by_date: dict[str, dict | None] | None = None,
    ) -> dict:
        def get_or_create_worker_ntfy_client() -> NtfyClient:
            cached_client = getattr(nela_worker_state, "ntfy_client", None)
            cached_cookie = getattr(nela_worker_state, "cookie_str", None)
            if cached_client is not None and cached_cookie == cookie_str:
                return cached_client
            with _time_block("nela_row.ntfy_client_init", timing_agg):
                new_client = NtfyClient(explicit_cookie_str=cookie_str)
            with _time_block("nela_row.ensure_authenticated", timing_agg):
                new_client.ensure_authenticated()
            nela_worker_state.ntfy_client = new_client
            nela_worker_state.cookie_str = cookie_str
            return new_client

        ntfy_meal_name = "-"
        livekid_meal_name = "-"
        errors = []
        try:
            client = get_or_create_worker_ntfy_client()
            try:
                livekid_meal = livekid_meal_name_for_day(
                    date_str,
                    timing_agg,
                    prefetched_presence=livekid_presence_by_date,
                )
                if livekid_meal:
                    livekid_meal_name = livekid_meal
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(f"LiveKid: {exc}")

            try:
                ntfy_meal_name = ntfy_meal_name_for_day(
                    client,
                    date_str,
                    nela_delivery_diet_id=nela_delivery_diet_id,
                    select_delivery_diet_id=select_delivery_diet_id,
                    timing_agg=timing_agg,
                )
            except Exception as exc:  # pylint: disable=broad-except
                errors.append(f"NTFY: {exc}")
        except Exception as exc:  # pylint: disable=broad-except
            errors.append(str(exc))

        return {
            "date": date_str,
            "day_of_week": datetime.strptime(date_str, "%Y-%m-%d").strftime("%A"),
            "ntfy_meal_name": ntfy_meal_name,
            "livekid_meal_name": livekid_meal_name,
            "error": "; ".join(errors) if errors else None,
        }

    def prefetch_livekid_presence_map(
        overview_dates: list[str],
        *,
        max_workers: int,
        timing_agg: TimingAgg | None,
    ) -> dict[str, dict | None]:
        _log.info(
            "NELA flow: prefetching LiveKid presence for %d dates with %d workers.",
            len(overview_dates),
            max_workers,
        )
        with _time_block("nela.route.livekid_presence_prefetch", timing_agg):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                livekid_presence_list = list(executor.map(fetch_livekid_presence, overview_dates))
        obiad_days = sum(1 for payload in livekid_presence_list if livekid_has_obiad(payload))
        _log.info(
            "NELA flow: LiveKid presence prefetch done (obiad_days=%d/%d).",
            obiad_days,
            len(overview_dates),
        )
        return dict(zip(overview_dates, livekid_presence_list))

    def prefetch_livekid_menus_for_obiad_days(
        livekid_presence_map: dict[str, dict | None],
        *,
        timing_agg: TimingAgg | None,
    ) -> None:
        _log.info("NELA flow: starting serial LiveKid menu prefetch for obiad days.")
        with _time_block("nela.route.livekid_menu_prefetch_serial", timing_agg):
            for date_str, presence_payload in livekid_presence_map.items():
                if not livekid_has_obiad(presence_payload):
                    with cache_lock:
                        livekid_cache[date_str] = None
                    _log.info("NELA flow: skip LiveKid menu date=%s (no obiad in presence).", date_str)
                    continue
                try:
                    _log.info("NELA flow: prefetching LiveKid menu for date=%s.", date_str)
                    menu_payload = fetch_livekid_menu(date_str)
                except Exception:  # pylint: disable=broad-except
                    # Keep row-level error reporting behavior for failed LiveKid reads.
                    _log.exception("NELA flow: LiveKid menu prefetch failed for date=%s.", date_str)
                    continue
                meal_name = livekid_meal_name_from_payloads(presence_payload, menu_payload)
                with cache_lock:
                    livekid_cache[date_str] = meal_name
                _log.info("NELA flow: stored prefetched LiveKid meal for date=%s.", date_str)
        _log.info("NELA flow: serial LiveKid menu prefetch finished.")

    def build_nela_rows_parallel(
        overview_dates: list[str],
        *,
        nela_delivery_diet_id: int | None,
        select_delivery_diet_id: int | None,
        cookie_str: str,
        livekid_presence_map: dict[str, dict | None],
        max_workers: int,
        timing_agg: TimingAgg | None,
    ) -> list[dict]:
        _log.info(
            "NELA flow: building rows in parallel for %d dates with %d workers.",
            len(overview_dates),
            max_workers,
        )
        with _time_block("nela.route.thread_pool", timing_agg):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                rows = list(
                    executor.map(
                        lambda date_str: build_nela_overview_row(
                            date_str,
                            nela_delivery_diet_id=nela_delivery_diet_id,
                            select_delivery_diet_id=select_delivery_diet_id,
                            cookie_str=cookie_str,
                            timing_agg=timing_agg,
                            livekid_presence_by_date=livekid_presence_map,
                        ),
                        overview_dates,
                    )
                )
        _log.info("NELA flow: row build finished.")
        return rows

    def build_nela_overview_payload(start_date: str, *, timing_agg: TimingAgg | None) -> dict:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        _log.info("NELA flow: build overview payload start=%s", start_date)

        with _time_block("nela.route.ntfy_client_init", timing_agg):
            base_client = NtfyClient(explicit_cookie_str=args.cookies)
        with _time_block("nela.route.ensure_authenticated", timing_agg):
            base_client.ensure_authenticated()
        with _time_block("nela.route.delivery_diets_fetch", timing_agg):
            delivery_diets = base_client.get_data(
                path=f"users/{base_client.user_id}/delivery-diets",
                params={
                    "last_delivery_day__gte": start_date,
                    "sort": "first_delivery_day.asc",
                    "expansions__in": "diets",
                },
            )

        with _time_block("nela.route.choose_diet_ids", timing_agg):
            try:
                nela_delivery_diet_id = choose_delivery_diet_id(delivery_diets, "Nela")
            except ValueError:
                nela_delivery_diet_id = None

            try:
                select_delivery_diet_id = choose_delivery_diet_id(delivery_diets, "Select")
            except ValueError:
                select_delivery_diet_id = None
        _log.info(
            "NELA flow: resolved delivery diet ids nela=%s select=%s",
            nela_delivery_diet_id,
            select_delivery_diet_id,
        )

        overview_dates = [
            day.isoformat()
            for offset in range(21)
            if (day := start + timedelta(days=offset)).weekday() < 5
        ]
        pool_workers = min(8, len(overview_dates)) if overview_dates else 1
        livekid_presence_map = prefetch_livekid_presence_map(
            overview_dates,
            max_workers=pool_workers,
            timing_agg=timing_agg,
        )
        prefetch_livekid_menus_for_obiad_days(
            livekid_presence_map,
            timing_agg=timing_agg,
        )
        rows = build_nela_rows_parallel(
            overview_dates,
            nela_delivery_diet_id=nela_delivery_diet_id,
            select_delivery_diet_id=select_delivery_diet_id,
            cookie_str=base_client.cookie_str or "",
            livekid_presence_map=livekid_presence_map,
            max_workers=pool_workers,
            timing_agg=timing_agg,
        )
        prev_start = (start - timedelta(days=21)).isoformat()
        next_start = (start + timedelta(days=21)).isoformat()
        return {
            "start_date": start_date,
            "prev_start": prev_start,
            "next_start": next_start,
            "overview_rows": rows,
        }

    def build_overview_row(
        date_str: str,
        *,
        delivery_diet_id: int,
        protein_target_g: float,
        fiber_target_g: float,
        cookie_str: str,
    ) -> dict:
        try:
            client = NtfyClient(explicit_cookie_str=cookie_str)
            client.ensure_authenticated()
            deliveries = client.get_data(
                path=f"users/{client.user_id}/deliveries",
                params={
                    "date": date_str,
                    "delivery_diet_id": str(delivery_diet_id),
                    "status__in": "TO-BE-REALIZED,REALIZED",
                    "aggregate_by__in": "nutritional_data:date",
                    "expansions__in": DELIVERY_EXPANSIONS,
                },
            )
            if not deliveries.get("results"):
                return {
                    "date": date_str,
                    "calories": None,
                    "protein": None,
                    "fiber": None,
                    "protein_target": protein_target_g,
                    "fiber_target": fiber_target_g,
                    "meets_targets": False,
                    "no_diet": True,
                    "error": None,
                }
            totals = nutrition_aggregates_by_name(deliveries)
            calories = totals.get("calorific_kcal")
            protein = totals.get("protein")
            fiber = totals.get("fiber")
            meets_targets = (
                protein is not None
                and fiber is not None
                and protein + 1e-9 >= protein_target_g
                and fiber + 1e-9 >= fiber_target_g
            )
            return {
                "date": date_str,
                "calories": calories,
                "protein": protein,
                "fiber": fiber,
                "protein_target": protein_target_g,
                "fiber_target": fiber_target_g,
                "meets_targets": meets_targets,
                "no_diet": False,
                "error": None,
            }
        except Exception as exc:  # pylint: disable=broad-except
            error_message = str(exc)
            no_diet = error_message.startswith("No deliveries found for the requested date/diet")
            return {
                "date": date_str,
                "calories": None,
                "protein": None,
                "fiber": None,
                "protein_target": None,
                "fiber_target": None,
                "meets_targets": False,
                "no_diet": no_diet,
                "error": None if no_diet else error_message,
            }

    @app.get("/")
    def overview() -> str:
        start_date = request.args.get("start", args.date)
        validate_date(start_date)
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        config_data = load_json_config(args.config)
        protein_target_g, fiber_target_g = read_caps_from_config(config_data)
        base_client = NtfyClient(explicit_cookie_str=args.cookies)
        base_client.ensure_authenticated()
        delivery_diets = base_client.get_data(
            path=f"users/{base_client.user_id}/delivery-diets",
            params={
                "last_delivery_day__gte": start_date,
                "sort": "first_delivery_day.asc",
                "expansions__in": "diets",
            },
        )
        delivery_diet_id = choose_delivery_diet_id(delivery_diets, args.diet_name)
        overview_dates = [
            day.isoformat()
            for offset in range(21)
            if (day := start + timedelta(days=offset)).weekday() < 5
        ]
        with ThreadPoolExecutor(max_workers=min(8, len(overview_dates))) as executor:
            rows = list(
                executor.map(
                    lambda date_str: build_overview_row(
                        date_str,
                        delivery_diet_id=delivery_diet_id,
                        protein_target_g=protein_target_g,
                        fiber_target_g=fiber_target_g,
                        cookie_str=base_client.cookie_str or "",
                    ),
                    overview_dates,
                )
            )
        prev_start = (start - timedelta(days=21)).isoformat()
        next_start = (start + timedelta(days=21)).isoformat()
        return render_template(
            "overview.html",
            overview_rows=rows,
            start_date=start_date,
            prev_start=prev_start,
            next_start=next_start,
            diet_name=args.diet_name,
        )

    @app.get("/nela")
    def nela_overview() -> str:
        start_date = nela_default_start_date_str()
        validate_date(start_date)
        sync_mode = request.args.get("sync") == "1"
        _log.info("NELA flow: GET /nela shell start=%s sync=%s", start_date, sync_mode)
        if sync_mode:
            timing_agg = TimingAgg() if _timing_enabled() else None
            payload = build_nela_overview_payload(start_date, timing_agg=timing_agg)
            if timing_agg is not None:
                timing_agg.emit(prefix="GET /nela?sync=1 timing (sorted by total ms)")
            return render_template(
                "nela.html",
                overview_rows=payload["overview_rows"],
                sync_mode=True,
            )
        return render_template(
            "nela.html",
            overview_rows=[],
            sync_mode=False,
        )

    @app.get("/api/nela/overview")
    def nela_overview_api() -> Response:
        start_date = nela_default_start_date_str()
        validate_date(start_date)
        timing_agg = TimingAgg() if _timing_enabled() else None
        _log.info("NELA flow: GET /api/nela/overview start=%s", start_date)
        payload = build_nela_overview_payload(start_date, timing_agg=timing_agg)
        if timing_agg is not None:
            timing_agg.emit(prefix="GET /api/nela/overview timing (sorted by total ms)")
        return jsonify(payload)

    @app.post("/nela/refresh")
    def nela_refresh() -> Response:
        clear_all_caches()
        return redirect(url_for("nela_overview"))

    @app.get("/nela_opcje")
    def nela_opcje_view() -> str:
        current_date = request.args.get("date", args.date)
        error_message = None
        options: List[dict] = []
        diet_name_slex = "Slex"

        try:
            validate_date(current_date)
        except ValueError as exc:
            error_message = str(exc)
        else:
            try:
                context = fetch_delivery_context(
                    date=current_date,
                    diet_name=diet_name_slex,
                    config_path=args.config,
                    explicit_cookie_str=args.cookies,
                )
                with cache_lock:
                    context_cache[current_date] = context
                _, rows = obiad_meal_from_rows_by_meal(context["rows_by_meal"])
                for row in rows:
                    image_id = row.get("image_id")
                    product_id = row.get("simple_product_id")
                    options.append(
                        {
                            "name": row.get("name") or "",
                            "calorific": row.get("calorific"),
                            "protein": row.get("protein"),
                            "fiber": row.get("fiber"),
                            "imageUrl": f"/images/{image_id}" if image_id else None,
                            "selected": False,
                            "simple_product_id": str(product_id) if product_id is not None else None,
                            "livekid": False,
                        }
                    )
            except Exception as exc:  # pylint: disable=broad-except
                error_message = str(exc)

            try:
                livekid_line = livekid_meal_name_for_day(current_date, None)
                if livekid_line and livekid_line != "Brak Menu":
                    options.insert(
                        0,
                        {
                            "name": livekid_line,
                            "calorific": None,
                            "protein": None,
                            "fiber": None,
                            "imageUrl": None,
                            "selected": False,
                            "simple_product_id": nela_livekid_favourite_product_id(current_date),
                            "livekid": True,
                        },
                    )
            except Exception:  # pylint: disable=broad-except
                pass

        with nela_favourites_lock:
            favourite_entries = load_nela_favourite_entries(favourites_path)

        for opt in options:
            opt["is_favourite"] = nela_meal_favourite_matches(opt, favourite_entries)

        favourite_options: List[dict] = []
        other_options: List[dict] = []
        for opt in options:
            if opt.get("is_favourite"):
                favourite_options.append(opt)
            else:
                other_options.append(opt)

        header = polish_weekday_and_display_date(current_date)
        if header:
            weekday_label, date_display = header
            page_title = f"Obiad — {weekday_label.capitalize()} {date_display} (Slex)"
        else:
            weekday_label, date_display = None, None
            page_title = "Obiad — opcje (Slex)"

        return render_template(
            "nela_opcje.html",
            favourite_options=favourite_options,
            other_options=other_options,
            error_message=error_message,
            current_date=current_date,
            weekday_label=weekday_label,
            date_display=date_display,
            page_title=page_title,
        )

    @app.get("/day")
    def day_view() -> str:
        current_date = request.args.get("date", args.date)
        error_message = None
        chooser_payload = None
        current_protein_target = None
        current_fiber_target = None
        try:
            validate_date(current_date)
            context = get_context_for_date(current_date)
            current_protein_target = parse_positive_target(
                request.args.get("protein_target"),
                context["protein_cap_g"],
                "Protein",
            )
            current_fiber_target = parse_positive_target(
                request.args.get("fiber_target"),
                context["fiber_cap_g"],
                "Fiber",
            )
            chooser_payload = get_chooser_payload_for_targets(
                current_date,
                protein_target_g=current_protein_target,
                fiber_target_g=current_fiber_target,
            )
        except Exception as exc:  # pylint: disable=broad-except
            error_message = str(exc)
            if current_protein_target is None or current_fiber_target is None:
                try:
                    context = get_context_for_date(current_date)
                    current_protein_target = context["protein_cap_g"]
                    current_fiber_target = context["fiber_cap_g"]
                except Exception:  # pylint: disable=broad-except
                    current_protein_target = ""
                    current_fiber_target = ""

        prev_date, next_date = adjacent_dates(current_date)
        return render_template(
            "meal_chooser.html",
            page_data_json=json.dumps(chooser_payload, ensure_ascii=False) if chooser_payload else "null",
            current_date=current_date,
            prev_date=prev_date,
            next_date=next_date,
            error_message=error_message,
            diet_name=args.diet_name,
            current_protein_target=current_protein_target,
            current_fiber_target=current_fiber_target,
        )

    @app.get("/api/chooser")
    def chooser_api() -> Response:
        current_date = request.args.get("date", args.date)
        validate_date(current_date)
        context = get_context_for_date(current_date)
        protein_target = parse_positive_target(
            request.args.get("protein_target"),
            context["protein_cap_g"],
            "Protein",
        )
        fiber_target = parse_positive_target(
            request.args.get("fiber_target"),
            context["fiber_cap_g"],
            "Fiber",
        )
        chooser_payload = get_chooser_payload_for_targets(
            current_date,
            protein_target_g=protein_target,
            fiber_target_g=fiber_target,
        )
        return jsonify(chooser_payload)

    @app.post("/api/save")
    def save_api() -> Response:
        payload = request.get_json(silent=True) or {}
        current_date = str(payload.get("date") or args.date)
        validate_date(current_date)

        context = get_context_for_date(current_date)
        protein_target = parse_positive_target(
            str(payload.get("protein_target")) if payload.get("protein_target") is not None else None,
            context["protein_cap_g"],
            "Protein",
        )
        fiber_target = parse_positive_target(
            str(payload.get("fiber_target")) if payload.get("fiber_target") is not None else None,
            context["fiber_cap_g"],
            "Fiber",
        )

        choice_indices = payload.get("selections")
        if not isinstance(choice_indices, list):
            return jsonify({"error": "Selections must be a list."}), 400

        try:
            choices = choices_from_indices(context["rows_by_meal"], [int(value) for value in choice_indices])
        except (TypeError, ValueError) as exc:
            return jsonify({"error": str(exc)}), 400

        def apply_with_context(active_context: dict) -> dict:
            active_choices = choices_from_indices(active_context["rows_by_meal"], [int(value) for value in choice_indices])
            return apply_optimal_plan_via_api(
                client=active_context["client"],
                delivery_id=active_context["delivery_id"],
                choices=active_choices,
            )

        apply_result = apply_with_context(context)
        if apply_result["failures"]:
            return jsonify({"error": "; ".join(apply_result["failures"])}), 502

        invalidate_date_cache(current_date)
        refreshed_context = get_context_for_date(current_date)
        chooser_payload = get_chooser_payload_for_targets(
            current_date,
            protein_target_g=protein_target,
            fiber_target_g=fiber_target,
        )
        return jsonify(
            {
                "message": "Selection saved.",
                "chooser": chooser_payload,
                "savedSelections": chooser_payload["savedSelections"],
                "deliveryId": refreshed_context["delivery_id"],
            }
        )

    @app.post("/api/nela/favourite")
    def nela_favourite_api() -> Response:
        payload = request.get_json(silent=True) or {}
        raw_id = payload.get("simple_product_id")
        if raw_id is None or raw_id == "":
            return jsonify({"error": "simple_product_id is required."}), 400
        product_id = str(raw_id)
        meal_name = str(payload.get("meal_name") or "").strip()
        favourite = payload.get("favourite")
        if not isinstance(favourite, bool):
            return jsonify({"error": "favourite must be a boolean."}), 400
        with nela_favourites_lock:
            entries = load_nela_favourite_entries(favourites_path)
            if favourite:
                add_nela_favourite_entry(entries, product_id=product_id, meal_name=meal_name)
            else:
                remove_nela_favourite_entry(entries, product_id=product_id, meal_name=meal_name)
            save_nela_favourite_entries(favourites_path, entries)
        return jsonify(
            {
                "simple_product_id": product_id,
                "meal_name": meal_name,
                "favourite": favourite,
            }
        )

    @app.get("/images/<path:image_id>")
    def meal_image(image_id: str) -> Response:
        if image_id not in image_cache:
            try:
                session_context = next(iter(context_cache.values()), None)
                if session_context is None:
                    session_context = get_context_for_date(args.date)
                image_cache[image_id] = fetch_image_bytes(
                    image_id,
                    client=session_context["client"],
                )
            except Exception:  # pylint: disable=broad-except
                abort(404)
        image_bytes, content_type = image_cache[image_id]
        return Response(image_bytes, mimetype=content_type)

    return app


def main() -> int:
    args = parse_args()
    app = create_app(args)
    print(f"[OK] Meal chooser ready at http://{args.host}:{args.port} (debug reload enabled)")
    app.run(host=args.host, port=args.port, debug=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
