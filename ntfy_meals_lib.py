from __future__ import annotations

import base64
from contextlib import contextmanager
import fcntl
import json
import os
import time
import urllib.parse
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests


ORION_BASE = "https://orion-api.ntfy.pl/api/v2.0"
MEAL_ORDER = ["BREAKFAST", "SECOND-BREAKFAST", "LUNCH", "TEA", "DINNER"]
CACHE_FILE = ".ntfy_cookie_cache.json"
DATA_DIR_ENV_VAR = "LIVEKID_DATA_DIR"
DEFAULT_DATA_DIR = ".data"
DELIVERY_EXPANSIONS = (
    "address_id,delivery_items,delivery_items.simple_products,"
    "delivery_items.diet_variant_meals,"
    "delivery_items.diet_variant_meals.diet_variant_meal_types,"
    "delivery_items.alternative_meals,"
    "delivery_items.simple_products.product_labels,"
    "delivery_items.simple_products.product_badges,"
    "delivery_items.simple_products.badges"
)


@dataclass
class SessionData:
    token: str
    user_id: int


def resolve_data_dir() -> Path:
    return Path(os.getenv(DATA_DIR_ENV_VAR, DEFAULT_DATA_DIR))


def resolve_cache_path(path: str | os.PathLike[str] | None = None) -> Path:
    if path is not None:
        return Path(path)
    return resolve_data_dir() / CACHE_FILE


def lock_path_for(target: Path) -> Path:
    return target.parent / f"{target.name}.lock"


@contextmanager
def file_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_path, path)


class NtfyClient:
    def __init__(self, *, explicit_cookie_str: Optional[str] = None, env_path: str = ".env") -> None:
        env_file_vars = load_env_file(env_path)
        username, password = credentials_from_env(env_file_vars)
        self.explicit_cookie_str = explicit_cookie_str
        self.env_cookie_str = os.getenv("NTFY_COOKIES") or env_file_vars.get("NTFY_COOKIES")
        self.username = username
        self.password = password
        self.cookie_str: Optional[str] = None
        self.cookies: Dict[str, str] = {}
        self.session_data: Optional[SessionData] = None

    @property
    def token(self) -> str:
        if self.session_data is None:
            raise ValueError("Client is not authenticated.")
        return self.session_data.token

    @property
    def user_id(self) -> int:
        if self.session_data is None:
            raise ValueError("Client is not authenticated.")
        return self.session_data.user_id

    def ensure_authenticated(self, force_refresh: bool = False) -> SessionData:
        if not force_refresh and self.cookie_str and not cookie_str_is_expired(self.cookie_str):
            if self.session_data is None:
                self.cookies = parse_cookie_string(self.cookie_str)
                self.session_data = session_from_cookies(self.cookies)
            return self.session_data

        if force_refresh and self.username and self.password:
            fresh_cookie_str = login_via_api(self.username, self.password)
            save_cookie_cache(fresh_cookie_str)
            self.cookie_str = fresh_cookie_str
        else:
            self.cookie_str = resolve_cookie_string(
                explicit_cookie_str=self.explicit_cookie_str,
                env_cookie_str=self.env_cookie_str,
                username=self.username,
                password=self.password,
            )

        self.cookies = parse_cookie_string(self.cookie_str)
        self.session_data = session_from_cookies(self.cookies)
        return self.session_data

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, str]] = None,
        payload: Optional[Dict[str, object]] = None,
        retry_on_unauthorized: bool = True,
    ) -> requests.Response:
        self.ensure_authenticated()
        response = send_ntfy_request(
            method=method,
            path=path,
            token=self.token,
            cookies=self.cookies,
            params=params,
            payload=payload,
        )
        if response.status_code == 401 and retry_on_unauthorized:
            self.ensure_authenticated(force_refresh=True)
            response = send_ntfy_request(
                method=method,
                path=path,
                token=self.token,
                cookies=self.cookies,
                params=params,
                payload=payload,
            )
        response.raise_for_status()
        return response

    def get_data(self, path: str, params: Dict[str, str]) -> dict:
        response = self.request("GET", path, params=params)
        return response.json()["data"]

    def patch_data(self, path: str, payload: Dict[str, object]) -> dict:
        response = self.request("PATCH", path, payload=payload)
        return response.json()["data"]

    def fetch_image_bytes(self, image_id: str) -> tuple[bytes, str]:
        response = self.request("GET", f"images/{urllib.parse.quote(image_id)}")
        content_type = response.headers.get("Content-Type", "image/jpeg").split(";", 1)[0]
        return response.content, content_type


def load_json_config(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as cfg_file:
        return json.load(cfg_file)


def read_caps_from_config(config_data: dict) -> tuple[float, float]:
    protein_cap = float(config_data.get("protein_cap_g", 150))
    fiber_cap = float(config_data.get("fiber_cap_g", 40))
    if protein_cap <= 0 or fiber_cap <= 0:
        raise ValueError("protein_cap_g and fiber_cap_g must be positive numbers.")
    return protein_cap, fiber_cap


def load_env_file(path: str = ".env") -> Dict[str, str]:
    env_vars: Dict[str, str] = {}
    if not os.path.exists(path):
        return env_vars

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if (
                len(value) >= 2
                and ((value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")))
            ):
                value = value[1:-1]
            env_vars[key] = value
    return env_vars


def credentials_from_env(env_vars: Dict[str, str]) -> tuple[Optional[str], Optional[str]]:
    username = (
        os.getenv("USER_NAME")
        or os.getenv("NTFY_USER_NAME")
        or env_vars.get("USER_NAME")
        or env_vars.get("NTFY_USER_NAME")
    )
    password = (
        os.getenv("PASSWORD")
        or os.getenv("NTFY_PASSWORD")
        or env_vars.get("PASSWORD")
        or env_vars.get("NTFY_PASSWORD")
    )
    return username, password


def validate_date(date_str: str) -> None:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Invalid date format. Expected YYYY-MM-DD.") from exc


def parse_cookie_string(cookie_str: str) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    for part in cookie_str.split("; "):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        cookies[key] = value
    return cookies


def cookie_dict_to_string(cookies: Dict[str, str]) -> str:
    return "; ".join(f"{key}={value}" for key, value in cookies.items())


def load_cookie_cache(path: str | os.PathLike[str] | None = None) -> Optional[str]:
    cache_path = resolve_cache_path(path)
    if not cache_path.exists():
        return None
    with file_lock(lock_path_for(cache_path)):
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:  # pylint: disable=broad-except
            return None
        cookie_str = payload.get("cookie_str")
        if isinstance(cookie_str, str) and cookie_str.strip():
            return cookie_str.strip()
        return None


def save_cookie_cache(cookie_str: str, path: str | os.PathLike[str] | None = None) -> None:
    cache_path = resolve_cache_path(path)
    payload = {
        "cookie_str": cookie_str,
        "cached_at": datetime.now().isoformat(timespec="seconds"),
    }
    with file_lock(lock_path_for(cache_path)):
        write_json_atomic(cache_path, payload)


def decode_jwt_payload(token: str) -> dict:
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("JWT token must have three parts.")
    payload_b64 = parts[1]
    payload_b64 += "=" * (-len(payload_b64) % 4)
    payload_bytes = base64.urlsafe_b64decode(payload_b64.encode("ascii"))
    return json.loads(payload_bytes.decode("utf-8"))


def session_from_cookies(cookies: Dict[str, str]) -> SessionData:
    if "session" not in cookies:
        raise ValueError("Missing 'session' cookie.")
    try:
        session_json = json.loads(urllib.parse.unquote(cookies["session"]))
        token = session_json["token"]
        user_id = int(session_json["userId"])
    except Exception as exc:  # pylint: disable=broad-except
        raise ValueError("Unable to parse 'session' cookie JSON.") from exc
    return SessionData(token=token, user_id=user_id)


def cookie_str_is_expired(cookie_str: str, skew_seconds: int = 60) -> bool:
    try:
        cookies = parse_cookie_string(cookie_str)
        session = session_from_cookies(cookies)
        payload = decode_jwt_payload(session.token)
        exp = int(payload["exp"])
    except Exception:  # pylint: disable=broad-except
        return True
    return exp <= int(time.time()) + skew_seconds


def send_ntfy_request(
    *,
    method: str,
    path: str,
    token: Optional[str] = None,
    cookies: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    payload: Optional[Dict[str, object]] = None,
    timeout: int = 30,
) -> requests.Response:
    url = f"{ORION_BASE}/{path.lstrip('/')}"
    session = requests.Session()
    headers = {
        "Api-Language": "pl",
        "Trace-Id": str(uuid.uuid4()),
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return session.request(
        method=method,
        url=url,
        params=params,
        json=payload,
        headers=headers,
        cookies=cookies,
        timeout=timeout,
    )


def login_via_api(username: str, password: str) -> str:
    print("[AUTH] Logging in via Orion API...")
    response = send_ntfy_request(
        method="POST",
        path="sessions",
        payload={"email": username, "password": password},
    )
    response.raise_for_status()
    data = response.json()["data"]
    token = data["token"]
    refresh_token = data["refresh_token"]
    jwt_payload = decode_jwt_payload(token)
    user_id = int(jwt_payload["id"])
    session_cookie = urllib.parse.quote(
        json.dumps(
            {
                "token": token,
                "refreshToken": refresh_token,
                "userId": user_id,
                "firstName": jwt_payload.get("first_name", ""),
            },
            separators=(",", ":"),
        )
    )
    return cookie_dict_to_string({"session": session_cookie, "user_id": str(user_id)})


def resolve_cookie_string(
    *,
    explicit_cookie_str: Optional[str],
    env_cookie_str: Optional[str],
    username: Optional[str],
    password: Optional[str],
) -> str:
    if explicit_cookie_str:
        if not cookie_str_is_expired(explicit_cookie_str):
            print("[AUTH] Using cookies from --cookies.")
            return explicit_cookie_str
        print("[AUTH] Cookies from --cookies are expired.")

    cached_cookie_str = load_cookie_cache()
    if cached_cookie_str:
        if not cookie_str_is_expired(cached_cookie_str):
            print(f"[AUTH] Using cached cookies from {CACHE_FILE}.")
            return cached_cookie_str
        print(f"[AUTH] Cached cookies in {CACHE_FILE} are expired.")

    if env_cookie_str:
        if not cookie_str_is_expired(env_cookie_str):
            print("[AUTH] Using cookies from environment/.env.")
            save_cookie_cache(env_cookie_str)
            return env_cookie_str
        print("[AUTH] Cookies from environment/.env are expired.")

    if username and password:
        fresh_cookie_str = login_via_api(username, password)
        save_cookie_cache(fresh_cookie_str)
        print(f"[AUTH] Saved fresh cookies to {CACHE_FILE}.")
        return fresh_cookie_str

    raise ValueError(
        "No valid cookies available. Provide USER_NAME/PASSWORD, or pass unexpired cookies via --cookies, "
        "NTFY_COOKIES, or .env."
    )


def choose_delivery_diet_id(data: dict, diet_name: str) -> int:
    target = diet_name.strip().lower()
    candidates = []
    for row in data.get("results", []):
        display_name = (row.get("user_diet_name") or "").strip()
        if display_name.lower().startswith(target):
            candidates.append(row)

    if not candidates:
        available = [row.get("user_diet_name", "") for row in data.get("results", [])]
        raise ValueError(
            f"No delivery diet found for name '{diet_name}'. "
            f"Available examples: {', '.join(available[:6])}"
        )

    candidates.sort(
        key=lambda row: (
            0 if row.get("status") == "TO-BE-REALIZED" else 1,
            -(row.get("id") or 0),
        )
    )
    return int(candidates[0]["id"])


def markdown_table(title: str, rows: List[dict]) -> str:
    lines = [
        f"### {title}",
        "",
        "| Meal option | Calories (kcal) | Protein (g) | Fiber (g) |",
        "|---|---:|---:|---:|",
    ]
    for row in rows:
        name = str(row.get("name", "")).replace("|", "\\|")
        kcal = row.get("calorific")
        protein = row.get("protein")
        fiber = row.get("fiber")
        lines.append(f"| {name} | {kcal} | {protein} | {fiber} |")
    lines.append("")
    return "\n".join(lines)


def nutrition_totals(choices: List[dict]) -> dict:
    calories = 0.0
    protein = 0.0
    fiber = 0.0
    for row in choices:
        calories += float(row.get("calorific") or 0)
        protein += float(row.get("protein") or 0)
        fiber += float(row.get("fiber") or 0)
    return {"calories": round(calories, 1), "protein": round(protein, 1), "fiber": round(fiber, 1)}


def compute_optimal_plan(
    rows_by_meal: Dict[str, List[dict]],
    protein_cap_g: float,
    fiber_cap_g: float,
) -> dict:
    scale = 10
    protein_cap = int(round(protein_cap_g * scale))
    fiber_cap = int(round(fiber_cap_g * scale))

    meal_groups = [(meal, rows) for meal, rows in rows_by_meal.items() if rows]
    if not meal_groups:
        raise ValueError("No meal groups available to build an optimal plan.")

    states = {(0, 0): {"cal": 0, "choices": [], "p_raw": 0, "f_raw": 0}}

    for meal_label, options in meal_groups:
        next_states = {}
        for (p_cap, f_cap), state in states.items():
            for option in options:
                if option.get("calorific") is None or option.get("protein") is None or option.get("fiber") is None:
                    continue

                p_add = int(round(float(option["protein"]) * scale))
                f_add = int(round(float(option["fiber"]) * scale))
                cal_add = int(round(float(option["calorific"]) * scale))

                new_p_raw = state["p_raw"] + p_add
                new_f_raw = state["f_raw"] + f_add
                new_cal = state["cal"] + cal_add
                new_key = (min(protein_cap, p_cap + p_add), min(fiber_cap, f_cap + f_add))

                candidate = {
                    "cal": new_cal,
                    "p_raw": new_p_raw,
                    "f_raw": new_f_raw,
                    "choices": state["choices"]
                    + [
                        {
                            "meal": meal_label,
                            "name": option["name"],
                            "calorific": option["calorific"],
                            "protein": option["protein"],
                            "fiber": option["fiber"],
                            "simple_product_id": option.get("simple_product_id"),
                            "delivery_item_id": option.get("delivery_item_id"),
                            "selected": option.get("selected", False),
                        }
                    ],
                }

                existing = next_states.get(new_key)
                if existing is None or candidate["cal"] < existing["cal"]:
                    next_states[new_key] = candidate

        states = next_states
        if not states:
            raise ValueError(f"No valid options for meal group '{meal_label}'.")

    best_key = min(states.keys(), key=lambda key: (-key[0], -key[1], states[key]["cal"]))
    best_state = states[best_key]

    return {
        "choices": best_state["choices"],
        "protein_capped": best_key[0] / scale,
        "fiber_capped": best_key[1] / scale,
        "protein_raw": best_state["p_raw"] / scale,
        "fiber_raw": best_state["f_raw"] / scale,
        "calories": best_state["cal"] / scale,
    }


def effective_macro_targets(
    requested_protein_g: float,
    requested_fiber_g: float,
    optimal_plan: dict,
) -> dict:
    effective_protein = min(requested_protein_g, float(optimal_plan["protein_raw"]))
    effective_fiber = min(requested_fiber_g, float(optimal_plan["fiber_raw"]))
    return {
        "protein": round(effective_protein, 1),
        "fiber": round(effective_fiber, 1),
        "protein_adjusted": effective_protein + 1e-9 < requested_protein_g,
        "fiber_adjusted": effective_fiber + 1e-9 < requested_fiber_g,
    }


def enumerate_feasible_plans(
    rows_by_meal: Dict[str, List[dict]],
    protein_target_g: float,
    fiber_target_g: float,
) -> List[dict]:
    scale = 10
    protein_target = int(round(protein_target_g * scale))
    fiber_target = int(round(fiber_target_g * scale))
    meal_groups = list(rows_by_meal.items())
    meal_count = len(meal_groups)
    if not meal_groups:
        return []

    suffix_max_protein = [0] * (meal_count + 1)
    suffix_max_fiber = [0] * (meal_count + 1)
    for idx in range(meal_count - 1, -1, -1):
        _, options = meal_groups[idx]
        suffix_max_protein[idx] = suffix_max_protein[idx + 1] + max(
            int(round(float(option.get("protein") or 0) * scale))
            for option in options
        )
        suffix_max_fiber[idx] = suffix_max_fiber[idx + 1] + max(
            int(round(float(option.get("fiber") or 0) * scale))
            for option in options
        )

    plans: List[dict] = []
    choice_indices = [0] * meal_count

    def dfs(meal_idx: int, calories: int, protein: int, fiber: int) -> None:
        if protein + suffix_max_protein[meal_idx] < protein_target:
            return
        if fiber + suffix_max_fiber[meal_idx] < fiber_target:
            return

        if meal_idx == meal_count:
            plans.append(
                {
                    "choices": choice_indices.copy(),
                    "calories": calories / scale,
                    "protein": protein / scale,
                    "fiber": fiber / scale,
                }
            )
            return

        _, options = meal_groups[meal_idx]
        for option_idx, option in enumerate(options):
            if option.get("calorific") is None or option.get("protein") is None or option.get("fiber") is None:
                continue
            choice_indices[meal_idx] = option_idx
            dfs(
                meal_idx + 1,
                calories + int(round(float(option["calorific"]) * scale)),
                protein + int(round(float(option["protein"]) * scale)),
                fiber + int(round(float(option["fiber"]) * scale)),
            )

    dfs(0, 0, 0, 0)
    plans.sort(key=lambda plan: (plan["calories"], -plan["protein"], -plan["fiber"], plan["choices"]))
    return plans


def optimal_choice_indices(rows_by_meal: Dict[str, List[dict]], optimal_plan: dict) -> List[int]:
    option_by_meal = {str(choice["meal"]): choice for choice in optimal_plan["choices"]}
    indices = []
    for meal_label, options in rows_by_meal.items():
        target = option_by_meal[meal_label]
        target_product_id = target.get("simple_product_id")
        target_name = target.get("name")
        match_idx = next(
            (
                idx
                for idx, option in enumerate(options)
                if option.get("simple_product_id") == target_product_id or option.get("name") == target_name
            ),
            None,
        )
        if match_idx is None:
            raise ValueError(f"Could not map optimal choice back to meal options for '{meal_label}'.")
        indices.append(match_idx)
    return indices


def selected_choice_indices(rows_by_meal: Dict[str, List[dict]]) -> List[int]:
    indices = []
    for meal_label, options in rows_by_meal.items():
        selected_idx = next((idx for idx, option in enumerate(options) if option.get("selected")), None)
        if selected_idx is None:
            raise ValueError(f"Could not find currently selected option for meal '{meal_label}'.")
        indices.append(selected_idx)
    return indices


def choices_from_indices(rows_by_meal: Dict[str, List[dict]], choice_indices: List[int]) -> List[dict]:
    meal_items = list(rows_by_meal.items())
    if len(choice_indices) != len(meal_items):
        raise ValueError("Choice count does not match meal count.")

    choices = []
    for meal_idx, option_idx in enumerate(choice_indices):
        meal_label, options = meal_items[meal_idx]
        if option_idx < 0 or option_idx >= len(options):
            raise ValueError(f"Option index out of range for meal '{meal_label}'.")
        option = options[option_idx]
        choices.append(
            {
                "meal": meal_label,
                "name": option.get("name"),
                "calorific": option.get("calorific"),
                "protein": option.get("protein"),
                "fiber": option.get("fiber"),
                "simple_product_id": option.get("simple_product_id"),
                "delivery_item_id": option.get("delivery_item_id"),
                "selected": option.get("selected", False),
            }
        )
    return choices


def product_main_image_id(product: dict) -> Optional[str]:
    images = product.get("images") or []
    for image in images:
        image_id = image.get("id")
        if image_id and image.get("type") == "MAIN":
            return str(image_id)
    for image in images:
        image_id = image.get("id")
        if image_id:
            return str(image_id)
    return None


def fetch_image_bytes(image_id: str, client: NtfyClient) -> tuple[bytes, str]:
    return client.fetch_image_bytes(image_id)


def selected_products_by_meal(delivery_payload: dict) -> Dict[str, str]:
    includes = delivery_payload.get("includes", {})
    results = delivery_payload.get("results", [])
    if not results:
        return {}

    delivery_id = results[0]["id"]
    items = [item for item in includes.get("delivery_items", []) if item.get("delivery_id") == delivery_id]
    products = {product["id"]: product for product in includes.get("simple_products", [])}
    meals = {meal["id"]: meal for meal in includes.get("diet_variant_meals", [])}
    meal_types = {meal_type["id"]: meal_type for meal_type in includes.get("diet_variant_meal_types", [])}

    out = {}
    for item in items:
        meal = meals.get(item.get("diet_variant_meal_id"))
        if not meal:
            continue
        meal_type = meal_types.get(meal.get("diet_variant_meal_type_id"), {})
        meal_label = (meal_type.get("meal_name", {}) or {}).get("value")
        if not meal_label:
            continue
        product_name = (products.get(item.get("simple_product_id")) or {}).get("name")
        if product_name:
            out[meal_label] = product_name
    return out


def nutrition_aggregates_by_name(delivery_payload: dict) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for row in delivery_payload.get("aggregates", []):
        name = row.get("name")
        value = row.get("value")
        if not name or value is None:
            continue
        try:
            out[str(name)] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def apply_optimal_plan_via_api(
    *,
    client: NtfyClient,
    delivery_id: int,
    choices: List[dict],
) -> dict:
    failures = []
    for idx, choice in enumerate(choices, start=1):
        meal = str(choice["meal"])
        option = str(choice["name"])
        item_id = choice.get("delivery_item_id")
        product_id = choice.get("simple_product_id")
        if not item_id or not product_id:
            failures.append(f"Missing delivery_item_id/simple_product_id for {meal}: {option}")
            continue
        if choice.get("selected"):
            print(f"[APPLY] {idx}/{len(choices)} Already selected for {meal}.")
            continue
        print(f"[APPLY] {idx}/{len(choices)} Applying {meal}: {option}")
        try:
            client.patch_data(
                path=f"users/{client.user_id}/deliveries/{delivery_id}/items/{item_id}",
                payload={"simple_product_id": int(product_id)},
            )
        except Exception as exc:  # pylint: disable=broad-except
            msg = f"Failed applying {meal}: {option} ({exc})"
            print(f"[WARN] {msg}")
            failures.append(msg)
    return {"failures": failures, "page_nutrition": {}}


def build_rows_by_meal(
    delivery_payload: dict,
    *,
    requested_date: Optional[str] = None,
    requested_diet_name: Optional[str] = None,
) -> tuple[int, Dict[str, List[dict]]]:
    results = delivery_payload.get("results", [])
    if not results:
        detail_parts = []
        if requested_date:
            detail_parts.append(f"date={requested_date}")
        if requested_diet_name:
            detail_parts.append(f"diet={requested_diet_name}")
        detail_text = f" ({', '.join(detail_parts)})" if detail_parts else ""
        raise ValueError(f"No deliveries found for the requested date/diet{detail_text}.")

    includes = delivery_payload.get("includes", {})
    delivery_id = results[0]["id"]

    delivery_items = [item for item in includes.get("delivery_items", []) if item.get("delivery_id") == delivery_id]
    products = {product["id"]: product for product in includes.get("simple_products", [])}
    meals = {meal["id"]: meal for meal in includes.get("diet_variant_meals", [])}
    meal_types = {meal_type["id"]: meal_type for meal_type in includes.get("diet_variant_meal_types", [])}
    alternatives = {
        alternative["delivery_item_id"]: alternative.get("simple_product_ids", [])
        for alternative in includes.get("alternative_meals", [])
    }

    product_rows_by_meal_key: Dict[str, List[dict]] = defaultdict(list)
    meal_labels: Dict[str, str] = {}

    for item in delivery_items:
        meal = meals.get(item.get("diet_variant_meal_id"))
        if not meal:
            continue
        meal_type = meal_types.get(meal.get("diet_variant_meal_type_id"), {})
        meal_name = meal_type.get("meal_name", {})
        meal_key = meal_name.get("key")
        meal_value = meal_name.get("value", meal_key)
        if not meal_key:
            continue

        meal_labels[meal_key] = meal_value
        option_ids = [item.get("simple_product_id"), *alternatives.get(item["id"], [])]
        seen_ids = set()
        for product_id in option_ids:
            if not product_id or product_id in seen_ids:
                continue
            seen_ids.add(product_id)
            product_rows_by_meal_key[meal_key].append(
                {
                    "simple_product_id": product_id,
                    "delivery_item_id": item["id"],
                    "selected": product_id == item.get("simple_product_id"),
                }
            )

    rows_by_meal: Dict[str, List[dict]] = {}
    for meal_key, product_rows in product_rows_by_meal_key.items():
        rows = []
        for product_row in product_rows:
            product_id = product_row["simple_product_id"]
            product = products.get(product_id)
            if not product:
                continue
            rows.append(
                {
                    "name": product.get("name"),
                    "calorific": product.get("calorific"),
                    "protein": product.get("protein"),
                    "fiber": product.get("fiber"),
                    "image_id": product_main_image_id(product),
                    "simple_product_id": product_id,
                    "delivery_item_id": product_row["delivery_item_id"],
                    "selected": product_row["selected"],
                }
            )
        rows.sort(key=lambda row: (row.get("name") or ""))
        rows_by_meal[meal_labels.get(meal_key, meal_key)] = rows

    ordered = {}
    for key in MEAL_ORDER:
        labels = [label for label in rows_by_meal if rows_by_meal[label]]
        for label in labels:
            if key == "BREAKFAST" and label.lower().startswith("śniadanie"):
                ordered[label] = rows_by_meal[label]
            elif key == "SECOND-BREAKFAST" and label.lower().startswith("drugie"):
                ordered[label] = rows_by_meal[label]
            elif key == "LUNCH" and label.lower().startswith("obiad"):
                ordered[label] = rows_by_meal[label]
            elif key == "TEA" and label.lower().startswith("podwieczorek"):
                ordered[label] = rows_by_meal[label]
            elif key == "DINNER" and label.lower().startswith("kolacja"):
                ordered[label] = rows_by_meal[label]

    for label, rows in rows_by_meal.items():
        if label not in ordered:
            ordered[label] = rows

    return delivery_id, ordered


def fetch_delivery_context(
    *,
    date: str,
    diet_name: str,
    config_path: str = "config.json",
    explicit_cookie_str: Optional[str] = None,
    env_path: str = ".env",
) -> dict:
    validate_date(date)
    config_data = load_json_config(config_path)
    protein_cap_g, fiber_cap_g = read_caps_from_config(config_data)
    client = NtfyClient(
        explicit_cookie_str=explicit_cookie_str,
        env_path=env_path,
    )
    client.ensure_authenticated()

    delivery_diets = client.get_data(
        path=f"users/{client.user_id}/delivery-diets",
        params={
            "last_delivery_day__gte": date,
            "sort": "first_delivery_day.asc",
            "expansions__in": "diets",
        },
    )
    delivery_diet_id = choose_delivery_diet_id(delivery_diets, diet_name)

    deliveries = client.get_data(
        path=f"users/{client.user_id}/deliveries",
        params={
            "date": date,
            "delivery_diet_id": str(delivery_diet_id),
            "status__in": "TO-BE-REALIZED,REALIZED",
            "aggregate_by__in": "nutritional_data:date",
            "expansions__in": DELIVERY_EXPANSIONS,
        },
    )

    delivery_id, rows_by_meal = build_rows_by_meal(
        deliveries,
        requested_date=date,
        requested_diet_name=diet_name,
    )
    return {
        "client": client,
        "cookies": client.cookies,
        "session": client.session_data,
        "delivery_diet_id": delivery_diet_id,
        "delivery_id": delivery_id,
        "delivery_payload": deliveries,
        "rows_by_meal": rows_by_meal,
        "protein_cap_g": protein_cap_g,
        "fiber_cap_g": fiber_cap_g,
    }


def build_chooser_payload(
    *,
    date: str,
    diet_name: str,
    rows_by_meal: Dict[str, List[dict]],
    protein_cap_g: float,
    fiber_cap_g: float,
) -> dict:
    optimal_plan = compute_optimal_plan(rows_by_meal, protein_cap_g=protein_cap_g, fiber_cap_g=fiber_cap_g)
    targets = effective_macro_targets(protein_cap_g, fiber_cap_g, optimal_plan)
    feasible_plans = enumerate_feasible_plans(
        rows_by_meal,
        protein_target_g=targets["protein"],
        fiber_target_g=targets["fiber"],
    )
    if not feasible_plans:
        raise ValueError("No feasible plans found for chooser payload.")

    initial_choices = optimal_choice_indices(rows_by_meal, optimal_plan)
    if initial_choices not in [plan["choices"] for plan in feasible_plans]:
        initial_choices = feasible_plans[0]["choices"]
    saved_choices = selected_choice_indices(rows_by_meal)

    meals_payload = []
    for meal_label, options in rows_by_meal.items():
        meals_payload.append(
            {
                "label": meal_label,
                "options": [
                    {
                        "name": option.get("name"),
                        "calorific": option.get("calorific"),
                        "protein": option.get("protein"),
                        "fiber": option.get("fiber"),
                        "imageId": option.get("image_id"),
                    }
                    for option in options
                ],
            }
        )

    return {
        "dietName": diet_name,
        "date": date,
        "requestedTargets": {"protein": protein_cap_g, "fiber": fiber_cap_g},
        "effectiveTargets": targets,
        "initialSelections": initial_choices,
        "savedSelections": saved_choices,
        "feasiblePlans": [plan["choices"] for plan in feasible_plans],
        "meals": meals_payload,
    }
