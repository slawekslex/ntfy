#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, Tuple

from flask import Flask, Response, abort, jsonify, render_template, request

from ntfy_meals_lib import (
    DELIVERY_EXPANSIONS,
    NtfyClient,
    apply_optimal_plan_via_api,
    build_chooser_payload,
    choose_delivery_diet_id,
    choices_from_indices,
    fetch_delivery_context,
    fetch_image_bytes,
    load_json_config,
    nutrition_aggregates_by_name,
    read_caps_from_config,
    validate_date,
)


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


def create_app(args: argparse.Namespace) -> Flask:
    app = Flask(__name__)
    image_cache: Dict[str, Tuple[bytes, str]] = {}
    chooser_cache: Dict[str, dict] = {}
    context_cache: Dict[str, dict] = {}

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
        overview_dates = [(start + timedelta(days=offset)).isoformat() for offset in range(21)]
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
