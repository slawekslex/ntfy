#!/usr/bin/env python3
"""
Fetch NTFY meal options for a diet/day and print nutrition tables.

Usage:
  python ntfy_meals_nutrition.py --date 2026-04-11
  python ntfy_meals_nutrition.py --date 2026-04-11 --diet-name "Slex"
  python ntfy_meals_nutrition.py --date 2026-04-11 --cookies "PHPSESSID=...; session=...; user_id=..."

Authentication:
  Provide cookie string via:
  - --cookies argument, OR
  - NTFY_COOKIES environment variable

Config:
  Optional config file (default: config.json):
  {
    "protein_cap_g": 150,
    "fiber_cap_g": 40
  }
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import requests


ORION_BASE = "https://orion-api.ntfy.pl/api/v2.0"
MEAL_ORDER = ["BREAKFAST", "SECOND-BREAKFAST", "LUNCH", "TEA", "DINNER"]


@dataclass
class SessionData:
    token: str
    user_id: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch NTFY meal nutrition tables.")
    parser.add_argument("--date", required=True, help="Date in format YYYY-MM-DD (e.g. 2026-04-11)")
    parser.add_argument("--diet-name", default="Slex", help="Diet display name prefix, default: Slex")
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to JSON config file with protein_cap_g and fiber_cap_g.",
    )
    parser.add_argument(
        "--optimal-plan",
        action="store_true",
        help=(
            "Compute optimal day plan (one option per meal) using dynamic programming with priorities: "
            "1) maximize protein up to 150g, 2) maximize fiber up to 40g, 3) minimize calories."
        ),
    )
    parser.add_argument(
        "--cookies",
        default=None,
        help="Raw cookie header string. If omitted, NTFY_COOKIES env var is used.",
    )
    return parser.parse_args()


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


def request_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Api-Language": "pl",
        "Trace-Id": str(uuid.uuid4()),
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
    }


def api_get(
    path: str,
    token: str,
    cookies: Dict[str, str],
    params: Dict[str, str],
) -> dict:
    url = f"{ORION_BASE}/{path.lstrip('/')}"
    response = requests.get(
        url,
        params=params,
        headers=request_headers(token),
        cookies=cookies,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["data"]


def choose_delivery_diet_id(data: dict, diet_name: str) -> int:
    target = diet_name.strip().lower()
    candidates = []
    for row in data.get("results", []):
        display_name = (row.get("user_diet_name") or "").strip()
        if display_name.lower().startswith(target):
            candidates.append(row)

    if not candidates:
        available = [r.get("user_diet_name", "") for r in data.get("results", [])]
        raise ValueError(
            f"No delivery diet found for name '{diet_name}'. "
            f"Available examples: {', '.join(available[:6])}"
        )

    # Prefer active plans and newest id as a tie-breaker.
    candidates.sort(
        key=lambda r: (
            0 if r.get("status") == "TO-BE-REALIZED" else 1,
            -(r.get("id") or 0),
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


def compute_optimal_plan(
    rows_by_meal: Dict[str, List[dict]],
    protein_cap_g: float,
    fiber_cap_g: float,
) -> dict:
    """
    Dynamic programming:
    - choose exactly one option from each meal group
    - objective (lexicographic):
      1) maximize total protein capped at configured cap
      2) maximize total fiber capped at configured cap
      3) minimize total calories
    """
    scale = 10
    protein_cap = int(round(protein_cap_g * scale))
    fiber_cap = int(round(fiber_cap_g * scale))

    meal_groups = [(meal, rows) for meal, rows in rows_by_meal.items() if rows]
    if not meal_groups:
        raise ValueError("No meal groups available to build an optimal plan.")

    # state[(protein_capped, fiber_capped)] = {"cal": int, "choices": List[dict], "p_raw": int, "f_raw": int}
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
                        }
                    ],
                }

                existing = next_states.get(new_key)
                if existing is None or candidate["cal"] < existing["cal"]:
                    next_states[new_key] = candidate

        states = next_states
        if not states:
            raise ValueError(f"No valid options for meal group '{meal_label}'.")

    # Pick best terminal state by lexicographic objective.
    # Max capped protein, then max capped fiber, then min calories.
    best_key = min(states.keys(), key=lambda k: (-k[0], -k[1], states[k]["cal"]))
    best_state = states[best_key]

    return {
        "choices": best_state["choices"],
        "protein_capped": best_key[0] / scale,
        "fiber_capped": best_key[1] / scale,
        "protein_raw": best_state["p_raw"] / scale,
        "fiber_raw": best_state["f_raw"] / scale,
        "calories": best_state["cal"] / scale,
    }


def build_rows_by_meal(delivery_payload: dict) -> Dict[str, List[dict]]:
    results = delivery_payload.get("results", [])
    if not results:
        raise ValueError("No deliveries found for the requested date/diet.")

    includes = delivery_payload.get("includes", {})
    delivery_id = results[0]["id"]

    delivery_items = [x for x in includes.get("delivery_items", []) if x.get("delivery_id") == delivery_id]
    products = {x["id"]: x for x in includes.get("simple_products", [])}
    meals = {x["id"]: x for x in includes.get("diet_variant_meals", [])}
    meal_types = {x["id"]: x for x in includes.get("diet_variant_meal_types", [])}
    alternatives = {
        x["delivery_item_id"]: x.get("simple_product_ids", [])
        for x in includes.get("alternative_meals", [])
    }

    product_ids_by_meal_key: Dict[str, List[int]] = defaultdict(list)
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
        product_ids_by_meal_key[meal_key].append(item.get("simple_product_id"))
        product_ids_by_meal_key[meal_key].extend(alternatives.get(item["id"], []))

    rows_by_meal: Dict[str, List[dict]] = {}
    for meal_key, ids in product_ids_by_meal_key.items():
        seen = set()
        unique_ids = []
        for pid in ids:
            if pid and pid not in seen:
                seen.add(pid)
                unique_ids.append(pid)

        rows = []
        for pid in unique_ids:
            product = products.get(pid)
            if not product:
                continue
            rows.append(
                {
                    "name": product.get("name"),
                    "calorific": product.get("calorific"),
                    "protein": product.get("protein"),
                    "fiber": product.get("fiber"),
                }
            )
        rows.sort(key=lambda r: (r.get("name") or ""))
        rows_by_meal[meal_labels.get(meal_key, meal_key)] = rows

    ordered = {}
    for key in MEAL_ORDER:
        labels = [label for label in rows_by_meal if rows_by_meal[label]]
        for label in labels:
            # keep only labels matching this meal key ordering
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

    # Include anything not matched by known ordering.
    for label, rows in rows_by_meal.items():
        if label not in ordered:
            ordered[label] = rows

    return ordered


def main() -> int:
    args = parse_args()
    validate_date(args.date)
    config_data = load_json_config(args.config)
    protein_cap_g, fiber_cap_g = read_caps_from_config(config_data)

    env_file_vars = load_env_file(".env")
    cookie_str = args.cookies or os.getenv("NTFY_COOKIES") or env_file_vars.get("NTFY_COOKIES")
    if not cookie_str:
        print(
            "Error: provide cookies via --cookies, NTFY_COOKIES env var, or .env file.",
            file=sys.stderr,
        )
        return 1

    cookies = parse_cookie_string(cookie_str)
    session = session_from_cookies(cookies)

    delivery_diets = api_get(
        path=f"users/{session.user_id}/delivery-diets",
        token=session.token,
        cookies=cookies,
        params={
            "last_delivery_day__gte": args.date,
            "sort": "first_delivery_day.asc",
            "expansions__in": "diets",
        },
    )

    delivery_diet_id = choose_delivery_diet_id(delivery_diets, args.diet_name)

    deliveries = api_get(
        path=f"users/{session.user_id}/deliveries",
        token=session.token,
        cookies=cookies,
        params={
            "date": args.date,
            "delivery_diet_id": str(delivery_diet_id),
            "status__in": "TO-BE-REALIZED,REALIZED",
            "expansions__in": (
                "address_id,delivery_items,delivery_items.simple_products,"
                "delivery_items.diet_variant_meals,"
                "delivery_items.diet_variant_meals.diet_variant_meal_types,"
                "delivery_items.alternative_meals,"
                "delivery_items.simple_products.product_labels,"
                "delivery_items.simple_products.product_badges,"
                "delivery_items.simple_products.badges"
            ),
        },
    )

    rows_by_meal = build_rows_by_meal(deliveries)

    print(f"# Nutrition table for {args.diet_name} on {args.date}")
    print("")
    for meal_label, rows in rows_by_meal.items():
        if rows:
            print(markdown_table(meal_label, rows))

    if args.optimal_plan:
        plan = compute_optimal_plan(rows_by_meal, protein_cap_g=protein_cap_g, fiber_cap_g=fiber_cap_g)
        print("## Optimal day plan")
        print("")
        print("| Meal | Selected option | Calories (kcal) | Protein (g) | Fiber (g) |")
        print("|---|---|---:|---:|---:|")
        for choice in plan["choices"]:
            meal = str(choice["meal"]).replace("|", "\\|")
            name = str(choice["name"]).replace("|", "\\|")
            print(
                f"| {meal} | {name} | {choice['calorific']} | {choice['protein']} | {choice['fiber']} |"
            )
        print("")
        print(
            "Totals: "
            f"{plan['calories']} kcal, "
            f"protein {plan['protein_raw']}g (capped objective: {plan['protein_capped']}g/{protein_cap_g}g), "
            f"fiber {plan['fiber_raw']}g (capped objective: {plan['fiber_capped']}g/{fiber_cap_g}g)."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
