#!/usr/bin/env python3
"""
Fetch NTFY meal options for a diet/day and print nutrition tables.

Usage:
  python ntfy_meals_nutrition.py --date 2026-04-11
  python ntfy_meals_nutrition.py --date 2026-04-11 --diet-name "Slex"
  python ntfy_meals_nutrition.py --date 2026-04-11 --cookies "PHPSESSID=...; session=...; user_id=..."

Web UI:
  python meal_chooser_web.py --date 2026-04-11
"""

from __future__ import annotations

import argparse
import sys

from ntfy_meals_lib import (
    apply_optimal_plan_via_api,
    compute_optimal_plan,
    fetch_delivery_context,
    markdown_table,
    nutrition_aggregates_by_name,
    nutrition_totals,
    selected_products_by_meal,
)


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
            "1) maximize protein up to cap, 2) maximize fiber up to cap, 3) minimize calories."
        ),
    )
    parser.add_argument(
        "--apply-optimal",
        action="store_true",
        help="Apply optimal plan on NTFY via API automatically.",
    )
    parser.add_argument(
        "--cookies",
        default=None,
        help="Raw cookie header string. If omitted, NTFY_COOKIES env var is used.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        context = fetch_delivery_context(
            date=args.date,
            diet_name=args.diet_name,
            config_path=args.config,
            explicit_cookie_str=args.cookies,
        )
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    rows_by_meal = context["rows_by_meal"]
    protein_cap_g = context["protein_cap_g"]
    fiber_cap_g = context["fiber_cap_g"]
    client = context["client"]
    delivery_id = context["delivery_id"]
    delivery_diet_id = context["delivery_diet_id"]

    print(f"# Nutrition table for {args.diet_name} on {args.date}")
    print("")
    for meal_label, rows in rows_by_meal.items():
        if rows:
            print(markdown_table(meal_label, rows))

    if args.optimal_plan or args.apply_optimal:
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

        if args.apply_optimal:
            print("")
            print("[INFO] Applying optimal plan via NTFY API...")
            apply_result = apply_optimal_plan_via_api(
                client=client,
                delivery_id=delivery_id,
                choices=plan["choices"],
            )

            print("[INFO] Validating saved selections via API...")
            refreshed_deliveries = client.get_data(
                path=f"users/{client.user_id}/deliveries",
                params={
                    "date": args.date,
                    "delivery_diet_id": str(delivery_diet_id),
                    "status__in": "TO-BE-REALIZED,REALIZED",
                    "aggregate_by__in": "nutritional_data:date",
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
            selected_now = selected_products_by_meal(refreshed_deliveries)
            aggregate_totals = nutrition_aggregates_by_name(refreshed_deliveries)
            expected = {choice["meal"]: choice["name"] for choice in plan["choices"]}

            mismatches = []
            for meal, expected_name in expected.items():
                actual = selected_now.get(meal)
                if actual != expected_name:
                    mismatches.append((meal, expected_name, actual))

            if apply_result["failures"]:
                print("[WARN] Some selection steps failed during API apply:")
                for msg in apply_result["failures"]:
                    print(f"  - {msg}")

            if mismatches:
                print("[WARN] Validation mismatch after save:")
                for meal, exp, act in mismatches:
                    print(f"  - {meal}: expected '{exp}' but page/API has '{act}'")
            else:
                print("[OK] All meals were selected as planned.")

            expected_totals = nutrition_totals(plan["choices"])
            print(
                "[CHECK] Script totals: "
                f"{expected_totals['calories']} kcal, "
                f"{expected_totals['protein']} g protein, "
                f"{expected_totals['fiber']} g fiber."
            )
            if aggregate_totals:
                print(
                    "[CHECK] NTFY totals: "
                    f"{aggregate_totals.get('calorific_kcal', 'n/a')} kcal, "
                    f"{aggregate_totals.get('protein', 'n/a')} g protein, "
                    f"{aggregate_totals.get('fiber', 'n/a')} g fiber."
                )
                total_mismatches = []
                total_mapping = {
                    "calories": "calorific_kcal",
                    "protein": "protein",
                    "fiber": "fiber",
                }
                for local_name, aggregate_name in total_mapping.items():
                    actual_value = aggregate_totals.get(aggregate_name)
                    expected_value = expected_totals[local_name]
                    if actual_value is None or abs(actual_value - expected_value) > 0.11:
                        total_mismatches.append((local_name, expected_value, actual_value))
                if total_mismatches:
                    print("[WARN] Nutrition total mismatch after save:")
                    for name, expected_value, actual_value in total_mismatches:
                        print(f"  - {name}: expected {expected_value}, NTFY has {actual_value}")
                else:
                    print("[OK] Nutrition totals match NTFY aggregates.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
