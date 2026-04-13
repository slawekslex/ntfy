from __future__ import annotations

import base64
import json
import os
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path

from meal_chooser_web import (
    create_app,
    format_livekid_menu,
    livekid_has_obiad,
    livekid_kid_id,
    livekid_meal_name_from_payloads,
    load_nela_favourite_entries,
    load_nela_favourite_product_ids,
    meal_names_fuzzy_match,
    nela_livekid_favourite_product_id,
    nela_meal_favourite_matches,
    nela_obiad_name,
    obiad_meal_from_rows_by_meal,
    select_first_meal_name,
    select_page_first_meal_name,
)


def make_delivery_payload(meals: list[dict]) -> dict:
    delivery_id = 101
    diet_variant_meals = []
    diet_variant_meal_types = []
    delivery_items = []
    simple_products = []

    for index, meal in enumerate(meals, start=1):
        meal_type_id = 1000 + index
        meal_id = 2000 + index
        product_id = 3000 + index

        diet_variant_meal_types.append(
            {
                "id": meal_type_id,
                "meal_name": {
                    "key": meal["key"],
                    "value": meal["label"],
                },
            }
        )
        diet_variant_meals.append(
            {
                "id": meal_id,
                "diet_variant_meal_type_id": meal_type_id,
            }
        )
        delivery_items.append(
            {
                "id": 4000 + index,
                "delivery_id": delivery_id,
                "diet_variant_meal_id": meal_id,
                "simple_product_id": product_id,
            }
        )
        simple_products.append(
            {
                "id": product_id,
                "name": meal["product_name"],
                "images": [],
            }
        )

    return {
        "results": [{"id": delivery_id}],
        "includes": {
            "delivery_items": delivery_items,
            "diet_variant_meals": diet_variant_meals,
            "diet_variant_meal_types": diet_variant_meal_types,
            "simple_products": simple_products,
            "alternative_meals": [],
        },
    }


def make_select_page_payload(product_name: str) -> dict:
    return {
        "results": [{"id": 101}],
        "includes": {
            "delivery_items": [
                {
                    "id": 9001,
                    "delivery_id": 101,
                    "diet_variant_meal_id": None,
                    "delivery_diet_id": None,
                    "simple_product_id": 3001,
                    "related_item_type": "ITEM",
                    "related_item_id": 1234,
                    "is_simple_product_selected_by_user": True,
                }
            ],
            "simple_products": [
                {
                    "id": 3001,
                    "name": product_name,
                    "images": [],
                }
            ],
            "diet_variant_meals": [],
            "diet_variant_meal_types": [],
            "alternative_meals": [],
        },
    }


def make_livekid_presence_payload(*meal_names: str) -> dict:
    return {
        "day": "2026-04-14",
        "meals": [{"name": meal_name} for meal_name in meal_names],
    }


def make_livekid_token(*, kid: int) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"kid": kid}).encode("utf-8")).decode("ascii").rstrip("=")
    return f"header.{payload}.signature"


class NelaPageLogicTestCase(unittest.TestCase):
    def test_nela_obiad_name_returns_selected_lunch(self) -> None:
        payload = make_delivery_payload(
            [
                {"key": "LUNCH", "label": "Obiad", "product_name": "Pomidorowa"},
                {"key": "DINNER", "label": "Kolacja", "product_name": "Kanapki"},
            ]
        )

        self.assertEqual(nela_obiad_name(payload), "Pomidorowa")

    def test_obiad_meal_from_rows_by_meal_returns_lunch_bucket(self) -> None:
        from ntfy_meals_lib import build_rows_by_meal

        payload = make_delivery_payload(
            [
                {"key": "BREAKFAST", "label": "Śniadanie", "product_name": "Owsianka"},
                {"key": "LUNCH", "label": "Obiad", "product_name": "Zupa"},
            ]
        )
        _, rows_by_meal = build_rows_by_meal(payload)
        label, rows = obiad_meal_from_rows_by_meal(rows_by_meal)

        self.assertEqual(label, "Obiad")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("name"), "Zupa")

    def test_nela_obiad_name_returns_none_when_lunch_missing(self) -> None:
        payload = make_delivery_payload(
            [
                {"key": "BREAKFAST", "label": "Śniadanie", "product_name": "Owsianka"},
            ]
        )

        self.assertIsNone(nela_obiad_name(payload))

    def test_select_first_meal_name_returns_first_ordered_meal(self) -> None:
        payload = make_delivery_payload(
            [
                {"key": "LUNCH", "label": "Obiad", "product_name": "Makaron"},
                {"key": "BREAKFAST", "label": "Śniadanie", "product_name": "Jajecznica"},
            ]
        )

        self.assertEqual(select_first_meal_name(payload), "Jajecznica")

    def test_select_page_first_meal_name_returns_selected_replacement_item(self) -> None:
        payload = make_select_page_payload(
            "Kotlety drobiowe z cukinią z sosem koperkowym, pieczone ziemniaki, pomidor z cebulką i śmietaną"
        )

        self.assertEqual(
            select_page_first_meal_name(payload),
            "Kotlety drobiowe z cukinią z sosem koperkowym, pieczone ziemniaki, pomidor z cebulką i śmietaną",
        )

    def test_livekid_has_obiad_detects_lunch_presence(self) -> None:
        self.assertTrue(livekid_has_obiad(make_livekid_presence_payload("Śniadanie", "Obiad")))
        self.assertFalse(livekid_has_obiad(make_livekid_presence_payload("Podwieczorek")))

    def test_livekid_has_obiad_ignores_weekend_default_lunch(self) -> None:
        self.assertFalse(livekid_has_obiad({"day": "2026-04-12", "meals": [{"name": "Obiad"}]}))

    def test_format_livekid_menu_joins_zupa_and_drugie(self) -> None:
        self.assertEqual(
            format_livekid_menu({"zupa": "Rosół", "drugie": "Kotlet z ziemniakami"}),
            "Rosół | Kotlet z ziemniakami",
        )

    def test_livekid_meal_name_from_payloads_returns_brak_menu_when_presence_has_obiad_but_menu_missing(self) -> None:
        self.assertEqual(
            livekid_meal_name_from_payloads(make_livekid_presence_payload("Obiad"), {"status": "missing"}),
            "Brak Menu",
        )

    def test_livekid_meal_name_from_payloads_returns_none_without_obiad(self) -> None:
        self.assertIsNone(
            livekid_meal_name_from_payloads(
                make_livekid_presence_payload("Śniadanie"),
                {"zupa": "Rosół", "drugie": "Kotlet z ziemniakami"},
            )
        )

    def test_livekid_meal_name_brak_menu_when_zupa_and_drugie_empty(self) -> None:
        self.assertEqual(
            livekid_meal_name_from_payloads(
                make_livekid_presence_payload("Obiad"),
                {"zupa": "", "drugie": ""},
            ),
            "Brak Menu",
        )

    def test_livekid_meal_name_single_course_for_nela_opcje_line(self) -> None:
        self.assertEqual(
            livekid_meal_name_from_payloads(make_livekid_presence_payload("Obiad"), {"zupa": "Rosół"}),
            "Rosół",
        )

    def test_meal_names_fuzzy_match_tolerates_minor_typo(self) -> None:
        a = "Kotlet schabowy z ziemniakami i surówką z kapusty"
        b = "Kotlet schabowy z ziemniakami i surówką z kapusty "  # trailing space normalized
        self.assertTrue(meal_names_fuzzy_match(a, b))
        c = "Kotlet schabowy z ziemniakami i surówką z kapusty."  # punctuation dropped in normalize? we don't strip period
        # Still long enough; period makes ratio slightly below 1.0 but usually still ≥ 0.9
        self.assertTrue(meal_names_fuzzy_match(a, c))

    def test_meal_names_fuzzy_match_rejects_short_strings(self) -> None:
        self.assertFalse(meal_names_fuzzy_match("abc", "abcd"))

    def test_nela_meal_favourite_matches_by_name_when_id_differs(self) -> None:
        entries = [
            {
                "simple_product_id": "old-id",
                "meal_name": "Pierś z kurczaka w sosie pieczarkowym, ryż, surówka",
            }
        ]
        opt = {
            "simple_product_id": "new-id",
            "name": "Pierś z kurczaka w sosie pieczarkowym, ryż, surówka",
        }
        self.assertTrue(nela_meal_favourite_matches(opt, entries))

    def test_nela_livekid_favourite_product_id_is_per_date(self) -> None:
        self.assertEqual(nela_livekid_favourite_product_id("2026-04-14"), "livekid:2026-04-14")

    def test_nela_meal_favourite_matches_livekid_synthetic_id(self) -> None:
        entries = [
            {
                "simple_product_id": "livekid:2026-04-14",
                "meal_name": "Rosół | Kotlet schabowy",
            }
        ]
        opt = {
            "simple_product_id": "livekid:2026-04-14",
            "name": "Rosół | Kotlet schabowy",
            "livekid": True,
        }
        self.assertTrue(nela_meal_favourite_matches(opt, entries))

    def test_livekid_kid_id_reads_kid_from_bearer_token(self) -> None:
        env_vars = {"LIVEKID_BEARER_TOKEN": make_livekid_token(kid=76591247)}
        previous_token = os.environ.pop("LIVEKID_BEARER_TOKEN", None)
        try:
            self.assertEqual(livekid_kid_id(env_vars), 76591247)
        finally:
            if previous_token is not None:
                os.environ["LIVEKID_BEARER_TOKEN"] = previous_token


class NelaPageAuthTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.previous_password = os.environ.get("APP_PASSWORD")
        os.environ["APP_PASSWORD"] = "test-password"
        self.app = create_app(
            Namespace(
                date="2026-04-11",
                diet_name="Test",
                config="config.json",
                cookies=None,
                host="127.0.0.1",
                port=5000,
            )
        )
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        if self.previous_password is None:
            os.environ.pop("APP_PASSWORD", None)
        else:
            os.environ["APP_PASSWORD"] = self.previous_password

    def test_nela_route_redirects_to_login_when_not_authenticated(self) -> None:
        response = self.client.get("/nela")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/login?next=/nela")


class NelaFavouritesTestCase(unittest.TestCase):
    def test_load_nela_favourite_entries_reads_version_one_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fav.json"
            path.write_text(
                json.dumps({"version": 1, "simple_product_ids": ["a", "b"]}),
                encoding="utf-8",
            )
            entries = load_nela_favourite_entries(path)
        self.assertEqual(
            entries,
            [
                {"simple_product_id": "a", "meal_name": ""},
                {"simple_product_id": "b", "meal_name": ""},
            ],
        )

    def setUp(self) -> None:
        self.previous_password = os.environ.get("APP_PASSWORD")
        os.environ["APP_PASSWORD"] = "test-password"
        self._tmpdir = tempfile.TemporaryDirectory()
        self.favourites_path = Path(self._tmpdir.name) / "nela_meal_favourites.json"
        self.app = create_app(
            Namespace(
                date="2026-04-11",
                diet_name="Test",
                config="config.json",
                cookies=None,
                host="127.0.0.1",
                port=5000,
            ),
            nela_favourites_path=self.favourites_path,
        )
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session["authenticated"] = True
            session.permanent = True

    def tearDown(self) -> None:
        self._tmpdir.cleanup()
        if self.previous_password is None:
            os.environ.pop("APP_PASSWORD", None)
        else:
            os.environ["APP_PASSWORD"] = self.previous_password

    def test_favourite_api_persists_and_removes_product_id(self) -> None:
        response = self.client.post(
            "/api/nela/favourite",
            json={
                "simple_product_id": "prod-42",
                "meal_name": "Test meal",
                "favourite": True,
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {"simple_product_id": "prod-42", "meal_name": "Test meal", "favourite": True},
        )
        self.assertEqual(load_nela_favourite_product_ids(self.favourites_path), {"prod-42"})
        entries = load_nela_favourite_entries(self.favourites_path)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["meal_name"], "Test meal")

        response = self.client.post(
            "/api/nela/favourite",
            json={
                "simple_product_id": "prod-42",
                "meal_name": "Test meal",
                "favourite": False,
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(load_nela_favourite_product_ids(self.favourites_path), set())

    def test_favourite_api_rejects_invalid_payload(self) -> None:
        response = self.client.post("/api/nela/favourite", json={"favourite": True})
        self.assertEqual(response.status_code, 400)

        response = self.client.post("/api/nela/favourite", json={"simple_product_id": "x", "favourite": "yes"})
        self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
