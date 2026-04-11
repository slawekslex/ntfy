from __future__ import annotations

import os
import unittest
from argparse import Namespace

from meal_chooser_web import create_app


class AppAuthTestCase(unittest.TestCase):
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

    def test_page_routes_redirect_to_login_when_not_authenticated(self) -> None:
        response = self.client.get("/day?date=2026-04-11")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/login?next=/day?date%3D2026-04-11")

    def test_api_routes_return_unauthorized_when_not_authenticated(self) -> None:
        response = self.client.get("/api/chooser?date=2026-04-11")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json(), {"error": "Authentication required."})

    def test_login_sets_authenticated_session(self) -> None:
        response = self.client.post(
            "/login",
            data={"password": "test-password", "next": "/day?date=2026-04-11"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/day?date=2026-04-11")
        with self.client.session_transaction() as current_session:
            self.assertTrue(current_session["authenticated"])
            self.assertTrue(current_session.permanent)

    def test_login_rejects_wrong_password(self) -> None:
        response = self.client.post("/login", data={"password": "wrong"})

        self.assertEqual(response.status_code, 401)
        self.assertIn(b"Wrong password.", response.data)

    def test_nela_refresh_redirects_back_to_current_start(self) -> None:
        with self.client.session_transaction() as current_session:
            current_session["authenticated"] = True
            current_session.permanent = True

        response = self.client.post("/nela/refresh", data={"start": "2026-04-20"})

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/nela?start=2026-04-20")


if __name__ == "__main__":
    unittest.main()
