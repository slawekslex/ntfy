from __future__ import annotations

import argparse
import os
from datetime import date, timedelta

from meal_chooser_web import create_app


def default_date() -> str:
    return (date.today() + timedelta(days=2)).isoformat()


def build_args() -> argparse.Namespace:
    port = int(os.getenv("PORT", os.getenv("NTFY_PORT", "10000")))
    return argparse.Namespace(
        date=os.getenv("NTFY_DATE", default_date()),
        diet_name=os.getenv("NTFY_DIET_NAME", "Slex"),
        config=os.getenv("NTFY_CONFIG", "config.json"),
        cookies=os.getenv("NTFY_COOKIES"),
        host="0.0.0.0",
        port=port,
    )


app = create_app(build_args())
