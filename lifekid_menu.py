from __future__ import annotations

import base64
from contextlib import contextmanager
import fcntl
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field


API_URL = "https://pl.api.api-livekid-prod.com/v1/menus"
TOKEN_ENV_VAR = "LIVEKID_BEARER_TOKEN"
MAIL_ENV_VAR = "LIVEKID_MAIL"
PASSWORD_ENV_VAR = "LIVEKID_PASSWORD"
OPENAI_API_KEY_ENV_VAR = "OPENAI_API_KEY"
OPENAI_MODEL_ENV_VAR = "OPENAI_MODEL"
DATA_DIR_ENV_VAR = "LIVEKID_DATA_DIR"
DEFAULT_DATA_DIR = ".data"
DEFAULT_OUTPUT_DIR = "livekid_menus"
CACHE_FILE = ".livekid_token_cache.json"
MENU_CACHE_FILE = ".livekid_menu_cache.json"
DEFAULT_OPENAI_MODEL = "gpt-5.4-mini"
MENU_MISSING_STATUS = "missing"
MISSING_MENU_TTL_SECONDS = 12 * 60 * 60


class LiveKidMenuError(RuntimeError):
    """Base error for LiveKid menu helpers."""


class LiveKidTokenExpiredError(LiveKidMenuError):
    """Raised when the bearer token is expired or invalid."""


@dataclass
class LiveKidSession:
    token: str


@dataclass
class LiveKidMenuFile:
    menu_id: int
    title: str
    day: str
    file_url: str
    seen: bool


class ParsedDayMenu(BaseModel):
    date: str = Field(description="Date in YYYY-MM-DD format.")
    zupa: str = Field(description="Soup description for that day.")
    drugie: str = Field(description="Main course description for that day.")


class ParsedMenuCollection(BaseModel):
    menus: list[ParsedDayMenu] = Field(description="One parsed menu entry per day.")


class CachedMissingMenu(BaseModel):
    status: str = Field(default=MENU_MISSING_STATUS)
    expires_at: int = Field(description="Unix timestamp when the missing marker expires.")


def load_env_file(path: str = ".env") -> dict[str, str]:
    env_vars: dict[str, str] = {}
    env_path = Path(path)
    if not env_path.exists():
        return env_vars

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        env_vars[key.strip()] = value
    return env_vars


def resolve_data_dir() -> Path:
    return Path(os.getenv(DATA_DIR_ENV_VAR, DEFAULT_DATA_DIR))


def resolve_output_dir(path: str | os.PathLike[str] | None = None) -> Path:
    if path is not None:
        return Path(path)
    return resolve_data_dir() / DEFAULT_OUTPUT_DIR


def resolve_cache_path(path: str | os.PathLike[str] | None, *, default_name: str) -> Path:
    if path is not None:
        return Path(path)
    return resolve_data_dir() / default_name


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


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_path, path)


def get_bearer_token(env_path: str = ".env") -> str:
    env_vars = load_env_file(env_path)
    token = os.getenv(TOKEN_ENV_VAR) or env_vars.get(TOKEN_ENV_VAR)
    if token:
        return token.strip()
    raise LiveKidMenuError(
        f"Missing {TOKEN_ENV_VAR}. Add it to {env_path} or export it in the environment."
    )


def livekid_credentials_from_env(env_path: str = ".env") -> tuple[str | None, str | None]:
    env_vars = load_env_file(env_path)
    mail = os.getenv(MAIL_ENV_VAR) or env_vars.get(MAIL_ENV_VAR)
    password = os.getenv(PASSWORD_ENV_VAR) or env_vars.get(PASSWORD_ENV_VAR)
    return mail, password


def openai_settings_from_env(env_path: str = ".env") -> tuple[str | None, str]:
    env_vars = load_env_file(env_path)
    api_key = os.getenv(OPENAI_API_KEY_ENV_VAR) or env_vars.get(OPENAI_API_KEY_ENV_VAR)
    model = os.getenv(OPENAI_MODEL_ENV_VAR) or env_vars.get(OPENAI_MODEL_ENV_VAR) or DEFAULT_OPENAI_MODEL
    return api_key, model


def validate_date(day: str) -> str:
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Invalid date format. Expected YYYY-MM-DD.") from exc
    return day


def _auth_headers(token: str, *, accept: str = "application/json") -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": accept,
        "User-Agent": "lifekid-menu-helper/1.0",
    }


def _raise_for_auth_error(response: requests.Response) -> None:
    if response.status_code not in {401, 403}:
        return

    body = response.text[:300].strip()
    raise LiveKidTokenExpiredError(
        "LiveKid bearer token expired or is no longer valid. "
        f"Update {TOKEN_ENV_VAR} in .env and try again. "
        f"(HTTP {response.status_code}, body starts with: {body!r})"
    )


def load_token_cache(path: str | os.PathLike[str] | None = None) -> str | None:
    cache_path = resolve_cache_path(path, default_name=CACHE_FILE)
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:  # pylint: disable=broad-except
        return None
    token = payload.get("token")
    if isinstance(token, str) and token.strip():
        return token.strip()
    return None


def save_token_cache(token: str, path: str | os.PathLike[str] | None = None) -> None:
    cache_path = resolve_cache_path(path, default_name=CACHE_FILE)
    payload = {
        "token": token,
        "cached_at": datetime.now().isoformat(timespec="seconds"),
    }
    with file_lock(lock_path_for(cache_path)):
        write_json_atomic(cache_path, payload)


def load_menu_cache(path: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    cache_path = resolve_cache_path(path, default_name=MENU_CACHE_FILE)
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:  # pylint: disable=broad-except
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def save_menu_cache(cache: dict[str, Any], path: str | os.PathLike[str] | None = None) -> None:
    cache_path = resolve_cache_path(path, default_name=MENU_CACHE_FILE)
    with file_lock(lock_path_for(cache_path)):
        write_json_atomic(cache_path, cache)


def missing_menu_marker() -> dict[str, str]:
    return {"status": MENU_MISSING_STATUS}


def cached_missing_menu_marker(*, ttl_seconds: int = MISSING_MENU_TTL_SECONDS) -> dict[str, Any]:
    return {
        "status": MENU_MISSING_STATUS,
        "expires_at": int(time.time()) + ttl_seconds,
    }


def _cache_menu_value(
    day: str,
    value: dict[str, Any],
    *,
    path: str | os.PathLike[str] | None = None,
) -> None:
    cache_path = resolve_cache_path(path, default_name=MENU_CACHE_FILE)
    with file_lock(lock_path_for(cache_path)):
        cache = load_menu_cache(cache_path)
        cache[day] = value
        write_json_atomic(cache_path, cache)


def _read_cached_day(day: str, *, path: str | os.PathLike[str] | None = None) -> dict[str, str] | None:
    cache_path = resolve_cache_path(path, default_name=MENU_CACHE_FILE)
    with file_lock(lock_path_for(cache_path)):
        cache = load_menu_cache(cache_path)
        value = cache.get(day)
        if not isinstance(value, dict):
            return None
        if value.get("status") == MENU_MISSING_STATUS:
            expires_at = value.get("expires_at")
            if isinstance(expires_at, int) and expires_at > int(time.time()):
                return missing_menu_marker()
            cache.pop(day, None)
            write_json_atomic(cache_path, cache)
            return None
        if "zupa" in value and "drugie" in value:
            return {
                "zupa": str(value["zupa"]),
                "drugie": str(value["drugie"]),
            }
        return None


def login_via_api(mail: str, password: str) -> LiveKidSession:
    print("[AUTH] Logging in via LiveKid API...")
    response = requests.post(
        "https://pl.api.api-livekid-prod.com/v1/accounts/login",
        json={"mail": mail, "password": password},
        headers={"Accept": "application/json", "User-Agent": "lifekid-menu-helper/1.0"},
        timeout=30,
    )
    response.raise_for_status()

    data: dict[str, Any] = response.json()
    roles = data.get("roles") or []
    for role in roles:
        token = role.get("jwt")
        if isinstance(token, str) and token.strip():
            return LiveKidSession(token=token.strip())

    raise LiveKidMenuError("LiveKid login succeeded, but no JWT was returned in roles[].jwt.")


class LiveKidClient:
    def __init__(self, *, explicit_token: str | None = None, env_path: str = ".env") -> None:
        env_vars = load_env_file(env_path)
        self.explicit_token = explicit_token
        self.env_token = os.getenv(TOKEN_ENV_VAR) or env_vars.get(TOKEN_ENV_VAR)
        self.mail = os.getenv(MAIL_ENV_VAR) or env_vars.get(MAIL_ENV_VAR)
        self.password = os.getenv(PASSWORD_ENV_VAR) or env_vars.get(PASSWORD_ENV_VAR)
        self.session: LiveKidSession | None = None

    @property
    def token(self) -> str:
        if self.session is None:
            raise LiveKidMenuError("LiveKid client is not authenticated.")
        return self.session.token

    def ensure_authenticated(self, *, force_refresh: bool = False) -> LiveKidSession:
        if not force_refresh and self.session is not None:
            return self.session

        if not force_refresh and self.explicit_token:
            print("[AUTH] Using token provided explicitly.")
            self.session = LiveKidSession(token=self.explicit_token.strip())
            return self.session

        if not force_refresh:
            cached_token = load_token_cache()
            if cached_token:
                print(f"[AUTH] Using cached token from {CACHE_FILE}.")
                self.session = LiveKidSession(token=cached_token)
                return self.session

        if not force_refresh and self.env_token:
            print("[AUTH] Using token from environment/.env.")
            self.session = LiveKidSession(token=self.env_token.strip())
            save_token_cache(self.session.token)
            return self.session

        if self.mail and self.password:
            self.session = login_via_api(self.mail, self.password)
            save_token_cache(self.session.token)
            print(f"[AUTH] Saved fresh token to {CACHE_FILE}.")
            return self.session

        raise LiveKidMenuError(
            "No valid LiveKid credentials available. Set LIVEKID_MAIL/LIVEKID_PASSWORD "
            f"or {TOKEN_ENV_VAR} in .env."
        )

    def request(
        self,
        method: str,
        url: str,
        *,
        accept: str,
        params: dict[str, str] | None = None,
        timeout: int = 30,
        retry_on_unauthorized: bool = True,
    ) -> requests.Response:
        self.ensure_authenticated()
        response = requests.request(
            method=method,
            url=url,
            params=params,
            headers=_auth_headers(self.token, accept=accept),
            timeout=timeout,
        )
        if response.status_code in {401, 403} and retry_on_unauthorized:
            print("[AUTH] LiveKid token was rejected, refreshing it...")
            self.ensure_authenticated(force_refresh=True)
            response = requests.request(
                method=method,
                url=url,
                params=params,
                headers=_auth_headers(self.token, accept=accept),
                timeout=timeout,
            )
        _raise_for_auth_error(response)
        response.raise_for_status()
        return response


def get_menu_metadata_for_day(
    day: str,
    *,
    token: str | None = None,
    client: LiveKidClient | None = None,
) -> LiveKidMenuFile:
    validated_day = validate_date(day)
    api_client = client or LiveKidClient(explicit_token=token)
    response = api_client.request(
        "GET",
        API_URL,
        params={"day": validated_day},
        accept="application/json",
        timeout=30,
    )

    payload: list[dict[str, Any]] = response.json()
    if not payload:
        raise LiveKidMenuError(f"No menu entries were returned for {validated_day}.")

    menu = payload[0]
    file_url = menu.get("file")
    if not file_url:
        raise LiveKidMenuError(f"The menu entry for {validated_day} does not contain a file URL.")

    return LiveKidMenuFile(
        menu_id=int(menu["id"]),
        title=str(menu.get("title", "")),
        day=validated_day,
        file_url=str(file_url),
        seen=bool(menu.get("seen", False)),
    )


def find_menu_metadata_for_day(
    day: str,
    *,
    token: str | None = None,
    client: LiveKidClient | None = None,
) -> LiveKidMenuFile | None:
    validated_day = validate_date(day)
    api_client = client or LiveKidClient(explicit_token=token)
    response = api_client.request(
        "GET",
        API_URL,
        params={"day": validated_day},
        accept="application/json",
        timeout=30,
    )

    payload: list[dict[str, Any]] = response.json()
    if not payload:
        return None

    menu = payload[0]
    file_url = menu.get("file")
    if not file_url:
        return None

    return LiveKidMenuFile(
        menu_id=int(menu["id"]),
        title=str(menu.get("title", "")),
        day=validated_day,
        file_url=str(file_url),
        seen=bool(menu.get("seen", False)),
    )


def download_menu_file(
    menu: LiveKidMenuFile,
    *,
    token: str | None = None,
    client: LiveKidClient | None = None,
    output_dir: str | os.PathLike[str] | None = None,
) -> Path:
    api_client = client or LiveKidClient(explicit_token=token)
    output_path = resolve_output_dir(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = Path(menu.file_url.split("?", 1)[0]).name or f"livekid-menu-{menu.day}.pdf"
    destination = output_path / filename
    with file_lock(lock_path_for(destination)):
        if destination.exists() and destination.stat().st_size > 0:
            print(f"[CACHE] Using existing PDF {destination}.")
            return destination
        response = api_client.request(
            "GET",
            menu.file_url,
            accept="application/pdf",
            timeout=60,
        )
        destination.write_bytes(response.content)
    return destination


def download_menu_for_day(
    day: str,
    *,
    token: str | None = None,
    output_dir: str | os.PathLike[str] | None = None,
) -> tuple[LiveKidMenuFile, Path]:
    client = LiveKidClient(explicit_token=token)
    menu = get_menu_metadata_for_day(day, client=client)
    destination = download_menu_file(menu, client=client, output_dir=output_dir)
    return menu, destination


def _build_pdf_parsing_prompt() -> str:
    return (
        "You are parsing a Polish school lunch PDF.\n"
        "Read the attached PDF and extract all meal days into this exact structure:\n"
        '{"YYYY-MM-DD": {"zupa": "OPIS_ZUPY", "drugie": "OPIS_DRUGIEGO"}}\n\n'
        "Rules:\n"
        "- Return one entry for every date visible in the PDF.\n"
        "- Use the date as the top-level key in YYYY-MM-DD format.\n"
        "- 'zupa' should contain only the soup name/description.\n"
        "- 'drugie' should contain the main dish description.\n"
        "- Ignore calories, allergens, grams, drinks, compote, and ingredient lists.\n"
        "- Include side items in 'drugie' only if they are clearly part of the second course.\n"
        "- Keep the text in Polish.\n"
        "- Return only the structured data."
    )


def parse_menu_pdf(
    pdf_path: str | os.PathLike[str],
    *,
    env_path: str = ".env",
    model: str | None = None,
) -> dict[str, dict[str, str]]:
    api_key, configured_model = openai_settings_from_env(env_path)
    if not api_key:
        raise LiveKidMenuError(
            f"Missing {OPENAI_API_KEY_ENV_VAR}. Add it to {env_path} or export it in the environment."
        )

    pdf_file = Path(pdf_path)
    if not pdf_file.exists():
        raise LiveKidMenuError(f"PDF file does not exist: {pdf_file}")
    raw_file_data = base64.b64encode(pdf_file.read_bytes()).decode("ascii")
    file_data = f"data:application/pdf;base64,{raw_file_data}"
    prompt = _build_pdf_parsing_prompt()

    llm = ChatOpenAI(
        model=model or configured_model,
        api_key=api_key,
        temperature=0,
    )
    structured_llm = llm.with_structured_output(ParsedMenuCollection)
    parsed = structured_llm.invoke(
        [
            HumanMessage(
                content=[
                    {"type": "text", "text": prompt},
                    {
                        "type": "file",
                        "file": {
                            "filename": pdf_file.name,
                            "file_data": file_data,
                        },
                    },
                ]
            )
        ]
    )

    result: dict[str, dict[str, str]] = {}
    for menu in parsed.menus:
        result[menu.date] = {
            "zupa": menu.zupa.strip(),
            "drugie": menu.drugie.strip(),
        }

    if not result:
        raise LiveKidMenuError("LLM parsing returned no menu entries.")

    return result


def get_menu_for_day(
    day: str,
    *,
    token: str | None = None,
    output_dir: str | os.PathLike[str] | None = None,
    cache_path: str | os.PathLike[str] | None = None,
    env_path: str = ".env",
    model: str | None = None,
) -> dict[str, str]:
    validated_day = validate_date(day)
    day_lock = resolve_data_dir() / ".locks" / f"menu-{validated_day}.lock"
    with file_lock(day_lock):
        cached_value = _read_cached_day(validated_day, path=cache_path)
        if cached_value is not None:
            print(f"[CACHE] Using cached menu result for {validated_day}.")
            return cached_value

        client = LiveKidClient(explicit_token=token, env_path=env_path)
        menu = find_menu_metadata_for_day(validated_day, client=client)
        if menu is None:
            print(f"[CACHE] Caching missing menu marker for {validated_day} for 12 hours.")
            _cache_menu_value(validated_day, cached_missing_menu_marker(), path=cache_path)
            return missing_menu_marker()

        pdf_path = download_menu_file(menu, client=client, output_dir=output_dir)
        parsed_menus = parse_menu_pdf(pdf_path, env_path=env_path, model=model)
        for parsed_day, parsed_menu in parsed_menus.items():
            _cache_menu_value(parsed_day, parsed_menu, path=cache_path)

        cached_after_parse = _read_cached_day(validated_day, path=cache_path)
        if cached_after_parse is not None:
            return cached_after_parse

        print(f"[CACHE] Parsed PDF did not contain {validated_day}; caching missing marker for 12 hours.")
        _cache_menu_value(validated_day, cached_missing_menu_marker(), path=cache_path)
        return missing_menu_marker()


def _current_day_str() -> str:
    return date.today().isoformat()


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    day = args[0] if args else _current_day_str()

    try:
        menu = get_menu_for_day(day)
    except LiveKidTokenExpiredError as exc:
        print(f"WARNING: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # pylint: disable=broad-except
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(json.dumps({day: menu}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
