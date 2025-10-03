# src/aeropulse/services/opensky_client.py

import os
import time
from typing import Dict, Optional, List, Tuple
import requests
from dotenv import load_dotenv

from aeropulse.utils.logging_config import setup_logger

load_dotenv()
logger = setup_logger(__name__)

OPENSKY_AUTH_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_API_BASE = "https://opensky-network.org/api"

# simple in-memory token cache
_token_cache: Dict[str, float | str | None] = {
    "access_token": None,
    "expires_at": 0.0,
}


def _get_access_token() -> str:
    now = time.time()
    if _token_cache["access_token"] and now < float(_token_cache["expires_at"]):
        return str(_token_cache["access_token"])

    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError(
            "Set OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET in your environment"
        )

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(OPENSKY_AUTH_URL, data=data, headers=headers, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    token = payload["access_token"]
    expires_in = float(payload.get("expires_in", 1800.0))
    # keep 60s safety buffer
    _token_cache["access_token"] = token
    _token_cache["expires_at"] = now + max(60.0, expires_in - 60.0)
    return token


def _auth_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {_get_access_token()}"}


def get_states_all(
    *,
    lamin: Optional[float] = None,
    lomin: Optional[float] = None,
    lamax: Optional[float] = None,
    lomax: Optional[float] = None,
    time_sec: Optional[int] = None,
    icao24: Optional[List[str]] = None,
    extended: bool = False,
) -> Dict:
    """
    Wrapper for GET /states/all (authenticated).
    Returns raw JSON.
    """
    params: List[Tuple[str, object]] = []
    if time_sec is not None:
        params.append(("time", int(time_sec)))
    if None not in (lamin, lomin, lamax, lomax):
        params.extend(
            [
                ("lamin", float(lamin)),
                ("lomin", float(lomin)),
                ("lamax", float(lamax)),
                ("lomax", float(lomax)),
            ]
        )
    if extended:
        params.append(("extended", 1))
    if icao24:
        for a in icao24:
            params.append(("icao24", a))

    url = f"{OPENSKY_API_BASE}/states/all"
    r = requests.get(url, headers=_auth_headers(), params=params, timeout=60)
    r.raise_for_status()
    return r.json()
