import os
import time
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_CURRENT_BASE = "https://api.openweathermap.org/data/2.5/weather"


def _redact(url: str) -> str:
    """Remove appid from URLs for safe logging."""
    import re

    return re.sub(r"(appid=)[^&]+", r"\\1<redacted>", url or "")


class OpenWeatherClient:
    """
    Minimal client for OpenWeather 'Current Weather Data' endpoint.
    Uses a single API key (no One Call 3.0).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        current_base: str = DEFAULT_CURRENT_BASE,
        timeout: float = 15.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self.api_key = api_key or os.getenv("OPENWEATHER_API_KEY")
        if not self.api_key:
            raise RuntimeError("Set OPENWEATHER_API_KEY in your environment.")

        self.current_base = current_base
        self.timeout = timeout

        self.session = requests.Session()
        retries = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def current(
        self,
        lat: float,
        lon: float,
        *,
        units: str = "standard",
        lang: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Current Weather Data:
        https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={KEY}
        """
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": units,
        }
        if lang:
            params["lang"] = lang

        resp = self.session.get(self.current_base, params=params, timeout=self.timeout)

        # Retry-After handling for 429
        if resp.status_code == 429 and "Retry-After" in resp.headers:
            try:
                wait = int(resp.headers["Retry-After"])
                time.sleep(min(wait, 60))
                resp = self.session.get(
                    self.current_base, params=params, timeout=self.timeout
                )
            except Exception:
                pass

        # Unauthorized → raise with safe URL (no key)
        if resp.status_code == 401:
            detail = (
                resp.json()
                if resp.headers.get("content-type", "").startswith("application/json")
                else {"message": resp.text[:200]}
            )
            raise RuntimeError(
                f"OpenWeather 401 Unauthorized (Current). url={_redact(getattr(resp, 'url', ''))} detail={detail}"
            )

        resp.raise_for_status()
        return resp.json()
