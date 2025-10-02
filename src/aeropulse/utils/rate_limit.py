import os
import time
from datetime import datetime, timezone, date


class DailyBudget:
    """
    Super-simple in-process daily budget.
    Not distributed; for single-run jobs it’s perfect.
    If you need distributed, back it with Postgres (table) or Redis.
    """

    def __init__(self, daily_limit: int, min_interval_sec: float = 0.0):
        self.daily_limit = daily_limit
        self.min_interval = min_interval_sec
        self.day = date.today()
        self.used = 0
        self.last_ts = 0.0

    def remaining(self) -> int:
        if date.today() != self.day:
            self.day = date.today()
            self.used = 0
        return max(0, self.daily_limit - self.used)

    def consume(self, n: int = 1):
        if date.today() != self.day:
            self.day = date.today()
            self.used = 0
        self.used += n

    def wait_min_interval(self):
        if self.min_interval <= 0:
            return
        now = time.time()
        delta = now - self.last_ts
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self.last_ts = time.time()


def env_daily_budget(default: int = 900) -> int:
    raw = os.getenv("OPENWEATHER_DAILY_BUDGET")
    try:
        val = int(raw) if raw else default
    except Exception:
        val = default
    return max(0, val)
