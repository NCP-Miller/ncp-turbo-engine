"""Thread-safe rate limiter for API calls."""

import threading
import time


class RateLimiter:
    """Thread-safe rate limiter. Call .wait() before each API call."""

    def __init__(self, calls_per_second):
        self._min_interval = 1.0 / calls_per_second
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# DEFAULT SHARED LIMITERS
# ---------------------------------------------------------------------------
# Import these where needed and call .wait() before any API call.
# The pipeline will share these across threads.
openai_limiter = RateLimiter(calls_per_second=8)      # ~480/min
apollo_limiter = RateLimiter(calls_per_second=5)      # ~300/min
firecrawl_limiter = RateLimiter(calls_per_second=3)   # ~180/min