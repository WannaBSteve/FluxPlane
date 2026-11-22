"""Sample the worker process while it executes one task."""

from __future__ import annotations

import os
import resource
import sys
import time
from pathlib import Path
from threading import Event, Thread
from typing import Self


def process_rss_mb() -> float | None:
    """Return current RSS on Linux and peak RSS on other supported Unix hosts."""
    try:
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
    except (OSError, IndexError, ValueError):
        try:
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            # macOS reports bytes; Linux and most BSD variants report KiB.  Linux
            # normally takes the /proc path above, but keep its documented unit here.
            divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
            return peak / divisor
        except (OSError, ValueError):
            return None


class RuntimeSampler:
    """Attribute process CPU and sampled RSS to a single worker task."""

    def __init__(self, interval_seconds: float = 0.025) -> None:
        self.interval_seconds = interval_seconds
        self.cpu_ms = 0.0
        self.peak_rss_mb: float | None = None
        self._stop = Event()
        self._thread: Thread | None = None
        self._cpu_start = 0.0

    def _sample(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._record_rss()

    def _record_rss(self) -> None:
        current = process_rss_mb()
        if current is not None:
            self.peak_rss_mb = max(self.peak_rss_mb or 0.0, current)

    def __enter__(self) -> Self:
        self._cpu_start = time.process_time()
        self._record_rss()
        self._thread = Thread(target=self._sample, name="worker-memory-sample", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1)
        self._record_rss()
        self.cpu_ms = max(0.0, (time.process_time() - self._cpu_start) * 1000)
