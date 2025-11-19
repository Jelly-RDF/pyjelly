from __future__ import annotations

import logging

from pytest_benchmark.fixture import BenchmarkFixture  # type: ignore[import-not-found]

log = logging.getLogger(__name__)


def print_throughput(bench: BenchmarkFixture, n_statements: int, label: str) -> None:
    mean_s = None
    st = getattr(bench, "stats", None)
    if isinstance(st, dict):
        mean_s = st.get("stats", {}).get("mean", None)
    else:
        inner = getattr(st, "stats", None)
        if isinstance(inner, dict):
            mean_s = inner.get("mean", None)

    if mean_s and mean_s > 0:
        tps = n_statements / mean_s
        bench.extra_info["statements"] = n_statements
        bench.extra_info["mean_seconds"] = mean_s
        bench.extra_info["throughput_statements_per_sec"] = tps
        log.info(
            "[%s] N=%s mean=%.6fs throughput=%.2f stmts/s",
            label,
            f"{n_statements:,}",
            mean_s,
            tps,
        )
