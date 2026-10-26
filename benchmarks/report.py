"""Tables of results, in Markdown: throughput of one task, and serialized sizes."""

from __future__ import annotations

import gzip
import statistics
import sys
from collections import defaultdict
from collections.abc import Callable, Sequence
from typing import Any

from benchmarks.measure import CaseResult
from benchmarks.suites import SUITES

# A case is marked when its sweeps differ by more than this, relative to the median
SPREAD_LIMIT = 0.10


def _table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    lines = [
        "| " + " | ".join(header) + " |",
        "|" + "|".join("---" if i == 0 else "--:" for i in range(len(header))) + "|",
    ]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def _geometric_means(
    values: dict[tuple[str, str], float],
    methods: Sequence[str],
    datasets: Sequence[str],
    decimals: int,
) -> list[str]:
    """
    Geometric mean of each method over the datasets it has a value for.

    A method that misses some datasets gets their number in brackets, as its mean
    isn't comparable with the others.
    """
    cells = []
    for method in methods:
        found = [values[d, method] for d in datasets if (d, method) in values]
        if not found:
            cells.append("-")
            continue
        cell = f"{statistics.geometric_mean(found):,.{decimals}f}"
        if len(found) < len(datasets):
            cell += f" ({len(found)}/{len(datasets)})"
        cells.append(cell)
    return cells


def throughput_table(task: str, results: Sequence[CaseResult]) -> str:
    """
    Median throughput of each case over its sweeps, in thousands per second.

    A value marked with `*` has sweeps that differ by more than 10%.
    """
    unit = "statements"
    scores: dict[tuple[str, str], list[float]] = defaultdict(list)
    datasets: list[str] = []
    methods: list[str] = []
    for result in results:
        if result.task != task:
            continue
        unit = SUITES[result.suite].unit
        if result.dataset not in datasets:
            datasets.append(result.dataset)
        if result.method not in methods:
            methods.append(result.method)
        if result.score is not None:
            scores[result.dataset, result.method].append(result.score / 1000)

    medians = {case: statistics.median(values) for case, values in scores.items()}
    rows = []
    for dataset in datasets:
        row = [dataset]
        for method in methods:
            values = scores.get((dataset, method))
            if not values:
                row.append("-")
                continue
            median = medians[dataset, method]
            spread = (max(values) - min(values)) / median
            mark = "*" if spread > SPREAD_LIMIT else ""
            row.append(f"{median:,.1f}{mark}")
        rows.append(row)
    rows.append(
        ["**geometric mean**", *_geometric_means(medians, methods, datasets, 1)]
    )
    title = f"### {task.capitalize()} throughput (thousands of {unit} per second)"
    return f"{title}\n\n{_table(['dataset', *methods], rows)}"


def _compressors() -> dict[str, Callable[[bytes], bytes]]:
    if sys.version_info >= (3, 14):
        # Only on Python 3.14 and newer
        from compression import zstd  # noqa: PLC0415

        def zstd_compress(data: bytes) -> bytes:
            return zstd.compress(data, level=3)
    else:
        # Only installed on older Python versions
        import zstandard  # type: ignore[import-not-found,unused-ignore]  # noqa: PLC0415

        def zstd_compress(data: bytes) -> bytes:
            compressed: bytes = zstandard.ZstdCompressor(level=3).compress(data)
            return compressed

    return {
        "plain": lambda data: data,
        "gzip": lambda data: gzip.compress(data, compresslevel=6),
        "zstd": zstd_compress,
    }


def size_tables(
    datasets: Sequence[str], methods: Sequence[str], rows: int, suite: str = "rdf"
) -> str:
    """
    Build tables of serialized sizes in bytes per statement (or cell).

    Always divided by the number of statements given to the writer, also when the
    writer dropped duplicates.
    """
    benchmarks = SUITES[suite]
    unit = benchmarks.unit.removesuffix("s")
    compressors = _compressors()
    sizes: dict[str, dict[tuple[str, str], float]] = defaultdict(dict)
    for dataset in datasets:
        data = benchmarks.load(dataset, rows)
        # Number of statements (or cells) given to the writer
        count = data.cells if suite == "sparql" else rows
        for name in methods:
            method = benchmarks.methods[name]
            if not method.supports(data) or not getattr(method, "can_write", True):
                continue
            written = method.write_bytes(data)
            for compression, compress in compressors.items():
                size = len(compress(written)) / count
                sizes[compression][dataset, name] = size
        print(f"sizes: {dataset} done", file=sys.stderr)

    tables = []
    for compression, values in sizes.items():
        table_rows: list[list[str]] = []
        for dataset in datasets:
            row = [dataset]
            row += [
                f"{values[dataset, m]:,.2f}" if (dataset, m) in values else "-"
                for m in methods
            ]
            table_rows.append(row)
        table_rows.append(
            ["**geometric mean**", *_geometric_means(values, methods, datasets, 2)]
        )
        title = f"### Size, {compression} (bytes per {unit})"
        tables.append(f"{title}\n\n{_table(['dataset', *methods], table_rows)}")
    return "\n\n".join(tables)


def results_from_json(content: dict[str, Any]) -> list[CaseResult]:
    return [CaseResult(**result) for result in content["results"]]
