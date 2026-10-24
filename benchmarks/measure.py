"""
Measuring throughput: one benchmark case per process, repeated in sweeps.

A case is one task (write or read), one dataset and one method. Like a JMH fork,
it runs in a fresh Python process: warmup iterations, then measured iterations,
each lasting about `--time` seconds. A sweep runs every case once. The result of
a case is the median of its sweeps.
"""

from __future__ import annotations

import cProfile
import gc
import importlib.machinery
import importlib.metadata
import json
import os
import platform
import pstats
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pyjelly.parse.decode
from benchmarks.data import load
from benchmarks.methods import METHODS, NullOutput, round_trip
from pyjelly.options import MAX_VERSION

REPO_DIR = Path(__file__).parent.parent


@dataclass
class CaseResult:
    task: str
    dataset: str
    method: str
    #: Statements done in one operation: every statement given to the writer, or
    #: every statement the reader found in the bytes
    statements: int = 0
    #: Throughput of each iteration, in statements per second
    warmup: list[float] = field(default_factory=list)
    measured: list[float] = field(default_factory=list)
    #: Mean throughput of the measured iterations, None if the case didn't run
    score: float | None = None
    #: Why the case didn't run, if it didn't
    skipped: str | None = None
    #: What the round-trip check found, if anything
    problems: list[str] = field(default_factory=list)
    sweep: int = 0


def _iteration(
    operation: Callable[[], object], statements: int, seconds: float
) -> float:
    """
    Run the operation until `seconds` have passed, and return the throughput.

    The last operation is always finished, so a slow method may run longer.
    """
    # Clear garbage left by setup or the previous iteration, outside the timing
    gc.collect()
    operations = 0
    start = time.perf_counter()
    while True:
        operation()
        operations += 1
        elapsed = time.perf_counter() - start
        if elapsed >= seconds:
            return operations * statements / elapsed


def run_case(  # noqa: PLR0913
    task: str,
    dataset: str,
    method_name: str,
    *,
    rows: int,
    warmup: int,
    iterations: int,
    seconds: float,
    profile: Path | None = None,
) -> CaseResult:
    """Run one case in this process. See the module docstring."""
    result = CaseResult(task, dataset, method_name)
    method = METHODS[method_name]
    data = load(dataset, rows)
    if not method.supports(data):
        result.skipped = "the format can't hold this kind of statements"
        return result

    # Setup, not measured: make the terms, check that the method round-trips the
    # data, and make the bytes to read
    statements = method.statements(data)
    try:
        written, result.problems = round_trip(method, data)
    except Exception as e:  # noqa: BLE001
        result.skipped = f"can't read its own output: {type(e).__name__}: {e}"
        return result
    for problem in result.problems:
        print(
            f"ROUND-TRIP WARNING: {method_name} on {dataset}: {problem}",
            file=sys.stderr,
        )

    operation: Callable[[], object]
    if task == "write":
        output = NullOutput()
        result.statements = len(statements)

        def operation() -> None:
            method.write(data, output)

    else:
        # A writer that drops duplicates leaves fewer statements to read
        result.statements = method.read(written, quads=data.quads)

        def operation() -> int:
            return method.read(written, quads=data.quads)

    # Everything made so far lives until the end. Freezing it keeps the garbage
    # collector from going through it again and again during the measurement.
    gc.collect()
    gc.freeze()

    result.warmup = [
        _iteration(operation, result.statements, seconds) for _ in range(warmup)
    ]
    profiler = cProfile.Profile() if profile else None
    if profiler:
        profiler.enable()
    result.measured = [
        _iteration(operation, result.statements, seconds) for _ in range(iterations)
    ]
    if profiler and profile:
        profiler.disable()
        profiler.dump_stats(profile)
        stats = pstats.Stats(profiler, stream=sys.stderr)
        stats.sort_stats("tottime").print_stats(25)
    result.score = statistics.mean(result.measured)
    return result


def environment() -> dict[str, Any]:
    """Describe what the benchmarks run on, to save with the results."""
    decode_file = pyjelly.parse.decode.__file__ or ""
    compiled = decode_file.endswith(tuple(importlib.machinery.EXTENSION_SUFFIXES))
    commit = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],  # noqa: S607
        capture_output=True,
        text=True,
        cwd=REPO_DIR,
        check=False,
    ).stdout.strip()
    affinity = os.sched_getaffinity(0) if hasattr(os, "sched_getaffinity") else None
    return {
        "python": sys.version,
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_affinity": sorted(affinity) if affinity else None,
        "pyjelly_commit": commit,
        "pyjelly_compiled_with_mypyc": compiled,
        "pyjelly_max_jelly_version": MAX_VERSION,
        "rdflib": importlib.metadata.version("rdflib"),
        "protobuf": importlib.metadata.version("protobuf"),
    }


def run_sweeps(  # noqa: PLR0913
    task: str,
    datasets: Sequence[str],
    methods: Sequence[str],
    *,
    sweeps: int,
    rows: int,
    warmup: int,
    iterations: int,
    seconds: float,
) -> list[CaseResult]:
    """Run every case `sweeps` times, each time in a new process."""
    results = []
    total = sweeps * len(datasets) * len(methods)
    for sweep in range(sweeps):
        for dataset in datasets:
            for method in methods:
                command = [
                    sys.executable,
                    "-m",
                    "benchmarks",
                    "case",
                    task,
                    dataset,
                    method,
                    f"--rows={rows}",
                    f"--warmup={warmup}",
                    f"--iterations={iterations}",
                    f"--time={seconds}",
                ]
                # Warnings from the case go straight to the terminal, the
                # result comes as JSON on stdout
                process = subprocess.run(  # noqa: S603
                    command,
                    stdout=subprocess.PIPE,
                    text=True,
                    cwd=REPO_DIR,
                    check=False,
                )
                if process.returncode == 0:
                    # The JSON is the last line, in case a library printed something
                    last_line = process.stdout.strip().splitlines()[-1]
                    result = CaseResult(**json.loads(last_line))
                else:
                    result = CaseResult(task, dataset, method)
                    result.skipped = (
                        f"the process failed with code {process.returncode}"
                    )
                result.sweep = sweep
                results.append(result)
                score = f"{result.score:,.0f} st/s" if result.score else result.skipped
                progress = f"[{len(results)}/{total}] sweep {sweep + 1}"
                print(f"{progress}: {task} {dataset} {method}: {score}")
    return results


def save(path: Path, settings: dict[str, Any], results: Sequence[CaseResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = {
        "environment": environment(),
        "settings": settings,
        "results": [asdict(result) for result in results],
    }
    path.write_text(json.dumps(content, indent=2))
