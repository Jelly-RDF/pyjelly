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
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import Any

import pyjelly.parse.decode
from benchmarks.methods import NullOutput
from benchmarks.suites import SUITES
from pyjelly.options import MAX_VERSION

# The folder that has the `benchmarks` package, where the cases run. With a
# symlink to the package, it isn't the repository, see README.md.
REPO_DIR = Path(__file__).parent.parent


@dataclass
class CaseResult:
    task: str
    dataset: str
    method: str
    #: Statements done in one operation: every statement the writer writes, or
    #: every statement the reader found in the bytes. In the sparql suite, cells
    #: (rows times variables).
    statements: int = 0
    #: Throughput of each iteration, in statements (or cells) per second
    warmup: list[float] = field(default_factory=list)
    measured: list[float] = field(default_factory=list)
    #: Mean throughput of the measured iterations, None if the case didn't run
    score: float | None = None
    #: Why the case didn't run, if it didn't
    skipped: str | None = None
    #: What the round-trip check found, if anything
    problems: list[str] = field(default_factory=list)
    #: Whether the round-trip check ran
    checked: bool = True
    #: The CPU cores the case ran on
    cpus: list[int] | None = None
    sweep: int = 0
    #: Suite of the case: "rdf" or "sparql", see suites.py
    suite: str = "rdf"


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
    check: bool = True,
    profile: Path | None = None,
    suite: str = "rdf",
) -> CaseResult:
    """
    Run one case in this process. See the module docstring.

    With `check=False`, the round-trip check is skipped. Do that only when the
    code under test hasn't changed since the check last passed.
    """
    result = CaseResult(task, dataset, method_name, checked=check, suite=suite)
    if hasattr(os, "sched_getaffinity"):
        result.cpus = sorted(os.sched_getaffinity(0))
    benchmarks = SUITES[suite]
    round_trip = benchmarks.round_trip
    method = benchmarks.methods[method_name]
    data = benchmarks.load(dataset, rows)
    if not method.supports(data):
        result.skipped = "the format can't hold this kind of statements"
        return result
    if task == "write" and not getattr(method, "can_write", True):
        result.skipped = "the library can't write the format"
        return result

    # Setup, not measured: make the terms (and the graph, for writers that need
    # one), check that the method round-trips the data, and make the bytes to read
    method.prepare(data)
    try:
        if check:
            written, result.problems = round_trip(method, data)
        else:
            written = method.write_bytes(data) if task == "read" else b""
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
        result.statements = method.write_count(data)

        def operation() -> None:
            method.write(data, output)

    else:
        # A writer that drops duplicates leaves fewer statements to read
        result.statements = method.read(written, quads=data.quads)

        def operation() -> int:
            return int(method.read(written, quads=data.quads))

    _measure(
        operation,
        result,
        warmup=warmup,
        iterations=iterations,
        seconds=seconds,
        profile=profile,
    )
    return result


def _measure(  # noqa: PLR0913
    operation: Callable[[], object],
    result: CaseResult,
    *,
    warmup: int,
    iterations: int,
    seconds: float,
    profile: Path | None,
) -> None:
    """Run the warmup and measured iterations, and fill in the result."""
    # Everything made so far lives until the end. Freezing it keeps the garbage
    # collector from going through it again and again during the measurement.
    # PyPy has no gc.freeze(), and doesn't need it: its collector doesn't go
    # through old objects again unless they change.
    gc.collect()
    if hasattr(gc, "freeze"):
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


def environment() -> dict[str, Any]:
    """Describe what the benchmarks run on, to save with the results."""
    decode_file = pyjelly.parse.decode.__file__ or ""
    compiled = decode_file.endswith(tuple(importlib.machinery.EXTENSION_SUFFIXES))
    commit = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],  # noqa: S607
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent,
        check=False,
    ).stdout.strip()
    affinity = os.sched_getaffinity(0) if hasattr(os, "sched_getaffinity") else None
    return {
        "python": sys.version,
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_affinity": sorted(affinity) if affinity else None,
        "pyjelly": importlib.metadata.version("pyjelly"),
        "pyjelly_commit": commit,
        "pyjelly_compiled_with_mypyc": compiled,
        "pyjelly_max_jelly_version": MAX_VERSION,
        "rdflib": importlib.metadata.version("rdflib"),
        "protobuf": importlib.metadata.version("protobuf"),
    }


def _run_case_process(  # noqa: PLR0913
    task: str,
    dataset: str,
    method: str,
    *,
    rows: int,
    warmup: int,
    iterations: int,
    seconds: float,
    check: bool,
    cpus: Sequence[int] | None,
    suite: str,
) -> CaseResult:
    """Run one case in a new process, pinned to `cpus` if given."""
    command = [
        sys.executable,
        "-m",
        "benchmarks",
        "case",
        task,
        dataset,
        method,
        f"--suite={suite}",
        f"--rows={rows}",
        f"--warmup={warmup}",
        f"--iterations={iterations}",
        f"--time={seconds}",
    ]
    if not check:
        command.append("--no-check")
    if cpus:
        command.append(f"--cpus={','.join(map(str, cpus))}")
    # Warnings from the case go straight to the terminal, the result comes as
    # JSON on stdout
    process = subprocess.run(  # noqa: S603
        command, stdout=subprocess.PIPE, text=True, cwd=REPO_DIR, check=False
    )
    if process.returncode == 0:
        # The JSON is the last line, in case a library printed something
        last_line = process.stdout.strip().splitlines()[-1]
        return CaseResult(**json.loads(last_line))
    result = CaseResult(task, dataset, method, checked=check, suite=suite)
    result.skipped = f"the process failed with code {process.returncode}"
    return result


def parse_cpu_list(text: str) -> list[int]:
    """Parse a Linux CPU list, as taskset and sysfs write it: "0-4,10"."""
    cpus: set[int] = set()
    for part in text.strip().split(","):
        if part:
            first, _, last = part.partition("-")
            cpus.update(range(int(first), int(last or first) + 1))
    return sorted(cpus)


def performance_cluster() -> list[int] | None:
    """
    Return the cores of one cluster of the fastest cores, None if unknown.

    On CPUs with performance and efficiency cores, the fastest cores are the ones
    with the highest maximum frequency. They can be split into clusters with
    different last-level caches, which differ in speed by a few percent (on the
    NVIDIA DGX Spark: cores 5-9 and 15-19, ~3%), so results are only comparable
    within one cluster. This is the last of them, read from Linux sysfs.
    """
    base = Path("/sys/devices/system/cpu")
    try:
        cpus = sorted(int(path.name[3:]) for path in base.glob("cpu[0-9]*"))
        frequencies = {
            cpu: int((base / f"cpu{cpu}/cpufreq/cpuinfo_max_freq").read_text())
            for cpu in cpus
        }
        top = max(frequencies.values())
        fastest = {cpu for cpu, frequency in frequencies.items() if frequency == top}
        clusters: list[list[int]] = []
        for cpu in sorted(fastest):
            caches = sorted(
                (base / f"cpu{cpu}/cache").glob("index[0-9]*"),
                key=lambda path: int(path.name[5:]),
            )
            shared = set(parse_cpu_list((caches[-1] / "shared_cpu_list").read_text()))
            cluster = sorted(fastest & shared)
            if cluster not in clusters:
                clusters.append(cluster)
    except (OSError, ValueError, IndexError):
        return None
    return clusters[-1] if clusters else None


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
    check: bool = True,
    cpu_sets: Sequence[Sequence[int]] = (),
    suite: str = "rdf",
) -> list[CaseResult]:
    """
    Run every case `sweeps` times, each time in a new process.

    The cases run pinned to a CPU set, if one is given. With several sets, the
    cases of a sweep run in parallel, one per set. Each case always runs on the
    same set (cases are dealt to the sets in turn), so that a speed difference
    between the sets can't make two runs of the same case differ. The next sweep
    starts when the previous one is done.
    """
    cases = [(dataset, method) for dataset in datasets for method in methods]
    total = sweeps * len(cases)
    results: list[CaseResult] = []
    lock = threading.Lock()
    sets: list[Sequence[int] | None] = list(cpu_sets) or [None]

    def run_share(sweep: int, worker: int) -> None:
        # Every len(sets)-th case, always on the same set
        for dataset, method in cases[worker :: len(sets)]:
            result = _run_case_process(
                task,
                dataset,
                method,
                rows=rows,
                warmup=warmup,
                iterations=iterations,
                seconds=seconds,
                check=check,
                cpus=sets[worker],
                suite=suite,
            )
            result.sweep = sweep
            unit = SUITES[suite].unit
            score = f"{result.score:,.0f} {unit}/s" if result.score else result.skipped
            with lock:
                results.append(result)
                progress = f"[{len(results)}/{total}] sweep {sweep + 1}"
                print(f"{progress}: {task} {dataset} {method}: {score}", flush=True)

    with ThreadPoolExecutor(max_workers=len(sets)) as pool:
        for sweep in range(sweeps):
            list(pool.map(partial(run_share, sweep), range(len(sets))))

    # In the order of the cases, not the order they finished in
    order = {case: i for i, case in enumerate(cases)}
    results.sort(key=lambda r: (r.sweep, order[r.dataset, r.method]))
    return results


def save(path: Path, settings: dict[str, Any], results: Sequence[CaseResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = {
        "environment": environment(),
        "settings": settings,
        "results": [asdict(result) for result in results],
    }
    path.write_text(json.dumps(content, indent=2))
