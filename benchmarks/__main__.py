"""
Command line of the benchmarks. Run `python -m benchmarks --help`.

See README.md in this directory for how to use it.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from benchmarks import data, measure, report
from benchmarks.methods import METHODS

RESULTS_DIR = Path(__file__).parent / "results"


def _names(value: str) -> list[str]:
    return [name.strip() for name in value.split(",") if name.strip()]


def _add_selection(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--datasets",
        type=_names,
        default=list(data.DATASETS),
        help="comma-separated RiverBench datasets (default: all 13)",
    )
    parser.add_argument(
        "--methods",
        type=_names,
        default=list(METHODS),
        help=f"comma-separated methods (default: all of {', '.join(METHODS)})",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=data.DEFAULT_ROWS,
        help="how many statements of each dataset to use (default: %(default)s)",
    )


def _add_timing(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--warmup", type=int, default=1, help="warmup iterations (default: %(default)s)"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="measured iterations (default: %(default)s)",
    )
    parser.add_argument(
        "--time",
        type=float,
        default=1.0,
        help="seconds per iteration, at least one operation (default: %(default)s)",
    )
    parser.add_argument(
        "--no-check",
        dest="check",
        action="store_false",
        help="skip the round-trip check (only if the code under test hasn't changed)",
    )


def _cpus(value: str) -> list[int]:
    """Parse a CPU list like taskset's: "5-9" or "5,6,7" or "0,5-9"."""
    cpus: list[int] = []
    for part in value.split(","):
        first, _, last = part.partition("-")
        cpus.extend(range(int(first), int(last or first) + 1))
    return cpus


def _format_cpus(cpus: list[int]) -> str:
    """Format CPU cores like taskset does: "5-9,15"."""
    ranges: list[list[int]] = []
    for cpu in sorted(cpus):
        if ranges and cpu == ranges[-1][1] + 1:
            ranges[-1][1] = cpu
        else:
            ranges.append([cpu, cpu])
    return ",".join(str(a) if a == b else f"{a}-{b}" for a, b in ranges)


def _default_cpu_sets() -> list[list[int]]:
    """
    Return where to run the cases when --cpus is not given.

    Results are only comparable on one kind of core, and cores of the same kind
    can still differ by cluster (see measure.performance_cluster()), so the cases
    run on one cluster of the fastest cores. If this process is already pinned to
    some cores (e.g. with taskset), they are used instead.
    """
    affinity = (
        sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else []
    )
    if affinity and len(affinity) < (os.cpu_count() or 0):
        print(f"Running on cores {_format_cpus(affinity)}, which this is pinned to")
        return [affinity]
    cluster = measure.performance_cluster()
    if cluster:
        print(
            f"Running on cores {_format_cpus(cluster)}, one cluster of the fastest "
            "cores (choose others with --cpus)"
        )
        return [cluster]
    print("Running on any core: could not find out which cores are the fastest")
    return []


def _run(args: argparse.Namespace) -> None:
    """Run the `run` command."""
    if not args.cpus:
        args.cpus = _default_cpu_sets()
    settings = {
        name: getattr(args, name)
        for name in (
            "task",
            "datasets",
            "methods",
            "rows",
            "warmup",
            "iterations",
            "time",
            "sweeps",
            "check",
            "cpus",
        )
    }
    results = measure.run_sweeps(
        args.task,
        args.datasets,
        args.methods,
        sweeps=args.sweeps,
        rows=args.rows,
        warmup=args.warmup,
        iterations=args.iterations,
        seconds=args.time,
        check=args.check,
        cpu_sets=args.cpus,
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    out = args.out or RESULTS_DIR / f"{stamp}-{args.task}.json"
    measure.save(out, settings, results)
    print(f"\nSaved to {out}\n")
    print(report.throughput_table(args.task, results))


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m benchmarks", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    fetch = commands.add_parser("fetch", help="download the datasets")
    fetch.add_argument("--datasets", type=_names, default=list(data.DATASETS))

    run = commands.add_parser("run", help="measure throughput, in sweeps")
    run.add_argument("task", choices=["write", "read"])
    _add_selection(run)
    _add_timing(run)
    run.add_argument(
        "--sweeps", type=int, default=3, help="how many sweeps (default: %(default)s)"
    )
    run.add_argument("--out", type=Path, help="where to save the results as JSON")
    run.add_argument(
        "--cpus",
        type=_cpus,
        action="append",
        default=[],
        help=(
            "a set of CPU cores to pin cases to, e.g. 15-19 (default: one cluster "
            "of the fastest cores, or the cores this is already pinned to). Give it "
            "several times to run that many cases in parallel, one per set"
        ),
    )

    case = commands.add_parser(
        "case", help="run one case in this process and print its result as JSON"
    )
    case.add_argument("task", choices=["write", "read"])
    case.add_argument("dataset")
    case.add_argument("method", choices=list(METHODS))
    case.add_argument("--rows", type=int, default=data.DEFAULT_ROWS)
    case.add_argument("--cpus", type=_cpus, help="CPU cores to pin the case to")
    _add_timing(case)
    case.add_argument(
        "--profile",
        type=Path,
        help="profile the measured iterations with cProfile, and save the stats here",
    )

    size = commands.add_parser("size", help="print serialized sizes")
    _add_selection(size)

    show = commands.add_parser("report", help="print tables from saved results")
    show.add_argument("results", type=Path, nargs="+")

    args = parser.parse_args()
    if args.command == "fetch":
        data.fetch(args.datasets)
    elif args.command == "case":
        if args.cpus:
            os.sched_setaffinity(0, args.cpus)
        result = measure.run_case(
            args.task,
            args.dataset,
            args.method,
            rows=args.rows,
            warmup=args.warmup,
            iterations=args.iterations,
            seconds=args.time,
            check=args.check,
            profile=args.profile,
        )
        print(json.dumps(asdict(result)))
    elif args.command == "run":
        _run(args)
    elif args.command == "size":
        print(report.size_tables(args.datasets, args.methods, args.rows))
    elif args.command == "report":
        for path in args.results:
            content = json.loads(path.read_text())
            results = report.results_from_json(content)
            print(f"## {path.name}\n\n{json.dumps(content['environment'], indent=2)}\n")
            for task in ("write", "read"):
                if any(result.task == task for result in results):
                    print(report.throughput_table(task, results), end="\n\n")


if __name__ == "__main__":
    main()
