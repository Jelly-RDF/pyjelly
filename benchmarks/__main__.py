"""
Command line of the benchmarks. Run `python -m benchmarks --help`.

See README.md in this directory for how to use it.
"""

from __future__ import annotations

import argparse
import json
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
        "--warmup", type=int, default=5, help="warmup iterations (default: %(default)s)"
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

    case = commands.add_parser(
        "case", help="run one case in this process and print its result as JSON"
    )
    case.add_argument("task", choices=["write", "read"])
    case.add_argument("dataset")
    case.add_argument("method", choices=list(METHODS))
    case.add_argument("--rows", type=int, default=data.DEFAULT_ROWS)
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
        result = measure.run_case(
            args.task,
            args.dataset,
            args.method,
            rows=args.rows,
            warmup=args.warmup,
            iterations=args.iterations,
            seconds=args.time,
            profile=args.profile,
        )
        print(json.dumps(asdict(result)))
    elif args.command == "run":
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
        )
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        out = args.out or RESULTS_DIR / f"{stamp}-{args.task}.json"
        measure.save(out, settings, results)
        print(f"\nSaved to {out}\n")
        print(report.throughput_table(args.task, results))
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
