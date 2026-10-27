"""Benchmark suites: Jelly-RDF and Jelly-SPARQL, each compared with other formats."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from benchmarks import data
from benchmarks.methods import METHODS, round_trip


@dataclass(frozen=True)
class Suite:
    name: str
    #: Unit of the throughput: statements or cells
    unit: str
    datasets: tuple[str, ...]
    default_datasets: tuple[str, ...]
    methods: Mapping[str, Any]
    load: Callable[[str, int], Any]
    round_trip: Callable[[Any, Any], tuple[bytes, list[str]]]


SUITES = {
    "rdf": Suite(
        "rdf",
        "statements",
        data.DATASETS,
        data.DATASETS,
        METHODS,
        data.load,
        round_trip,
    ),
}

try:
    from benchmarks import sparql_data
    from benchmarks.sparql_methods import SPARQL_METHODS, sparql_round_trip
except ImportError:
    # pyjelly 0.8 and older have no Jelly-SPARQL, but the rdf suite still runs on
    # them, to compare them with the current version
    pass
else:
    SUITES["sparql"] = Suite(
        "sparql",
        "cells",
        sparql_data.DATASETS,
        sparql_data.DEFAULT_DATASETS,
        SPARQL_METHODS,
        sparql_data.load,
        sparql_round_trip,
    )
