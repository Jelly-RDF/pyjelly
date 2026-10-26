"""The "from Jelly" conformance tests of Jelly-RDF 1.1 and 1.2."""

from __future__ import annotations

from typing import Any

import pytest
from rdflib import Dataset, Graph

from pyjelly.integrations.generic.generic_sink import Prefix as GenericPrefix
from pyjelly.integrations.generic.parse import (
    parse_jelly_flat as generic_parse_jelly_flat,
)
from pyjelly.integrations.generic.parse import (
    parse_jelly_grouped as generic_parse_jelly_grouped,
)
from pyjelly.integrations.rdflib.parse import parse_jelly_flat, parse_jelly_grouped
from pyjelly.integrations.rdflib.statements import Prefix
from tests.conformance_tests.test_rdf._common import ConformanceCase, load_cases
from tests.meta import RDF_FROM_JELLY_MANIFESTS
from tests.utils.ordered_memory import OrderedMemory
from tests.utils.rdf_compare import (
    Statement,
    assert_same_statements,
    generic_statements,
    rdflib_graph_statements,
    rdflib_statements,
    read_statements,
)

CASES = [case for path in RDF_FROM_JELLY_MANIFESTS for case in load_cases(path)]


def _params(*, positive: bool, rdflib: bool = False) -> list[Any]:
    return [
        pytest.param(case, id=case.id)
        for case in CASES
        if case.positive == positive and (case.rdflib or not rdflib)
    ]


def _grouped(case: ConformanceCase) -> bool:
    """Whether the expected statements are in groups, one per frame or message."""
    return case.suite == "v1_1" or case.messages


def _check(case: ConformanceCase, groups: list[list[Statement]]) -> None:
    expected = [read_statements(path) for path in case.results]
    assert len(groups) == len(expected), (
        f"expected {len(expected)} groups of statements, got {len(groups)}"
    )
    for index, (e, a) in enumerate(zip(expected, groups, strict=True)):
        assert_same_statements(e, a, hint=f"{case.results[index].name}")


@pytest.mark.parametrize("case", _params(positive=True, rdflib=True))
def test_rdflib_parses_positive(case: ConformanceCase) -> None:
    with case.actions[0].open("rb") as file:
        if _grouped(case):
            groups = [
                rdflib_graph_statements(graph)
                for graph in parse_jelly_grouped(
                    file,
                    graph_factory=lambda: Graph(store=OrderedMemory()),
                    dataset_factory=lambda: Dataset(store=OrderedMemory()),
                )
            ]
        else:
            statements = parse_jelly_flat(file)
            groups = [
                rdflib_statements(s for s in statements if not isinstance(s, Prefix))
            ]
    _check(case, groups)


@pytest.mark.parametrize("case", _params(positive=True))
def test_generic_parses_positive(case: ConformanceCase) -> None:
    with case.actions[0].open("rb") as file:
        if _grouped(case):
            groups = [
                generic_statements(sink) for sink in generic_parse_jelly_grouped(file)
            ]
        else:
            statements = generic_parse_jelly_flat(file)
            groups = [
                generic_statements(
                    s for s in statements if not isinstance(s, GenericPrefix)
                )
            ]
    _check(case, groups)


@pytest.mark.parametrize("case", _params(positive=False, rdflib=True))
def test_rdflib_parsing_fails_negative(case: ConformanceCase) -> None:
    dataset = Dataset(store=OrderedMemory())
    with pytest.raises(Exception):  # noqa: B017,PT011 any error will do
        dataset.parse(location=str(case.actions[0]), format="jelly")


@pytest.mark.parametrize("case", _params(positive=False))
def test_generic_parsing_fails_negative(case: ConformanceCase) -> None:
    with (
        pytest.raises(Exception),  # noqa: B017,PT011 any error will do
        case.actions[0].open("rb") as file,
    ):
        list(generic_parse_jelly_flat(file))
