"""The "from Jelly" conformance tests of Jelly-SPARQL."""

from __future__ import annotations

from typing import IO, Any

import pytest
from rdflib.query import Result

from pyjelly.errors import JellyIncompleteResultsError
from pyjelly.integrations.generic.sparql import (
    parse_jelly_sparql as generic_parse_jelly_sparql,
)
from pyjelly.integrations.generic.sparql import (
    parse_jelly_sparql_sequence as generic_parse_jelly_sparql_sequence,
)
from pyjelly.integrations.rdflib.sparql import parse_jelly_sparql_sequence
from tests.conformance_tests.test_rdf._common import ConformanceCase, load_cases
from tests.meta import SPARQL_FROM_JELLY_MANIFEST
from tests.utils.sparql_compare import (
    Results,
    assert_same_result_sets,
    generic_results,
    rdflib_results,
    read_srj,
)

CASES = load_cases(SPARQL_FROM_JELLY_MANIFEST)


def _params(*, positive: bool, rdflib: bool = False) -> list[Any]:
    return [
        pytest.param(case, id=case.id)
        for case in CASES
        if case.positive == positive and (case.rdflib or not rdflib)
    ]


def _expected(case: ConformanceCase) -> list[Results]:
    return [read_srj(path) for path in case.results]


@pytest.mark.parametrize("case", _params(positive=True, rdflib=True))
def test_rdflib_parses_positive(case: ConformanceCase) -> None:
    with case.actions[0].open("rb") as file:
        if case.punctuated:
            results = [rdflib_results(r) for r in parse_jelly_sparql_sequence(file)]
        else:
            results = [rdflib_results(Result.parse(file, format="jelly"))]
    assert_same_result_sets(_expected(case), results, hint=case.id)


@pytest.mark.parametrize("case", _params(positive=True))
def test_generic_parses_positive(case: ConformanceCase) -> None:
    with case.actions[0].open("rb") as file:
        if case.punctuated:
            results = [
                generic_results(r) for r in generic_parse_jelly_sparql_sequence(file)
            ]
        else:
            results = [generic_results(generic_parse_jelly_sparql(file))]
    assert_same_result_sets(_expected(case), results, hint=case.id)


def _read_all_rdflib(file: IO[bytes], *, punctuated: bool) -> None:
    if not punctuated:
        Result.parse(file, format="jelly")
        return
    # Results that are incomplete don't end a sequence, they say so
    for result in parse_jelly_sparql_sequence(file):
        if result.error:
            raise JellyIncompleteResultsError(result.error, result.error)


@pytest.mark.parametrize("case", _params(positive=False, rdflib=True))
def test_rdflib_parsing_fails_negative(case: ConformanceCase) -> None:
    with (
        pytest.raises(Exception),  # noqa: B017,PT011 any error will do
        case.actions[0].open("rb") as file,
    ):
        _read_all_rdflib(file, punctuated=case.punctuated)


def _read_all(file: IO[bytes]) -> None:
    for result_set in generic_parse_jelly_sparql_sequence(file):
        list(result_set)


@pytest.mark.parametrize("case", _params(positive=False))
def test_generic_parsing_fails_negative(case: ConformanceCase) -> None:
    with (
        pytest.raises(Exception),  # noqa: B017,PT011 any error will do
        case.actions[0].open("rb") as file,
    ):
        _read_all(file)
