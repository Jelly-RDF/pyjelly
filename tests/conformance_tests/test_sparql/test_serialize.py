"""The "to Jelly" conformance tests of Jelly-SPARQL."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from rdflib.query import Result

from pyjelly import jelly
from pyjelly.integrations.generic.sparql import (
    parse_jelly_sparql_results,
    sparql_stream,
)
from pyjelly.integrations.rdflib.sparql import (
    serialize_jelly_sparql,
    serialize_jelly_sparql_results,
)
from pyjelly.options import LookupPreset, SparqlOptions
from pyjelly.parse.sparql import sparql_frames
from pyjelly.serialize.sparql import FrameWriter, write_solutions
from tests.conformance_tests.test_rdf._common import ConformanceCase, load_cases
from tests.meta import SPARQL_TO_JELLY_MANIFEST, TEST_OUTPUTS_DIR
from tests.utils.sparql_compare import (
    assert_same_result_sets,
    generic_results,
    generic_rows,
    read_srj,
)

CASES = load_cases(SPARQL_TO_JELLY_MANIFEST)


def _params(*, positive: bool, rdflib: bool = False) -> list[Any]:
    return [
        pytest.param(case, id=case.id)
        for case in CASES
        if case.positive == positive and (case.rdflib or not rdflib)
    ]


def _output(case: ConformanceCase) -> Path:
    return TEST_OUTPUTS_DIR / f"{case.id}.jellys"


def _stream_options(case: ConformanceCase) -> jelly.SparqlResultsOptions:
    assert case.options
    with case.options.open("rb") as file:
        return next(sparql_frames(file)).options


def _options(case: ConformanceCase) -> SparqlOptions:
    """Return the options of a test, as written in its stream options file."""
    options = _stream_options(case)
    return SparqlOptions(
        lookup_preset=LookupPreset(
            max_names=options.max_name_table_size,
            max_prefixes=options.max_prefix_table_size,
            max_datatypes=options.max_datatype_table_size,
        ),
        rdf_version=options.rdf_version,
        punctuated=options.stream_type == jelly.SPARQL_STREAM_TYPE_PUNCTUATED,
        stream_name=options.stream_name,
    )


def _write_generic(case: ConformanceCase) -> None:
    stream = sparql_stream(_options(case))
    with _output(case).open("wb") as file:
        writer = FrameWriter(file, delimited=True)
        for path in case.inputs:
            results = read_srj(path)
            if results.boolean is not None:
                writer.write(stream.ask(results.boolean))
            else:
                write_solutions(
                    stream, results.variables, generic_rows(results), writer
                )


def _write_rdflib(case: ConformanceCase) -> None:
    results = []
    for path in case.inputs:
        with path.open("rb") as file:
            results.append(Result.parse(file, format="json"))
    options = _options(case)
    with _output(case).open("wb") as file:
        if options.punctuated:
            serialize_jelly_sparql_results(results, file, options)
        else:
            (result,) = results
            serialize_jelly_sparql(result, file, options)


def _check(case: ConformanceCase) -> None:
    output = _output(case)
    with output.open("rb") as file:
        frames = list(sparql_frames(file))
    assert frames[0].options == _stream_options(case)
    assert frames[-1].HasField("trailer")
    assert not frames[-1].trailer.error
    with output.open("rb") as file:
        actual = [generic_results(r) for r in parse_jelly_sparql_results(file)]
    expected = [read_srj(path) for path in case.inputs]
    assert_same_result_sets(expected, actual, hint=case.id)


@pytest.mark.parametrize("case", _params(positive=True, rdflib=True))
def test_serializes_rdflib_positive(case: ConformanceCase) -> None:
    _write_rdflib(case)
    _check(case)


@pytest.mark.parametrize("case", _params(positive=True))
def test_serializes_generic_positive(case: ConformanceCase) -> None:
    _write_generic(case)
    _check(case)


@pytest.mark.parametrize("case", _params(positive=False, rdflib=True))
def test_serializing_fails_rdflib_negative(case: ConformanceCase) -> None:
    with pytest.raises(Exception):  # noqa: B017,PT011 any error will do
        _write_rdflib(case)


@pytest.mark.parametrize("case", _params(positive=False))
def test_serializing_fails_generic_negative(case: ConformanceCase) -> None:
    with pytest.raises(Exception):  # noqa: B017,PT011 any error will do
        _write_generic(case)
