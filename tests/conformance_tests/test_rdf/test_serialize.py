"""The "to Jelly" conformance tests of Jelly-RDF 1.1 and 1.2."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from pyjelly.integrations.generic.parse import parse_jelly_grouped
from tests.conformance_tests.test_rdf._common import ConformanceCase, load_cases
from tests.meta import RDF_TO_JELLY_MANIFESTS, TEST_OUTPUTS_DIR
from tests.serialize import write_generic_sink, write_graph_or_dataset
from tests.utils.rdf_compare import (
    assert_same_statements,
    generic_statements,
    read_statements,
)
from tests.utils.rdf_test_cases import jelly_validate, needs_jelly_cli

CASES = [case for path in RDF_TO_JELLY_MANIFESTS for case in load_cases(path)]

# rdflib can't parse the input: https://github.com/Jelly-RDF/pyjelly/issues/145
RDFLIB_CANT_PARSE = {"v1_1-triples_rdf_1_1-pos_014"}


def _params(*, positive: bool, rdflib: bool = False) -> list[Any]:
    params = []
    for case in CASES:
        if case.positive != positive or not (case.rdflib or not rdflib):
            continue
        marks = []
        if case.graphs:
            marks.append(
                pytest.mark.skip(reason="pyjelly doesn't write GRAPHS streams")
            )
        if rdflib and case.id in RDFLIB_CANT_PARSE:
            marks.append(pytest.mark.skip(reason="rdflib can't parse the input"))
        params.append(pytest.param(case, id=case.id, marks=marks))
    return params


def _output(case: ConformanceCase) -> Path:
    return TEST_OUTPUTS_DIR / f"{case.id}.jelly"


def _check(case: ConformanceCase, output: Path) -> None:
    hint = f"Test ID: {case.id}, output: {output}"
    if not case.messages:
        jelly_validate(
            output,
            "--compare-to-rdf-file",
            case.results[0],
            "--compare-to-format",
            "jelly",
            "--compare-ordered",
            "--options-file",
            case.options,
            hint=hint,
        )
    else:
        jelly_validate(output, "--options-file", case.options, hint=hint)
        with output.open("rb") as file:
            messages = [generic_statements(sink) for sink in parse_jelly_grouped(file)]
        assert len(messages) == len(case.inputs)
        for path, message in zip(case.inputs, messages, strict=True):
            assert_same_statements(read_statements(path), message, hint=path.name)


@needs_jelly_cli
@pytest.mark.parametrize("case", _params(positive=True, rdflib=True))
def test_serializes_rdflib_positive(case: ConformanceCase) -> None:
    output = _output(case)
    assert case.options
    write_graph_or_dataset(*case.inputs, options=case.options, out_filename=output)
    _check(case, output)


@needs_jelly_cli
@pytest.mark.parametrize("case", _params(positive=True))
def test_serializes_generic_positive(case: ConformanceCase) -> None:
    output = _output(case)
    assert case.options
    write_generic_sink(*case.inputs, options=case.options, out_filename=output)
    _check(case, output)


@pytest.mark.parametrize("case", _params(positive=False, rdflib=True))
def test_serializing_fails_rdflib_negative(case: ConformanceCase) -> None:
    assert case.options
    with pytest.raises(Exception):  # noqa: B017,PT011 any error will do
        write_graph_or_dataset(
            *case.inputs, options=case.options, out_filename=_output(case)
        )


@pytest.mark.parametrize("case", _params(positive=False))
def test_serializing_fails_generic_negative(case: ConformanceCase) -> None:
    assert case.options
    with pytest.raises(Exception):  # noqa: B017,PT011 any error will do
        write_generic_sink(
            *case.inputs, options=case.options, out_filename=_output(case)
        )
