from __future__ import annotations

import io
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Callable

import pytest
from rdflib import Dataset, Graph

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.parse import (
    parse_jelly_flat as generic_parse_jelly_flat,
)
from pyjelly.integrations.generic.parse import (
    parse_jelly_grouped as generic_parse_jelly_grouped,
)
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.serialize.streams import QuadStream, SerializerOptions


def _make_flat_triples_bytes() -> bytes:
    g = Graph()
    g.parse("tests/e2e_test_cases/triples_rdf_1_1/nt-syntax-subm-01.nt")
    return g.serialize(encoding="jelly", format="jelly")


def _make_flat_quads_bytes() -> bytes:
    ds = Dataset()
    ds.parse("tests/e2e_test_cases/quads_rdf_1_1/weather-quads.nq")
    return ds.serialize(
        encoding="jelly",
        format="jelly",
        stream=QuadStream.for_rdflib(),
    )


def _make_grouped_graphs_bytes() -> bytes:
    ds = Dataset()
    g1 = Graph(identifier="g1")
    g1.parse("tests/e2e_test_cases/triples_rdf_1_1/nt-syntax-subm-01.nt")
    g2 = Graph(identifier="g2")
    g2.parse("tests/e2e_test_cases/triples_rdf_1_1/p2_ontology.nt")
    ds.add_graph(g1)
    ds.add_graph(g2)
    return ds.serialize(
        options=SerializerOptions(logical_type=jelly.LOGICAL_STREAM_TYPE_GRAPHS),
        encoding="jelly",
        format="jelly",
    )


@dataclass(frozen=True)
class Case:
    make_bytes: Callable[[], bytes]
    expected_physical: int
    expected_logical: int
    parser: Callable[..., Any]
    strict: bool
    should_raise: bool
    msg: str | None


@pytest.mark.parametrize(
    "case",
    [
        Case(
            make_bytes=_make_flat_triples_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            parser=generic_parse_jelly_grouped,
            strict=True,
            should_raise=True,
            msg="expected GROUPED",
        ),
        Case(
            make_bytes=_make_flat_triples_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            parser=generic_parse_jelly_grouped,
            strict=False,
            should_raise=False,
            msg=None,
        ),
        Case(
            make_bytes=_make_flat_triples_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            parser=generic_parse_jelly_flat,
            strict=True,
            should_raise=False,
            msg=None,
        ),
        Case(
            make_bytes=_make_flat_quads_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_QUADS,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            parser=generic_parse_jelly_flat,
            strict=True,
            should_raise=False,
            msg=None,
        ),
        Case(
            make_bytes=_make_grouped_graphs_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_GRAPHS,
            parser=generic_parse_jelly_flat,
            strict=True,
            should_raise=True,
            msg="expected FLAT",
        ),
        Case(
            make_bytes=_make_grouped_graphs_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_GRAPHS,
            parser=generic_parse_jelly_grouped,
            strict=True,
            should_raise=False,
            msg=None,
        ),
    ],
)
def test_generic_logical_matrix(case: Case) -> None:
    data = case.make_bytes()
    opts, _ = get_options_and_frames(io.BytesIO(data))
    assert opts.stream_types.physical_type == case.expected_physical
    assert opts.stream_types.logical_type == case.expected_logical

    ctx = (
        pytest.raises(JellyConformanceError, match=case.msg)
        if case.should_raise
        else nullcontext()
    )
    with ctx:
        list(case.parser(io.BytesIO(data), logical_type_strict=case.strict))


def test_generic_flat_strict_requires_stream_types() -> None:
    dummy = b"x"
    options = type("Opt", (), {"stream_types": None})()
    frames: list[object] = []
    from unittest.mock import patch

    with (
        patch(
            "pyjelly.integrations.generic.parse.get_options_and_frames",
            return_value=(options, frames),
        ),
        pytest.raises(JellyConformanceError, match="requires options.stream_types"),
    ):
        list(generic_parse_jelly_flat(io.BytesIO(dummy), logical_type_strict=True))


def test_generic_grouped_strict_unspecified_raises() -> None:
    class ST:
        flat = False
        logical_type = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
        physical_type = jelly.PHYSICAL_STREAM_TYPE_TRIPLES

    dummy = b"x"
    options = type("Opt", (), {"stream_types": ST()})()
    frames: list[object] = []
    from unittest.mock import patch

    with (
        patch(
            "pyjelly.integrations.generic.parse.get_options_and_frames",
            return_value=(options, frames),
        ),
        pytest.raises(JellyConformanceError, match="expected GROUPED"),
    ):
        list(generic_parse_jelly_grouped(io.BytesIO(dummy), logical_type_strict=True))


def test_generic_flat_unsupported_physical_raises() -> None:
    class ST:
        flat = True
        logical_type = jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
        physical_type = jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED

    dummy = b"x"
    options = type("Opt", (), {"stream_types": ST()})()
    frames: list[object] = []
    from unittest.mock import patch

    with (
        patch(
            "pyjelly.integrations.generic.parse.get_options_and_frames",
            return_value=(options, frames),
        ),
        pytest.raises(NotImplementedError, match="not supported"),
    ):
        list(generic_parse_jelly_flat(io.BytesIO(dummy), logical_type_strict=False))
