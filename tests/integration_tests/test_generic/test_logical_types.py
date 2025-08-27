from __future__ import annotations

import io
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any, Callable

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.generic_sink import (
    IRI,
    DefaultGraph,
    GenericStatementSink,
    Literal,
    Quad,
    Triple,
)
from pyjelly.integrations.generic.parse import (
    parse_jelly_flat as generic_parse_jelly_flat,
)
from pyjelly.integrations.generic.parse import (
    parse_jelly_grouped as generic_parse_jelly_grouped,
)
from pyjelly.integrations.generic.serialize import (
    flat_stream_to_file,
    graphs_stream_frames,
    grouped_stream_to_file,
)
from pyjelly.options import StreamParameters
from pyjelly.parse.ioutils import get_options_and_frames
from pyjelly.serialize.streams import SerializerOptions


def _make_flat_triples_bytes() -> bytes:
    sink = GenericStatementSink()

    subject = IRI("http://example.org/subject")
    predicate = IRI("http://example.org/predicate")
    object1 = Literal("value1")
    object2 = IRI("http://example.org/object")

    sink.add(Triple(subject, predicate, object1))
    sink.add(Triple(subject, predicate, object2))

    output = io.BytesIO()
    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        params=StreamParameters(
            generalized_statements=True,
            rdf_star=True,
            namespace_declarations=False,
        ),
    )
    flat_stream_to_file(sink.store, output, options=options)
    return output.getvalue()


def _make_flat_quads_bytes() -> bytes:
    sink = GenericStatementSink()

    subject = IRI("http://example.org/subject")
    predicate = IRI("http://example.org/predicate")
    object1 = Literal("value1")
    graph = IRI("http://example.org/graph")

    sink.add(Quad(subject, predicate, object1, graph))
    sink.add(Quad(subject, predicate, object1, DefaultGraph))

    output = io.BytesIO()
    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(
            generalized_statements=True,
            rdf_star=True,
            namespace_declarations=False,
        ),
    )
    flat_stream_to_file(sink.store, output, options=options)
    return output.getvalue()


def _make_physical_graphs_bytes() -> bytes:
    from pyjelly.integrations.generic.serialize import GenericSinkTermEncoder
    from pyjelly.serialize.streams import GraphStream

    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        params=StreamParameters(
            generalized_statements=True,
            rdf_star=True,
            namespace_declarations=False,
        ),
    )

    stream = GraphStream(
        encoder=GenericSinkTermEncoder(),
        options=options,
    )

    sink = GenericStatementSink()

    sink.add(
        Quad(
            IRI("http://ex/s1"),
            IRI("http://ex/p1"),
            IRI("http://ex/o1"),
            IRI("http://ex/g1"),
        )
    )
    sink.add(
        Quad(
            IRI("http://ex/s2"),
            IRI("http://ex/p2"),
            IRI("http://ex/o2"),
            IRI("http://ex/g2"),
        )
    )

    output = io.BytesIO()

    frames = list(graphs_stream_frames(stream, sink))

    for frame in frames:
        output.write(frame.SerializeToString())

    return output.getvalue()


def _make_grouped_graphs_bytes() -> bytes:
    graph1 = GenericStatementSink(IRI("http://example.org/graph1"))
    graph2 = GenericStatementSink(IRI("http://example.org/graph2"))

    subject = IRI("http://example.org/subject")
    predicate = IRI("http://example.org/predicate")

    graph1.add(Triple(subject, predicate, Literal("graph1_value")))
    graph2.add(Triple(subject, predicate, Literal("graph2_value")))

    output = io.BytesIO()
    options = SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_GRAPHS,
        params=StreamParameters(
            generalized_statements=True,
            rdf_star=True,
            namespace_declarations=False,
        ),
    )
    grouped_stream_to_file((g for g in [graph1, graph2]), output, options=options)
    return output.getvalue()


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
        Case(
            make_bytes=_make_flat_quads_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_QUADS,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            parser=generic_parse_jelly_grouped,
            strict=True,
            should_raise=True,
            msg="expected GROUPED",
        ),
        Case(
            make_bytes=_make_flat_quads_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_QUADS,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            parser=generic_parse_jelly_grouped,
            strict=False,
            should_raise=False,
            msg=None,
        ),
        Case(
            make_bytes=_make_flat_quads_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_QUADS,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            parser=generic_parse_jelly_flat,
            strict=False,
            should_raise=False,
            msg=None,
        ),
        Case(
            make_bytes=_make_flat_triples_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            parser=generic_parse_jelly_flat,
            strict=False,
            should_raise=False,
            msg=None,
        ),
        Case(
            make_bytes=_make_grouped_graphs_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_GRAPHS,
            parser=generic_parse_jelly_grouped,
            strict=False,
            should_raise=False,
            msg=None,
        ),
        Case(
            make_bytes=_make_physical_graphs_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            parser=generic_parse_jelly_flat,
            strict=True,
            should_raise=False,
            msg=None,
        ),
        Case(
            make_bytes=_make_physical_graphs_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            parser=generic_parse_jelly_flat,
            strict=False,
            should_raise=False,
            msg=None,
        ),
        Case(
            make_bytes=_make_physical_graphs_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            parser=generic_parse_jelly_grouped,
            strict=True,
            should_raise=True,
            msg="expected GROUPED",
        ),
        Case(
            make_bytes=_make_physical_graphs_bytes,
            expected_physical=jelly.PHYSICAL_STREAM_TYPE_GRAPHS,
            expected_logical=jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
            parser=generic_parse_jelly_grouped,
            strict=False,
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
