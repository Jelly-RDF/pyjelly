"""Jelly-SPARQL with the generic API."""

from __future__ import annotations

import io
from collections.abc import Iterator

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError, JellyIncompleteResultsError
from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    Literal,
    Node,
    Triple,
)
from pyjelly.integrations.generic.sparql import (
    parse_jelly_sparql,
    parse_jelly_sparql_results,
    serialize_jelly_sparql,
    serialize_jelly_sparql_ask,
    sparql_stream,
)
from pyjelly.options import LookupPreset, SparqlOptions
from pyjelly.parse.sparql import sparql_frames
from pyjelly.serialize.sparql import FrameWriter, write_solutions

EX = "http://example.org/"
XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"

A = IRI(EX + "a")
P = IRI(EX + "p")

ROWS: list[tuple[Node | None, ...]] = [
    (A, Literal("1", datatype=XSD_INTEGER), None),
    (A, Literal("hello", "en"), Literal("مرحبا", "ar", direction="rtl")),
    (BlankNode("b"), None, Triple(A, P, Literal("x"))),
    (None, None, None),
    (
        BlankNode("b"),
        Literal("hello", "en"),
        Triple(BlankNode("c"), P, Triple(A, P, A)),
    ),
]


def _write(
    rows: list[tuple[Node | None, ...]], options: SparqlOptions | None = None
) -> io.BytesIO:
    out = io.BytesIO()
    serialize_jelly_sparql(["s", "o", "t"], rows, out, options)
    out.seek(0)
    return out


def test_round_trip() -> None:
    results = parse_jelly_sparql(_write(ROWS))
    assert results.variables == ["s", "o", "t"]
    assert not results.is_boolean
    assert list(results) == ROWS
    assert not results.truncated
    assert not results.error


@pytest.mark.parametrize("frame_size", [1, 3, 7, 1000])
def test_frames(frame_size: int) -> None:
    out = _write(ROWS * 20, SparqlOptions(frame_size=frame_size))
    frames = list(sparql_frames(out))
    rows_per_frame = max(1, frame_size // 3)
    assert all(frame.row_count <= rows_per_frame for frame in frames)
    out.seek(0)
    assert list(parse_jelly_sparql(out)) == ROWS * 20


def test_rdf_version() -> None:
    options = SparqlOptions(rdf_version=jelly.RDF_VERSION_1_2_BASIC)
    with pytest.raises(JellyConformanceError, match=r"RDF 1\.2 Basic"):
        _write(ROWS, options)
    options = SparqlOptions(rdf_version=jelly.RDF_VERSION_1_1)
    with pytest.raises(JellyConformanceError, match=r"RDF 1\.1"):
        _write(ROWS[1:2], options)


def test_triple_terms_in_several_columns() -> None:
    # Each column has an IRI sequence of its own in its triple terms
    t1 = Triple(IRI(EX + "s1"), P, IRI(EX + "o1"))
    t2 = Triple(IRI(EX + "s2"), IRI(EX + "q"), Triple(A, P, IRI(EX + "o2")))
    rows: list[tuple[Node | None, ...]] = [
        (t1, t2, IRI(EX + "z")),
        (t2, t1, t1),
        (t1, None, t2),
    ] * 50
    options = SparqlOptions(
        lookup_preset=LookupPreset(max_names=128, max_prefixes=4, max_datatypes=4),
        frame_size=9,
    )
    assert list(parse_jelly_sparql(_write(rows, options))) == rows


def test_lookups_full() -> None:
    rows: list[tuple[Node | None, ...]] = [
        (IRI(f"{EX}{i}"), Literal(str(i), datatype=f"{EX}type{i % 40}"), None)
        for i in range(2000)
    ]
    options = SparqlOptions(
        lookup_preset=LookupPreset(max_names=128, max_prefixes=2, max_datatypes=16)
    )
    out = _write(rows, options)
    frames = list(sparql_frames(out))
    assert len(frames) > 2000 // 128
    out.seek(0)
    assert list(parse_jelly_sparql(out)) == rows


def test_ask() -> None:
    out = io.BytesIO()
    serialize_jelly_sparql_ask(True, out, links=[EX + "doc"])  # noqa: FBT003
    out.seek(0)
    results = parse_jelly_sparql(out)
    assert results.is_boolean
    assert results.boolean is True
    assert results.variables == []
    assert results.links == [EX + "doc"]
    assert list(results) == []


def test_non_delimited() -> None:
    out = _write(ROWS * 10, SparqlOptions(delimited=False))
    (frame,) = sparql_frames(out)
    assert frame.row_count == len(ROWS) * 10
    out.seek(0)
    assert list(parse_jelly_sparql(out)) == ROWS * 10


def test_non_delimited_too_large() -> None:
    rows: list[tuple[Node | None, ...]] = [
        (IRI(f"{EX}{i}"), None, None) for i in range(200)
    ]
    options = SparqlOptions(
        lookup_preset=LookupPreset(max_names=128, max_prefixes=0, max_datatypes=0),
        delimited=False,
    )
    with pytest.raises(JellyConformanceError, match="one frame"):
        _write(rows, options)


def _failing_rows() -> Iterator[tuple[Node | None, ...]]:
    yield from ROWS[:2]
    msg = "timeout"
    raise TimeoutError(msg)


def test_incomplete() -> None:
    out = io.BytesIO()
    with pytest.raises(TimeoutError):
        serialize_jelly_sparql(["s", "o", "t"], _failing_rows(), out)
    out.seek(0)
    results = parse_jelly_sparql(out)
    rows: list[tuple[Node | None, ...]] = []
    with pytest.raises(JellyIncompleteResultsError, match="timeout"):
        rows.extend(results)
    # The rows before the error are read
    assert rows == ROWS[:2]
    assert results.error == "timeout"


def test_sequence_of_result_sets() -> None:
    options = SparqlOptions(punctuated=True)
    stream = sparql_stream(options)
    out = io.BytesIO()
    writer = FrameWriter(out, delimited=True)
    write_solutions(stream, ["s", "o", "t"], ROWS, writer)
    writer.write(stream.ask(False))  # noqa: FBT003
    write_solutions(stream, ["x"], [(BlankNode("b"),)], writer, links=[EX + "l"])
    out.seek(0)
    first, second, third = parse_jelly_sparql_results(out)
    assert list(first) == ROWS
    assert second.boolean is False
    assert third.variables == ["x"]
    assert third.links == [EX + "l"]
    (row,) = list(third)
    # Blank node labels are scoped to a result set
    assert row[0] != ROWS[2][0]


def test_rows_are_kept_for_later() -> None:
    stream = sparql_stream(SparqlOptions(punctuated=True, frame_size=3))
    out = io.BytesIO()
    writer = FrameWriter(out, delimited=True)
    write_solutions(stream, ["s", "o", "t"], ROWS, writer)
    write_solutions(stream, ["s", "o", "t"], ROWS[:1], writer)
    out.seek(0)
    first, second = parse_jelly_sparql_results(out)
    assert list(second) == ROWS[:1]
    assert list(first) == ROWS


def test_flat_reader_rejects_punctuated() -> None:
    out = io.BytesIO()
    serialize_jelly_sparql_ask(
        True,  # noqa: FBT003
        out,
        SparqlOptions(punctuated=True),
    )
    out.seek(0)
    with pytest.raises(JellyConformanceError, match="parse_jelly_sparql_results"):
        parse_jelly_sparql(out)
