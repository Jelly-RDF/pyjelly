"""Writing Jelly-RDF 1.2: the column batches of frames, and what can't be written."""

from __future__ import annotations

import io
from collections.abc import Iterable

import pytest
from rdflib import Graph, URIRef

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    DefaultGraph,
    GenericStatementSink,
    GraphName,
    Literal,
    Node,
    Quad,
    Triple,
)
from pyjelly.integrations.generic.parse import parse_jelly_flat, parse_jelly_grouped
from pyjelly.integrations.generic.serialize import (
    GenericSinkTermEncoder,
    flat_stream_to_frames,
    grouped_stream_to_frames,
)
from pyjelly.options import (
    LANG_STRING_DATATYPE_IRI,
    VERSION_1_1,
    LookupPreset,
    StreamParameters,
)
from pyjelly.serialize.encode import MAX_TRIPLE_TERM_DEPTH
from pyjelly.serialize.flows import FlatTriplesFrameFlow, GraphsFrameFlow
from pyjelly.serialize.ioutils import write_delimited
from pyjelly.serialize.streams import (
    QuadStream,
    SerializerOptions,
    TripleStream,
)

INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
S = IRI("https://a.org/s")
P = IRI("https://a.org/p")


def a(name: str) -> IRI:
    return IRI(f"https://a.org/{name}")


def frames(
    statements: Iterable[Triple | Quad],
    *,
    preset: LookupPreset | None = None,
    frame_size: int = 1000,
    rdf_version: jelly.RdfVersion = jelly.RDF_VERSION_UNSPECIFIED,
) -> list[jelly.RdfStreamFrame]:
    statements = list(statements)
    quads = isinstance(statements[0], Quad)
    flow_class = QuadStream if quads else TripleStream
    options = SerializerOptions(
        flow=flow_class.default_delimited_flow_class(frame_size=frame_size),
        lookup_preset=preset or LookupPreset(),
        params=StreamParameters(rdf_version=rdf_version),
    )
    return list(flat_stream_to_frames((s for s in statements), options))


def delimited(frames: Iterable[jelly.RdfStreamFrame]) -> io.BytesIO:
    out = io.BytesIO()
    for frame in frames:
        write_delimited(frame, out)
    out.seek(0)
    return out


def read(frames: Iterable[jelly.RdfStreamFrame]) -> list[object]:
    return list(parse_jelly_flat(delimited(frames)))


def test_column_of_the_specification_example() -> None:
    # The example of "Term types" in the specification: an IRI, a blank node, the
    # same literal twice, and another IRI
    objects: list[Node] = [
        a("x"),
        BlankNode("b1"),
        Literal("hello"),
        Literal("hello"),
        a("y"),
    ]
    statements = [Triple(S, P, o) for o in objects]
    (frame,) = frames(statements)
    column = frame.columns.objects
    assert column.kinds == b"\x18"
    assert list(column.layouts) == [64]
    # s and p take names 1 and 2
    assert list(column.name_ids) == [3, 0]
    assert list(column.prefix_ids) == [1]
    assert list(column.lex_values) == ["hello"]
    assert list(column.bnodes) == ["b1"]
    assert not column.literal_kinds
    assert read([frame]) == statements


def test_repeated_terms_are_runs() -> None:
    subjects = [a("s1")] * 3 + [a("s2")] + [a("s3")] * 2 + [a("s4")] * 40
    statements = [Triple(s, P, Literal(str(i))) for i, s in enumerate(subjects)]
    (frame,) = frames(statements)
    # A run of 3 cells (length code 1), a value of one cell, a run of 2 cells
    # (skip 1, length code 0), and a run of 40 cells: length code 15, and 38 - 15
    # in the next value
    assert list(frame.columns.subjects.layouts) == [1, 32, 15, 23]
    assert list(frame.columns.predicates.layouts) == [15, len(statements) - 2 - 15]
    assert read([frame]) == statements


def test_literal_kinds() -> None:
    # The example of "Literals in columns" in the specification
    objects = [
        Literal("cat", "en"),
        Literal("42", datatype=INTEGER),
        Literal("chat", "fr"),
        Literal("dog", "en"),
    ]
    (frame,) = frames(Triple(S, P, o) for o in objects)
    column = frame.columns.objects
    assert list(column.literal_kinds) == [2, 1, 4, 2]
    assert list(column.langtags) == ["en", "fr"]
    assert not column.langtag_directions
    (frame,) = frames(Triple(S, P, Literal(str(i), "en")) for i in range(3))
    assert list(frame.columns.objects.literal_kinds) == [2]
    (frame,) = frames(Triple(S, P, Literal(str(i))) for i in range(3))
    assert not frame.columns.objects.literal_kinds


def test_base_directions() -> None:
    objects = [
        Literal("a", "en", direction="ltr"),
        Literal("b", "en"),
        Literal("c", "en", direction="rtl"),
        Literal("d", "en", direction="ltr"),
    ]
    statements = [Triple(S, P, o) for o in objects]
    (frame,) = frames(statements)
    column = frame.columns.objects
    assert list(column.langtags) == ["en", "en", "en"]
    assert list(column.langtag_directions) == [
        jelly.RDF_BASE_DIRECTION_LTR,
        jelly.RDF_BASE_DIRECTION_UNSPECIFIED,
        jelly.RDF_BASE_DIRECTION_RTL,
    ]
    assert list(column.literal_kinds) == [2, 4, 6, 2]
    assert read([frame]) == statements


def test_prefix_ids() -> None:
    # The example of "IRIs in columns" in the specification
    objects = [a("x1"), a("x2"), a("x1"), IRI("https://b.org/z")]
    (frame,) = frames(Triple(S, P, o) for o in objects)
    assert list(frame.columns.objects.prefix_ids) == [1, 0, 0, 2]


def test_graphs() -> None:
    g = IRI("https://a.org/g")
    graphs: list[GraphName] = [DefaultGraph, g, DefaultGraph, DefaultGraph]
    statements = [Quad(S, P, Literal(str(i)), graph) for i, graph in enumerate(graphs)]
    (frame,) = frames(statements)
    # One unbound cell, then g, and the unbound cells at the end are left out
    assert list(frame.columns.graphs.layouts) == [16]
    assert read([frame]) == statements
    # A batch with all statements in the default graph has no graphs column
    (frame,) = frames(Quad(S, P, Literal(str(i)), DefaultGraph) for i in range(3))
    assert not frame.columns.HasField("graphs")


def test_frames_end_when_the_lookups_are_full() -> None:
    preset = LookupPreset(max_names=128, max_prefixes=4, max_datatypes=4)
    statements = [Triple(a(f"s{i}"), P, a(f"o{i}")) for i in range(300)]
    written = frames(statements, preset=preset)
    assert len(written) > 1
    for frame in written:
        names = sum(len(entry.values) for entry in frame.columns.names)
        assert names <= preset.max_names
    assert read(written) == statements


def test_triple_terms() -> None:
    nested = Triple(BlankNode("x"), P, Literal("1", datatype=INTEGER))
    statements = [
        Triple(S, P, Triple(a("a"), a("b"), a("c"))),
        Triple(S, P, Triple(a("a"), a("b"), nested)),
        Triple(S, P, Triple(a("a"), a("b"), BlankNode("y"))),
        Triple(S, P, Literal("v", "en", direction="ltr")),
    ]
    (frame,) = frames(statements)
    terms = frame.columns.objects.triple_terms
    # The IRIs of the triple terms are a sequence of their own: a, b and c are the
    # names after s and p, written as 0 for the one before + 1
    assert terms[0].s_iri.name_id == 3
    assert terms[0].p_iri.name_id == 0
    assert terms[0].o_iri.name_id == 0
    assert terms[1].o_triple_term.s_bnode == "x"
    assert read([frame]) == statements


@pytest.mark.parametrize(
    ("rdf_version", "term", "error"),
    [
        (jelly.RDF_VERSION_1_1, Literal("a", "en", direction="ltr"), "RDF 1.1"),
        (jelly.RDF_VERSION_1_1, Triple(S, P, S), "RDF 1.1"),
        (jelly.RDF_VERSION_1_2_BASIC, Triple(S, P, S), "RDF 1.2 Basic"),
    ],
)
def test_terms_that_the_rdf_version_has_not(
    rdf_version: jelly.RdfVersion, term: object, error: str
) -> None:
    with pytest.raises(JellyConformanceError, match=error):
        frames([Triple(S, P, term)], rdf_version=rdf_version)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("statement", "error"),
    [
        (Triple(Literal("a"), P, S), "subject of a statement cannot be a literal"),
        (Triple(S, BlankNode("b"), S), "predicate of a statement cannot be a blank"),
        (Triple(Triple(S, P, S), P, S), "subject of a statement cannot be a triple"),
        (Quad(S, P, S, Literal("g")), "graph of a statement cannot be a literal"),
        (Triple(S, P, Triple(Literal("a"), P, S)), "subject of a triple term"),
        (Triple(S, P, Triple(S, BlankNode("b"), S)), "predicate of a triple term"),
        (
            Triple(S, P, Literal("a", datatype=LANG_STRING_DATATYPE_IRI)),
            "must have a language tag",
        ),
        (Triple(S, P, Literal("a", "en", direction="up")), "unknown base direction"),
    ],
)
def test_what_jelly_rdf_1_2_cannot_write(statement: Triple | Quad, error: str) -> None:
    with pytest.raises(JellyConformanceError, match=error):
        frames([statement])


def test_triple_terms_nested_too_deep() -> None:
    term: object = S
    for _ in range(MAX_TRIPLE_TERM_DEPTH + 1):
        term = Triple(S, P, term)  # type: ignore[arg-type]
    with pytest.raises(JellyConformanceError, match="nested deeper"):
        frames([Triple(S, P, term)])  # type: ignore[arg-type]


def test_base_directions_need_jelly_rdf_1_2() -> None:
    options = SerializerOptions(params=StreamParameters(version=VERSION_1_1))
    statement = Triple(S, P, Literal("a", "en", direction="ltr"))
    with pytest.raises(
        JellyConformanceError, match=r"only be written in Jelly-RDF 1\.2"
    ):
        list(flat_stream_to_frames((s for s in [statement]), options))


def test_name_lookup_too_small() -> None:
    options = SerializerOptions(lookup_preset=LookupPreset(max_names=16))
    with pytest.raises(JellyConformanceError, match="at least 128"):
        TripleStream(encoder=GenericSinkTermEncoder(), options=options)


def test_namespace_declarations_start_a_new_frame() -> None:
    stream = TripleStream(encoder=GenericSinkTermEncoder())
    stream.enroll()
    assert stream.namespace_declaration("ex", "https://a.org/") is None
    stream.triple(Triple(S, P, S))
    # The declarations of a frame come before its statements
    frame = stream.namespace_declaration("ex2", "https://b.org/")
    assert frame is not None
    assert frame.columns.row_count == 1
    assert [ns.name for ns in frame.columns.namespaces] == ["ex"]
    last = stream.flow.to_stream_frame()
    assert last is not None
    assert [ns.name for ns in last.columns.namespaces] == ["ex2"]


def test_messages() -> None:
    graphs = []
    for size in (2, 0, 3):
        sink = GenericStatementSink()
        for i in range(size):
            sink.add(Triple(S, P, Literal(str(i))))
        graphs.append(sink)
    # Each graph is one RDF Message
    options = SerializerOptions(flow=GraphsFrameFlow())
    written = list(grouped_stream_to_frames((g for g in graphs), options))
    assert written[0].rows[0].options.stream_type == jelly.RDF_STREAM_TYPE_MESSAGES
    assert [list(f.columns.message_lengths) for f in written] == [[2], [0], [3]]
    # A flat flow puts many messages in one frame
    options = SerializerOptions(
        flow=FlatTriplesFrameFlow(),
        params=StreamParameters(stream_type=jelly.RDF_STREAM_TYPE_MESSAGES),
    )
    stream = TripleStream(encoder=GenericSinkTermEncoder(), options=options)
    stream.enroll()
    for sink in graphs:
        assert not list(stream.triples(sink))
        stream.flow.frame_from_graph()
    frame = stream.flow.to_stream_frame()
    assert frame is not None
    assert list(frame.columns.message_lengths) == [2, 0, 3]
    messages = list(parse_jelly_grouped(delimited([frame])))
    assert [len(message) for message in messages] == [2, 0, 3]


def test_rdflib_non_delimited_stream_needs_one_frame() -> None:
    graph = Graph()
    for i in range(300):
        graph.add((URIRef(f"https://a.org/s{i}"), URIRef(P._iri), URIRef(f"{i}")))
    options = SerializerOptions(
        lookup_preset=LookupPreset(max_names=128),
        params=StreamParameters(delimited=False),
    )
    with pytest.raises(JellyConformanceError, match="does not fit in one frame"):
        graph.serialize(destination=io.BytesIO(), format="jelly", options=options)
