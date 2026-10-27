"""
Reading invalid Jelly-RDF input, after jelly-jvm's ProtoDecoderHardeningSpec.

The rows are valid protobuf, but refer to things that are not there or are not
allowed in the stream. The reader must reject them with JellyConformanceError.
"""

from __future__ import annotations

import io
from collections.abc import Callable
from typing import IO, Any

import pytest

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError, JellyConformanceError
from pyjelly.integrations.generic.decode import GenericStatementSinkAdapter
from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    DefaultGraph,
    GenericStatementSink,
    Literal,
    Quad,
    Triple,
)
from pyjelly.integrations.generic.parse import parse_jelly_flat
from pyjelly.integrations.generic.serialize import guess_stream, stream_frames
from pyjelly.options import LookupPreset, StreamParameters
from pyjelly.parse.sparql import read_result_sets
from pyjelly.serialize.ioutils import write_delimited, write_single
from pyjelly.serialize.streams import SerializerOptions
from tests.utils import byte_fuzzer, wire_fuzzer

Row = jelly.RdfStreamRow
DATATYPES = 8


def options(datatypes: int = DATATYPES) -> jelly.RdfStreamRow:
    return Row(
        options=jelly.RdfStreamOptions(
            physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
            max_name_table_size=128,
            max_prefix_table_size=8,
            max_datatype_table_size=datatypes,
            version=2,
        )
    )


def iri(prefix: int, name: int) -> jelly.RdfIri:
    return jelly.RdfIri(prefix_id=prefix, name_id=name)


def name(index: int, value: str) -> jelly.RdfStreamRow:
    return Row(name=jelly.RdfNameEntry(id=index, value=value))


def prefix(index: int, value: str) -> jelly.RdfStreamRow:
    return Row(prefix=jelly.RdfPrefixEntry(id=index, value=value))


def datatype(index: int, value: str) -> jelly.RdfStreamRow:
    return Row(datatype=jelly.RdfDatatypeEntry(id=index, value=value))


def triple(**terms: Any) -> jelly.RdfStreamRow:
    return Row(triple=jelly.RdfTriple(**terms))


def namespace(label: str, value: jelly.RdfIri | None = None) -> jelly.RdfStreamRow:
    return Row(namespace=jelly.RdfNamespaceDeclaration(name=label, value=value))


def read(data: bytes) -> list[Any]:
    return list(parse_jelly_flat(io.BytesIO(data)))


def read_rows(*rows: jelly.RdfStreamRow) -> list[Any]:
    out = io.BytesIO()
    write_delimited(jelly.RdfStreamFrame(rows=rows), out)
    return read(out.getvalue())


def literal(lex: str, index: int) -> jelly.RdfStreamRow:
    """Return the triple <s> <s> "lex"^^dt, with dt the datatype at the index."""
    return triple(
        s_iri=iri(0, 1),
        p_iri=iri(0, 1),
        o_literal=jelly.RdfLiteral(lex=lex, datatype=index),
    )


REJECTED = {
    "datatype entry past the end": (
        [options(), datatype(DATATYPES + 1, "http://dt")],
        "the lookup has 8 entries",
    ),
    "datatype entry id above 2^31": (
        [options(), datatype(2**32 - 1, "http://dt")],
        "the lookup has 8 entries",
    ),
    "datatype entry with no datatype lookup": (
        [options(datatypes=0), datatype(1, "http://dt")],
        "the lookup has 0 entries",
    ),
    "datatype entries that run off the end": (
        [options()] + [datatype(0, f"http://dt{i}") for i in range(DATATYPES + 1)],
        "the lookup has 8 entries",
    ),
    "literal with a datatype never set": (
        [options(), name(1, "http://s"), literal("1", 1)],
        "lookup entry 1, which is not set",
    ),
    "literal with a datatype past the end": (
        [options(), name(1, "http://s"), literal("1", DATATYPES + 1)],
        "lookup entry 9, which is not set",
    ),
    "literal with a datatype, with no datatype lookup": (
        [options(datatypes=0), name(1, "http://s"), literal("1", 1)],
        "lookup entry 1, which is not set",
    ),
    "namespace declaration with no IRI": (
        [options(), namespace("ex")],
        "which is not set",
    ),
    "namespace declaration with a name never set": (
        [options(), prefix(1, "http://example.org/"), namespace("ex", iri(1, 1))],
        "lookup entry 1, which is not set",
    ),
    "namespace declaration with a prefix never set": (
        [options(), name(1, "x"), namespace("ex", iri(1, 1))],
        "lookup entry 1, which is not set",
    ),
    "IRI with a name id above 2^31": (
        [options(), triple(s_iri=iri(0, 2**32 - 1), p_iri=iri(0, 1), o_iri=iri(0, 1))],
        "lookup entry 4294967295, which is not set",
    ),
    "IRI with a prefix id above 2^31": (
        [
            options(),
            name(1, "x"),
            triple(s_iri=iri(2**32 - 1, 1), p_iri=iri(0, 1), o_iri=iri(0, 1)),
        ],
        "lookup entry 4294967295, which is not set",
    ),
    "row with nothing in it": ([options(), Row()], "a row of the stream is empty"),
    "repeated stream options that are not the same": (
        [options(), options(datatypes=16)],
        "not the same as the first ones",
    ),
    "quads in a stream of triples": (
        [
            options(),
            name(1, "http://s"),
            Row(
                quad=jelly.RdfQuad(
                    s_iri=iri(0, 1), p_iri=iri(0, 1), o_iri=iri(0, 1), g_iri=iri(0, 1)
                )
            ),
        ],
        "PHYSICAL_STREAM_TYPE_TRIPLES cannot have quads",
    ),
    "graph start in a stream of triples": (
        [options(), Row(graph_start=jelly.RdfGraphStart(g_default_graph={}))],
        "PHYSICAL_STREAM_TYPE_TRIPLES cannot have graph start markers",
    ),
    "graph start with no graph": (
        [options(), Row(graph_start=jelly.RdfGraphStart())],
        "a graph start marker has no graph",
    ),
    "quoted triple with a repeated term": (
        [
            options(),
            name(1, "http://s"),
            triple(
                s_iri=iri(0, 1),
                p_iri=iri(0, 1),
                o_triple_term=jelly.RdfTriple(s_iri=iri(0, 1), p_iri=iri(0, 1)),
            ),
        ],
        "repeated terms are not allowed in quoted triples",
    ),
    "repeated term with no term before it": (
        [options(), triple()],
        "missing repeated term subject",
    ),
}


@pytest.mark.parametrize(("rows", "error"), list(REJECTED.values()), ids=list(REJECTED))
def test_rejects(rows: list[jelly.RdfStreamRow], error: str) -> None:
    with pytest.raises(JellyConformanceError, match=error):
        read_rows(*rows)


def test_rejects_namespace_declaration_with_a_name_never_set_in_columns() -> None:
    batch = jelly.RdfColumnBatch(
        prefixes=[jelly.RdfLookupEntryPacked(values=["http://example.org/"])],
        namespaces=[jelly.RdfNamespaceDeclaration(name="ex", value=iri(1, 1))],
    )
    stream_options = jelly.RdfStreamOptions(
        physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
        max_name_table_size=128,
        max_prefix_table_size=8,
        version=3,
    )
    out = io.BytesIO()
    write_delimited(
        jelly.RdfStreamFrame(rows=[Row(options=stream_options)], columns=batch), out
    )
    with pytest.raises(JellyConformanceError, match="which is not set"):
        read(out.getvalue())


# Length prefixes of delimited streams, after jelly-jvm's DelimitedMessageReaderSpec

RDF_FRAME = jelly.RdfStreamFrame(
    rows=[options(), name(1, "http://example.org/" + "x" * 3000)]
).SerializeToString()
SPARQL_FRAME = jelly.SparqlResultsFrame(
    options=jelly.SparqlResultsOptions(max_name_table_size=128, version=1),
    variables=["x"],
    row_count=1,
    names=[jelly.RdfLookupEntryPacked(values=["http://example.org/" + "x" * 3000])],
    columns=[jelly.RdfColumn(name_ids=[1])],
).SerializeToString()


def read_rdf(inp: IO[bytes]) -> None:
    list(parse_jelly_flat(inp))


def read_sparql(inp: IO[bytes]) -> None:
    for result_set in read_result_sets(inp, GenericStatementSinkAdapter):
        list(result_set)


READERS = {"rdf": (RDF_FRAME, read_rdf), "sparql": (SPARQL_FRAME, read_sparql)}

BAD_LENGTHS: dict[str, Callable[[bytes], bytes]] = {
    "frame shorter than its length, cut at 3": lambda delimited: delimited[:3],
    "frame shorter than its length, cut at 100": lambda delimited: delimited[:100],
    "frame shorter than its length by 1": lambda delimited: delimited[:-1],
    "length of 2^31 - 1": lambda _: b"\xff\xff\xff\xff\x07\x01\x02\x03",
    "length of 2^32 - 1": lambda _: b"\xff\xff\xff\xff\x0f\x01\x02\x03",
    "length of 2^64 - 1": lambda _: b"\xff" * 9 + b"\x01\x01\x02",
    "length cut off": lambda _: b"\x80",
    "length cut off after a frame": lambda delimited: delimited + b"\x80",
    "frame cut off after a frame": lambda delimited: delimited + delimited[:50],
}


@pytest.mark.parametrize("reader", list(READERS))
@pytest.mark.parametrize("bad", list(BAD_LENGTHS))
def test_rejects_bad_length(reader: str, bad: str) -> None:
    frame, read_stream = READERS[reader]
    delimited = wire_fuzzer.varint(len(frame)) + frame
    with pytest.raises(JellyConformanceError, match="Is it cut short"):
        read_stream(io.BytesIO(BAD_LENGTHS[bad](delimited)))


@pytest.mark.parametrize("reader", list(READERS))
def test_closed_input(reader: str) -> None:
    frame, read_stream = READERS[reader]
    inp = io.BytesIO(wire_fuzzer.varint(len(frame)) + frame)
    inp.close()
    with pytest.raises(ValueError, match="closed"):
        read_stream(inp)


def corpus() -> list[bytes]:
    """
    Return valid streams to start the fuzzing from.

    Triples and quads in the row layout (Jelly-RDF 1.1) and the column layout
    (1.2), delimited and not. They have every term type, lookup entries, repeated
    terms, namespace declarations and quoted triples.
    """
    ex = "http://example.org/"
    s, p, o = IRI(ex + "s"), IRI(ex + "p"), IRI(ex + "o")
    triples = [
        Triple(s, p, o),
        Triple(s, p, Literal("plain")),
        Triple(s, IRI(ex + "q"), Literal("1", datatype="http://dt/int")),
        Triple(BlankNode("b1"), p, Literal("hello", langtag="en")),
        Triple(BlankNode("b1"), p, Triple(s, p, o)),
        Triple(s, p, o),
    ]
    graph = IRI(ex + "g")
    quads = [
        Quad(t.s, t.p, t.o, graph if i % 2 else DefaultGraph)
        for i, t in enumerate(triples)
    ]
    streams = []
    for statements, logical_type in (
        (triples, jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        (quads, jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS),
    ):
        for version, frame_size in ((2, 4), (2, 250), (3, 4), (3, 250)):
            sink = GenericStatementSink()
            sink.bind("ex", IRI(ex))
            for statement in statements:
                sink.add(statement)
            params = StreamParameters(
                version=version,
                rdf_star=version == 2,
                namespace_declarations=True,
                delimited=frame_size == 4,
            )
            serializer_options = SerializerOptions(
                frame_size=frame_size,
                logical_type=logical_type,
                params=params,
                lookup_preset=LookupPreset(
                    max_names=128, max_prefixes=8, max_datatypes=8
                ),
            )
            out = io.BytesIO()
            for frame in stream_frames(guess_stream(serializer_options, sink), sink):
                if params.delimited:
                    write_delimited(frame, out)
                else:
                    write_single(frame, out)
            streams.append(out.getvalue())
    return streams


def test_corpus_is_valid() -> None:
    for data in corpus():
        assert len([s for s in read(data) if isinstance(s, (Triple, Quad))]) == 6


def test_fuzz_mutated_streams() -> None:
    reached = 0

    def consume(data: bytes) -> None:
        nonlocal reached
        try:
            read(data)
        except JellyConformanceError as error:
            if "Is it cut short" not in str(error):
                reached += 1
            raise
        reached += 1

    # Options whose physical and logical types don't match raise
    # JellyAssertionError
    found = byte_fuzzer.findings(
        corpus(), consume, (JellyConformanceError, JellyAssertionError)
    )
    assert not found, "\n".join(found)
    # Most mutations are rejected by protobuf. At least 5% must get to the decoder
    assert reached > byte_fuzzer.ITERATIONS // 20
