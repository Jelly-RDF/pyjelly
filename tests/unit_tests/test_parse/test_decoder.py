import pytest

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.decode import GenericTriplesAdapter
from pyjelly.integrations.generic.generic_sink import IRI, Literal
from pyjelly.options import VERSION_1_1, LookupPreset, StreamParameters, StreamTypes
from pyjelly.parse.decode import Decoder, ParserOptions


def make_decoder() -> Decoder:
    options = ParserOptions(
        stream_types=StreamTypes(),
        lookup_preset=LookupPreset(max_names=8, max_prefixes=8),
        params=StreamParameters(version=VERSION_1_1),
    )
    return Decoder(adapter=GenericTriplesAdapter(options))


def test_iri_terms_are_reused_until_their_entries_change() -> None:
    decoder = make_decoder()
    decoder.ingest_prefix_entry(jelly.RdfPrefixEntry(id=1, value="http://a/"))
    decoder.ingest_prefix_entry(jelly.RdfPrefixEntry(id=2, value="http://b/"))
    decoder.ingest_name_entry(jelly.RdfNameEntry(id=1, value="x"))

    first = decoder.decode_iri(jelly.RdfIri(prefix_id=1, name_id=1))
    assert first == IRI("http://a/x")
    assert decoder.decode_iri(jelly.RdfIri(prefix_id=1, name_id=1)) is first
    # Prefix 0 repeats the last prefix, name 0 is the last name + 1
    decoder.ingest_name_entry(jelly.RdfNameEntry(id=2, value="y"))
    assert decoder.decode_iri(jelly.RdfIri(prefix_id=0, name_id=0)) == IRI("http://a/y")

    assert decoder.decode_iri(jelly.RdfIri(prefix_id=2, name_id=1)) == IRI("http://b/x")
    decoder.ingest_prefix_entry(jelly.RdfPrefixEntry(id=1, value="http://c/"))
    assert decoder.decode_iri(jelly.RdfIri(prefix_id=1, name_id=1)) == IRI("http://c/x")
    decoder.ingest_name_entry(jelly.RdfNameEntry(id=1, value="z"))
    assert decoder.decode_iri(jelly.RdfIri(prefix_id=1, name_id=1)) == IRI("http://c/z")


def test_literal_terms_are_reused_only_when_equal() -> None:
    decoder = make_decoder()
    decoder.ingest_datatype_entry(jelly.RdfDatatypeEntry(id=1, value="http://dt/a"))
    decoder.ingest_datatype_entry(jelly.RdfDatatypeEntry(id=2, value="http://dt/b"))

    plain = decoder.decode_literal(jelly.RdfLiteral(lex="1"))
    assert decoder.decode_literal(jelly.RdfLiteral(lex="1")) is plain
    literals = [
        plain,
        decoder.decode_literal(jelly.RdfLiteral(lex="2")),
        decoder.decode_literal(jelly.RdfLiteral(lex="1", langtag="en")),
        decoder.decode_literal(jelly.RdfLiteral(lex="1", langtag="de")),
        decoder.decode_literal(jelly.RdfLiteral(lex="1", datatype=1)),
        decoder.decode_literal(jelly.RdfLiteral(lex="1", datatype=2)),
    ]
    assert literals == [
        Literal("1"),
        Literal("2"),
        Literal("1", langtag="en"),
        Literal("1", langtag="de"),
        Literal("1", datatype="http://dt/a"),
        Literal("1", datatype="http://dt/b"),
    ]


def test_repeated_terms_must_be_set_first() -> None:
    decoder = make_decoder()
    decoder.ingest_prefix_entry(jelly.RdfPrefixEntry(id=1, value="http://a/"))
    decoder.ingest_name_entry(jelly.RdfNameEntry(id=1, value="x"))
    iri = jelly.RdfIri(prefix_id=1, name_id=1)
    with pytest.raises(JellyConformanceError, match="missing repeated term predicate"):
        decoder.decode_triple(jelly.RdfTriple(s_iri=iri, o_iri=iri))

    # Terms set once can be repeated
    first = decoder.decode_triple(jelly.RdfTriple(s_iri=iri, p_iri=iri, o_bnode="b"))
    assert decoder.decode_triple(jelly.RdfTriple(o_iri=iri)) == (
        first[0],
        first[1],
        IRI("http://a/x"),
    )


def test_empty_row_is_an_error() -> None:
    frame = jelly.RdfStreamFrame(rows=[jelly.RdfStreamRow()])
    with pytest.raises(JellyConformanceError, match="a row of the stream is empty"):
        list(make_decoder().iter_rows(frame))
