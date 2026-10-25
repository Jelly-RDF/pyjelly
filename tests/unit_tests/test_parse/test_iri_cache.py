from pyjelly import jelly
from pyjelly.integrations.generic.generic_sink import IRI
from pyjelly.integrations.generic.parse import GenericStatementSinkAdapter
from pyjelly.options import LookupPreset, StreamParameters, StreamTypes
from pyjelly.parse.decode import Decoder, ParserOptions


def make_decoder() -> Decoder:
    options = ParserOptions(
        stream_types=StreamTypes(),
        lookup_preset=LookupPreset(max_names=8, max_prefixes=8),
        params=StreamParameters(),
    )
    return Decoder(adapter=GenericStatementSinkAdapter(options))


def test_iri_terms_are_reused_until_their_entries_change() -> None:
    decoder = make_decoder()
    decoder.ingest_prefix_entry(jelly.RdfPrefixEntry(id=1, value="http://a/"))
    decoder.ingest_prefix_entry(jelly.RdfPrefixEntry(id=2, value="http://b/"))
    decoder.ingest_name_entry(jelly.RdfNameEntry(id=1, value="x"))

    first = decoder.decode_iri(jelly.RdfIri(prefix_id=1, name_id=1))
    assert first == IRI("http://a/x")
    # Same entries: the same term
    assert decoder.decode_iri(jelly.RdfIri(prefix_id=1, name_id=1)) is first
    # Prefix 0 repeats the last prefix, name 0 is the last name + 1
    decoder.ingest_name_entry(jelly.RdfNameEntry(id=2, value="y"))
    assert decoder.decode_iri(jelly.RdfIri(prefix_id=0, name_id=0)) == IRI("http://a/y")

    # Another prefix
    assert decoder.decode_iri(jelly.RdfIri(prefix_id=2, name_id=1)) == IRI("http://b/x")
    # The prefix entry is overwritten
    decoder.ingest_prefix_entry(jelly.RdfPrefixEntry(id=1, value="http://c/"))
    assert decoder.decode_iri(jelly.RdfIri(prefix_id=1, name_id=1)) == IRI("http://c/x")
    # The name entry is overwritten
    decoder.ingest_name_entry(jelly.RdfNameEntry(id=1, value="z"))
    assert decoder.decode_iri(jelly.RdfIri(prefix_id=1, name_id=1)) == IRI("http://c/z")
