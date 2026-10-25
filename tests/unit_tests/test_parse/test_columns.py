"""Reading Jelly-RDF 1.2: what the conformance tests don't cover."""

from __future__ import annotations

import io
from typing import Any

import pytest
from rdflib import Graph

from pyjelly import jelly
from pyjelly.errors import JellyConformanceError
from pyjelly.integrations.generic.generic_sink import IRI, Triple
from pyjelly.integrations.generic.parse import parse_jelly_flat
from pyjelly.parse.columns import MESSAGE_END
from pyjelly.parse.decode import check_stream_options
from pyjelly.serialize.ioutils import write_delimited


def options(**fields: Any) -> jelly.RdfStreamOptions:
    values: dict[str, Any] = {
        "physical_type": jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
        "max_name_table_size": 128,
        "max_prefix_table_size": 8,
        "max_datatype_table_size": 8,
        "version": 3,
    }
    values.update(fields)
    return jelly.RdfStreamOptions(**values)


def options_row(**fields: Any) -> list[jelly.RdfStreamRow]:
    return [jelly.RdfStreamRow(options=options(**fields))]


def stream(*frames: jelly.RdfStreamFrame) -> io.BytesIO:
    out = io.BytesIO()
    for frame in frames:
        write_delimited(frame, out)
    out.seek(0)
    return out


def one_triple(objects: jelly.RdfColumn) -> jelly.RdfColumnBatch:
    """Return a batch of one triple <a> <a> o, where name 1 is the IRI a."""
    return jelly.RdfColumnBatch(
        row_count=1,
        names=[jelly.RdfLookupEntryPacked(values=["http://a"])],
        subjects=jelly.RdfColumn(name_ids=[1]),
        predicates=jelly.RdfColumn(name_ids=[1]),
        objects=objects,
    )


def test_statements_before_the_options() -> None:
    frames = (
        jelly.RdfStreamFrame(columns=jelly.RdfColumnBatch()),
        jelly.RdfStreamFrame(rows=options_row()),
    )
    with pytest.raises(JellyConformanceError, match="before the stream options"):
        list(parse_jelly_flat(stream(*frames)))


def test_namespace_declaration_without_iri() -> None:
    batch = jelly.RdfColumnBatch(namespaces=[jelly.RdfNamespaceDeclaration(name="ex")])
    frame = jelly.RdfStreamFrame(rows=options_row(), columns=batch)
    with pytest.raises(JellyConformanceError, match="'ex' has no IRI"):
        list(parse_jelly_flat(stream(frame)))


def test_base_direction_in_rdf_1_1() -> None:
    objects = jelly.RdfColumn(
        lex_values=["a"],
        literal_kinds=[2],
        langtags=["en"],
        langtag_directions=[jelly.RDF_BASE_DIRECTION_LTR],
    )
    frame = jelly.RdfStreamFrame(
        rows=options_row(rdf_version=jelly.RDF_VERSION_1_1),
        columns=one_triple(objects),
    )
    with pytest.raises(JellyConformanceError, match=r"declares RDF 1\.1"):
        list(parse_jelly_flat(stream(frame)))


def triple_term() -> jelly.RdfColumn:
    term = jelly.RdfTripleTerm(
        s_iri=jelly.RdfIri(name_id=1),
        p_iri=jelly.RdfIri(name_id=1),
        o_iri=jelly.RdfIri(name_id=1),
    )
    return jelly.RdfColumn(triple_terms=[term])


@pytest.mark.parametrize(
    "rdf_version", [jelly.RDF_VERSION_1_1, jelly.RDF_VERSION_1_2_BASIC]
)
def test_triple_terms_in_rdf_versions_without_them(
    rdf_version: jelly.RdfVersion,
) -> None:
    frame = jelly.RdfStreamFrame(
        rows=options_row(rdf_version=rdf_version), columns=one_triple(triple_term())
    )
    with pytest.raises(JellyConformanceError, match="without triple terms"):
        list(parse_jelly_flat(stream(frame)))


def test_triple_terms() -> None:
    frame = jelly.RdfStreamFrame(rows=options_row(), columns=one_triple(triple_term()))
    a = IRI("http://a")
    assert list(parse_jelly_flat(stream(frame))) == [Triple(a, a, Triple(a, a, a))]


@pytest.mark.parametrize(
    ("objects", "feature"),
    [
        (
            jelly.RdfColumn(
                lex_values=["a"],
                literal_kinds=[2],
                langtags=["en"],
                langtag_directions=[jelly.RDF_BASE_DIRECTION_RTL],
            ),
            "literals with a base direction",
        ),
        (triple_term(), "quoted triple"),
    ],
)
def test_rdflib_has_no_rdf_1_2_terms(objects: jelly.RdfColumn, feature: str) -> None:
    frame = jelly.RdfStreamFrame(rows=options_row(), columns=one_triple(objects))
    with pytest.raises(NotImplementedError, match=feature):
        Graph().parse(stream(frame), format="jelly")


def test_row_layout_streams_ignore_columns() -> None:
    triple = jelly.RdfTriple(
        s_iri=jelly.RdfIri(name_id=1),
        p_iri=jelly.RdfIri(name_id=1),
        o_iri=jelly.RdfIri(name_id=1),
    )
    rows = [
        *options_row(version=2, logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES),
        jelly.RdfStreamRow(name=jelly.RdfNameEntry(value="http://a")),
        jelly.RdfStreamRow(triple=triple),
    ]
    frame = jelly.RdfStreamFrame(rows=rows, columns=one_triple(triple_term()))
    a = IRI("http://a")
    assert list(parse_jelly_flat(stream(frame))) == [Triple(a, a, a)]


def test_message_end() -> None:
    assert repr(MESSAGE_END) == "MESSAGE_END"


@pytest.mark.parametrize(
    ("fields", "error"),
    [
        ({"version": 2, "stream_type": 1}, "only valid in Jelly-RDF 1.2"),
        ({"version": 2, "rdf_version": 1}, "only valid in Jelly-RDF 1.2"),
        ({"version": 2, "logical_type": 99}, "unknown logical stream type"),
        ({"version": 1, "max_name_table_size": 7}, "name lookup size is 7"),
        ({"max_prefix_table_size": 4097}, "prefix lookup size is 4097"),
        ({"max_datatype_table_size": 257}, "datatype lookup size is 257"),
        ({"max_name_table_size": 16385}, "name lookup size is 16385"),
    ],
)
def test_stream_options_the_parser_rejects(fields: dict[str, Any], error: str) -> None:
    with pytest.raises(JellyConformanceError, match=error):
        check_stream_options(options(**fields))


def test_largest_lookups_the_parser_accepts() -> None:
    check_stream_options(
        options(
            max_name_table_size=16384,
            max_prefix_table_size=4096,
            max_datatype_table_size=256,
        )
    )
    check_stream_options(options(version=1, max_name_table_size=8))
