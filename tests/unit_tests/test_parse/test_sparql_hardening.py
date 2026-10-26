"""
Reading invalid Jelly-SPARQL input, after jelly-jvm's SparqlDecoderHardeningSpec.

The frames are valid protobuf, but declare huge sizes or refer to things that are
not there. The reader must reject them with JellyConformanceError, without
allocating memory for the declared sizes.
"""

from __future__ import annotations

import io
import warnings
from typing import Any

import pytest

from pyjelly import jelly
from pyjelly.errors import (
    JellyConformanceError,
    JellyIncompleteResultsError,
    JellyTruncatedResultsWarning,
)
from pyjelly.integrations.generic import sparql as generic_sparql
from pyjelly.integrations.generic.decode import GenericStatementSinkAdapter
from pyjelly.integrations.generic.generic_sink import IRI, BlankNode, Literal
from pyjelly.options import (
    MAX_SPARQL_FRAME_ROWS,
    MAX_SPARQL_FRAME_VALUES,
    LookupPreset,
    SparqlOptions,
)
from pyjelly.parse.sparql import SparqlDecoder, read_result_sets
from tests.utils import byte_fuzzer

OPTIONS = jelly.SparqlResultsOptions(
    max_name_table_size=128,
    max_prefix_table_size=16,
    max_datatype_table_size=16,
    version=1,
)
DATATYPES = OPTIONS.max_datatype_table_size

Entry = jelly.RdfLookupEntryPacked
Column = jelly.RdfColumn

KIND_IRI = 0
KIND_LITERAL = 1
KIND_BNODE = 2


def decoder(
    *, max_rows: int = MAX_SPARQL_FRAME_ROWS, max_values: int = MAX_SPARQL_FRAME_VALUES
) -> SparqlDecoder:
    return SparqlDecoder(
        GenericStatementSinkAdapter, max_rows=max_rows, max_values=max_values
    )


def frame(
    rows: int, *columns: jelly.RdfColumn, **fields: Any
) -> jelly.SparqlResultsFrame:
    """Return a frame with the stream options and the variable x, unless given."""
    fields.setdefault("variables", ["x"])
    return jelly.SparqlResultsFrame(
        options=OPTIONS, row_count=rows, columns=list(columns), **fields
    )


def kinds(values: list[int]) -> bytes:
    """Pack the kinds of the values, 2 bits each."""
    out = bytearray((len(values) + 3) // 4)
    for i, kind in enumerate(values):
        out[i >> 2] |= kind << ((i & 3) << 1)
    return bytes(out)


def datatype(index: int) -> int:
    """Return the literal kind of a literal with this datatype."""
    return 2 * index - 1


def langtag(index: int) -> int:
    """Return the literal kind of a literal with this language tag."""
    return 2 * index + 2


def decode(data: jelly.SparqlResultsFrame, **limits: int) -> list[Any]:
    return decoder(**limits).decode(data).rows


REJECTED = {
    "row count above 2^27": (frame(1 << 27, variables=[]), "at most 134217727 rows"),
    "row count above 2^31": (frame(2**31 - 1, Column()), "at most 134217727 rows"),
    "row count above 2^32 - 1": (
        frame(2**32 - 1, variables=[]),
        "not 4294967295",
    ),
    "row count above the default limit": (
        frame(MAX_SPARQL_FRAME_ROWS + 1, variables=[]),
        f"more than the {MAX_SPARQL_FRAME_ROWS} allowed",
    ),
    "datatype entry past the end": (
        frame(0, Column(), datatypes=[Entry(id=DATATYPES + 1, values=["http://dt"])]),
        "the lookup has 16 entries",
    ),
    "datatype entry id above 2^31": (
        frame(0, Column(), datatypes=[Entry(id=2**32 - 1, values=["http://dt"])]),
        "the lookup has 16 entries",
    ),
    "datatype entries that run off the end": (
        frame(
            0,
            Column(),
            datatypes=[
                Entry(id=1, values=[f"http://dt{i}" for i in range(DATATYPES + 1)])
            ],
        ),
        "the lookup has 16 entries",
    ),
    "literal with a datatype past the end": (
        frame(1, Column(lex_values=["1"], literal_kinds=[datatype(DATATYPES + 1)])),
        "outside the datatype lookup",
    ),
    "literal with a datatype never set": (
        frame(1, Column(lex_values=["1"], literal_kinds=[datatype(1)])),
        "lookup entry 1, which is not set",
    ),
    "literal kind per value with a datatype never set": (
        frame(2, Column(lex_values=["1", "2"], literal_kinds=[0, datatype(1)])),
        "lookup entry 1, which is not set",
    ),
    "literal with a language tag the column does not have": (
        frame(
            2,
            Column(
                lex_values=["a", "b"],
                literal_kinds=[langtag(0), langtag(1)],
                langtags=["en"],
            ),
        ),
        "language tag 1",
    ),
    "uniform literal kind with no language tags": (
        frame(1, Column(lex_values=["a"], literal_kinds=[langtag(0)])),
        "language tag 0",
    ),
    "more than one literal kind, but not one per literal": (
        frame(3, Column(lex_values=["a", "b", "c"], literal_kinds=[0, 0])),
        "2 literal kinds for 3 literals",
    ),
    "base directions for only some language tags": (
        frame(
            2,
            Column(
                lex_values=["a", "b"],
                literal_kinds=[langtag(0), langtag(1)],
                langtags=["en", "fr"],
                langtag_directions=[jelly.RDF_BASE_DIRECTION_LTR],
            ),
        ),
        "1 base directions for 2 language tags",
    ),
    "unknown base direction": (
        frame(
            1,
            Column(
                lex_values=["a"],
                literal_kinds=[langtag(0)],
                langtags=["en"],
                langtag_directions=[7],
            ),
        ),
        "unknown base direction: 7",
    ),
    "kinds of the wrong length": (
        frame(2, Column(kinds=b"\x06\x00", bnodes=["a"], lex_values=["x"])),
        "2 bytes of kinds for 2 values",
    ),
    "kinds that do not match the values": (
        frame(
            2,
            Column(
                kinds=kinds([KIND_BNODE, KIND_BNODE]), bnodes=["a"], lex_values=["x"]
            ),
        ),
        "more blank nodes than it has",
    ),
    "kinds with unused bits set": (
        frame(2, Column(kinds=bytes([0x06 | 0x40]), bnodes=["a"], lex_values=["x"])),
        "unused bits",
    ),
    "IRI with a name never set": (
        frame(
            1,
            Column(name_ids=[1], prefix_ids=[1]),
            prefixes=[Entry(id=1, values=["http://example.org/"])],
        ),
        "name 1 or prefix 1, which is not set",
    ),
    "IRI with a prefix never set": (
        frame(
            1,
            Column(name_ids=[1], prefix_ids=[1]),
            names=[Entry(id=1, values=["x"])],
        ),
        "name 1 or prefix 1, which is not set",
    ),
    "IRI with kinds, with a name never set": (
        frame(
            1,
            Column(kinds=kinds([KIND_IRI]), name_ids=[1], prefix_ids=[1]),
            prefixes=[Entry(id=1, values=["http://example.org/"])],
        ),
        "name 1 or prefix 1, which is not set",
    ),
    "IRI with a name id above 2^31": (
        frame(1, Column(name_ids=[2**32 - 1])),
        "outside the lookups",
    ),
    "IRI with a prefix id above 2^31": (
        frame(
            1,
            Column(name_ids=[1], prefix_ids=[2**32 - 1]),
            names=[Entry(id=1, values=["x"])],
        ),
        "outside the lookups",
    ),
}


@pytest.mark.parametrize(("data", "error"), list(REJECTED.values()), ids=list(REJECTED))
def test_rejects(data: jelly.SparqlResultsFrame, error: str) -> None:
    with pytest.raises(JellyConformanceError, match=error):
        decode(data)


def test_row_count_up_to_the_limit() -> None:
    data = frame(
        5, Column(name_ids=[1]), names=[Entry(id=1, values=["http://example.org/x"])]
    )
    rows = decode(data, max_rows=5)
    assert rows == [(IRI("http://example.org/x"),)] + [(None,)] * 4
    with pytest.raises(JellyConformanceError, match="more than the 4 allowed"):
        decode(data, max_rows=4)


def test_value_count_above_the_default_limit() -> None:
    # 1000 empty columns of 2^20 rows each: a few kilobytes of input that would
    # need gigabytes of cells. The row count is within the row limit.
    variables = [f"v{i}" for i in range(1000)]
    data = frame(
        MAX_SPARQL_FRAME_ROWS, *[Column()] * len(variables), variables=variables
    )
    with pytest.raises(
        JellyConformanceError, match=f"the {MAX_SPARQL_FRAME_VALUES} values allowed"
    ):
        decode(data)


def test_value_count_up_to_the_limit() -> None:
    def data(rows: int) -> jelly.SparqlResultsFrame:
        return frame(rows, Column(), Column(), variables=["x", "y"])

    assert len(decode(data(5), max_values=10)) == 5
    with pytest.raises(JellyConformanceError, match="6 rows of 2 variables"):
        decode(data(6), max_values=10)


# A column long enough to check that the kinds are read to the end
LONG_COLUMN = 70_001


def long_mixed_column() -> tuple[jelly.RdfColumn, list[int]]:
    """Return a column of blank nodes and literals, and its kinds."""
    values = [KIND_BNODE if i % 3 == 0 else KIND_LITERAL for i in range(LONG_COLUMN)]
    column = Column(
        kinds=kinds(values),
        bnodes=[f"b{i}" for i in range(0, LONG_COLUMN, 3)],
        lex_values=[f"l{i}" for i in range(LONG_COLUMN) if i % 3],
    )
    return column, values


def test_long_mixed_column() -> None:
    column, _ = long_mixed_column()
    rows = decode(frame(LONG_COLUMN, column))
    assert len(rows) == LONG_COLUMN
    for i in (0, 1, 65531, 65532, 65533, 70000):
        expected = BlankNode(f"b{i}") if i % 3 == 0 else Literal(f"l{i}")
        assert str(rows[i][0]) == str(expected)


def test_long_mixed_column_with_a_mismatch_near_the_end() -> None:
    column, values = long_mixed_column()
    # Value 69998 is a literal, the kinds say it is a blank node
    values[69998] = KIND_BNODE
    column.kinds = kinds(values)
    with pytest.raises(JellyConformanceError, match="more blank nodes than it has"):
        decode(frame(LONG_COLUMN, column))


def test_kinds_on_a_column_of_one_type() -> None:
    column = Column(kinds=kinds([KIND_BNODE, KIND_BNODE]), bnodes=["a", "b"])
    rows = decode(frame(2, column))
    assert [str(row[0]) for row in rows] == ["_:a", "_:b"]


def test_base_direction_checked_only_where_used() -> None:
    # The second tag has an unknown direction, but no value refers to it
    column = Column(
        lex_values=["a"],
        literal_kinds=[langtag(0)],
        langtags=["en", "fr"],
        langtag_directions=[jelly.RDF_BASE_DIRECTION_UNSPECIFIED, 7],
    )
    assert decode(frame(1, column)) == [(Literal("a", langtag="en"),)]


def test_column_with_no_values() -> None:
    assert decode(frame(1, Column())) == [(None,)]


def corpus() -> list[bytes]:
    """
    Return valid streams to start the fuzzing from.

    They have every term type, columns with and without kinds, lookup entries,
    repeated and unbound cells, delimited and non-delimited streams, a trailer
    and a boolean result.
    """
    a = IRI("http://example.org/a")
    rows: list[tuple[Any, ...]] = [
        (a, Literal("plain"), BlankNode("b1")),
        # Repeats the row before it
        (a, Literal("plain"), BlankNode("b1")),
        (IRI("http://example.org/b"), Literal("1", datatype="http://dt/int"), None),
        # Mixes term types in the columns, so that they get kinds
        (BlankNode("b2"), Literal("hello", langtag="en"), IRI("http://example.org/c")),
        (None, None, None),
        (
            IRI("http://example.org/d"),
            Literal("x", langtag="ar", direction="rtl"),
            BlankNode("b3"),
        ),
    ]
    small = LookupPreset(max_names=128, max_prefixes=8, max_datatypes=8)
    streams = []
    for options in (
        SparqlOptions(lookup_preset=small, delimited=False),
        SparqlOptions(lookup_preset=small, frame_values=6),
    ):
        out = io.BytesIO()
        generic_sparql.serialize_jelly_sparql(["x", "y", "z"], rows, out, options)
        streams.append(out.getvalue())
    out = io.BytesIO()
    generic_sparql.serialize_jelly_sparql_ask(
        value=True, out=out, options=SparqlOptions(lookup_preset=small, delimited=False)
    )
    streams.append(out.getvalue())
    return streams


def read_all(data: bytes) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", JellyTruncatedResultsWarning)
        # A low row limit, so that mutated row counts are rejected quickly
        for result_set in read_result_sets(
            data, GenericStatementSinkAdapter, max_rows=64
        ):
            if result_set.is_boolean:
                result_set.boolean  # noqa: B018
            else:
                list(result_set)


def test_corpus_is_valid() -> None:
    for data in corpus():
        read_all(data)


def test_fuzz_mutated_streams() -> None:
    reached = 0

    def consume(data: bytes) -> None:
        nonlocal reached
        try:
            read_all(data)
        except JellyConformanceError as error:
            if "Is it cut short" not in str(error):
                reached += 1
            raise
        reached += 1

    found = byte_fuzzer.findings(
        corpus(), consume, (JellyConformanceError, JellyIncompleteResultsError)
    )
    assert not found, "\n".join(found)
    # Most mutations are rejected by protobuf. At least 5% must get to the decoder
    assert reached > byte_fuzzer.ITERATIONS // 20
