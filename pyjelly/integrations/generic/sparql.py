"""
Jelly-SPARQL support for the generic API (SPARQL query results without rdflib).

Values are generic API terms (IRI, Literal, BlankNode, and Triple for triple
terms). Unbound variables are None.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from typing import IO

from pyjelly.integrations.generic.decode import GenericStatementSinkAdapter
from pyjelly.integrations.generic.generic_sink import Node
from pyjelly.integrations.generic.serialize import GenericSinkTermEncoder
from pyjelly.options import SparqlOptions
from pyjelly.parse.sparql import SparqlResultSet, read_result_set, read_result_sets
from pyjelly.serialize.sparql import FrameWriter, SparqlStream, write_solutions


def parse_jelly_sparql(inp: IO[bytes] | bytes) -> SparqlResultSet:
    """
    Read SPARQL query results from Jelly-SPARQL.

    Rows are read during iteration (only once), so the input must stay open
    until then.

    Args:
        inp (IO[bytes] | bytes): the stream, delimited or not. It must have one
            result set (the FLAT stream type, the default).

    Returns:
        SparqlResultSet: the results. Iterating it gives the rows, as tuples with
            one value per variable, None if the variable is unbound.

    Raises:
        JellyIncompleteResultsError: if the writer reported that the results are
            incomplete. Raised at the end of the iteration, or right away for a
            boolean result.

    """
    return read_result_set(inp, GenericStatementSinkAdapter)


def parse_jelly_sparql_sequence(inp: IO[bytes] | bytes) -> Iterator[SparqlResultSet]:
    """
    Read a sequence of SPARQL query results from one Jelly-SPARQL stream.

    This reads PUNCTUATED streams, which hold any number of results. A FLAT
    stream (the default) holds one. Rows are read during iteration. If the next
    result set is read first, the unread rows are kept in memory.

    Args:
        inp (IO[bytes] | bytes): the stream, delimited.

    Yields:
        SparqlResultSet: the results, one per result set.

    """
    return read_result_sets(inp, GenericStatementSinkAdapter)


def sparql_stream(options: SparqlOptions | None = None) -> SparqlStream:
    """Create a Jelly-SPARQL writer for generic API terms."""
    if options is None:
        options = SparqlOptions()
    encoder = GenericSinkTermEncoder(lookup_preset=options.lookup_preset)
    return SparqlStream(encoder, options)


def serialize_jelly_sparql(
    variables: Iterable[str],
    rows: Iterable[Sequence[Node | None]],
    out: IO[bytes],
    options: SparqlOptions | None = None,
    *,
    links: Sequence[str] = (),
) -> None:
    """
    Write a solution sequence (the result of a SELECT query) in Jelly-SPARQL.

    If the rows fail midway (e.g., the query fails), the stream is ended with the
    error so readers know the results are incomplete, and the exception is
    re-raised.

    Args:
        variables (Iterable[str]): variable names, in projection order.
        rows (Iterable[Sequence[Node | None]]): the rows, with one value per
            variable, None if the variable is unbound.
        out (IO[bytes]): output stream.
        options (SparqlOptions | None): options of the stream.
        links (Sequence[str]): link IRIs for the results.

    """
    stream = sparql_stream(options)
    writer = FrameWriter(out, delimited=stream.options.delimited)
    write_solutions(stream, variables, rows, writer, links=links)


def serialize_jelly_sparql_ask(
    value: bool,  # noqa: FBT001
    out: IO[bytes],
    options: SparqlOptions | None = None,
    *,
    links: Sequence[str] = (),
) -> None:
    """
    Write a boolean result (of an ASK query) in Jelly-SPARQL.

    Args:
        value (bool): the result.
        out (IO[bytes]): output stream.
        options (SparqlOptions | None): options of the stream.
        links (Sequence[str]): link IRIs for the result.

    """
    stream = sparql_stream(options)
    FrameWriter(out, delimited=stream.options.delimited).write(
        stream.ask(value, links=links)
    )


#: One result in a sequence: (variables, rows) for a SELECT query, or a bool for an
#: ASK query
SequenceItem = tuple[Iterable[str], Iterable[Sequence[Node | None]]] | bool


def serialize_jelly_sparql_sequence(
    results: Iterable[SequenceItem],
    out: IO[bytes],
    options: SparqlOptions | None = None,
) -> None:
    """
    Write a sequence of SPARQL query results in one Jelly-SPARQL stream.

    The stream is PUNCTUATED, with one result set per result.

    Args:
        results (Iterable[SequenceItem]): the results, each a (variables, rows)
            pair for a SELECT query, or a bool for an ASK query.
        out (IO[bytes]): output stream.
        options (SparqlOptions | None): options of the stream, which must be
            punctuated. Defaults to SparqlOptions(punctuated=True).

    """
    if options is None:
        options = SparqlOptions(punctuated=True)
    if not options.punctuated:
        msg = "a sequence of results needs SparqlOptions(punctuated=True)"
        raise ValueError(msg)
    stream = sparql_stream(options)
    writer = FrameWriter(out, delimited=True)
    for result in results:
        if isinstance(result, bool):
            writer.write(stream.ask(result))
        else:
            variables, rows = result
            write_solutions(stream, variables, rows, writer)
