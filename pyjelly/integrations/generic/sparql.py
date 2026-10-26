"""
Jelly-SPARQL with the generic API: SPARQL query results, without rdflib.

The values of the results are the terms of the generic API (IRI, Literal,
BlankNode, and Triple for triple terms), and None for unbound variables.
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

    The rows are read as they are iterated (once), so the input must stay open
    until then.

    Args:
        inp (IO[bytes] | bytes): the stream, delimited or not. It must have one
            result set (the FLAT stream type, the default).

    Returns:
        SparqlResultSet: the results. Iterating it gives the rows, as tuples with
            one value per variable, None if the variable is unbound.

    Raises:
        JellyIncompleteResultsError: if the writer said that the results are
            incomplete: when the rows were iterated, or now for a boolean result.

    """
    return read_result_set(inp, GenericStatementSinkAdapter)


def parse_jelly_sparql_sequence(inp: IO[bytes] | bytes) -> Iterator[SparqlResultSet]:
    """
    Read a sequence of SPARQL query results from one Jelly-SPARQL stream.

    The stream has the PUNCTUATED type, which holds any number of results (a
    stream of the FLAT type, the default, has one). The rows are read as they are
    iterated. Rows not yet iterated when the next result set is read are kept in
    memory.

    Args:
        inp (IO[bytes] | bytes): the stream, delimited.

    Yields:
        SparqlResultSet: the results, one per result set.

    """
    return read_result_sets(inp, GenericStatementSinkAdapter)


def sparql_stream(options: SparqlOptions | None = None) -> SparqlStream:
    """Return a stream that writes Jelly-SPARQL with terms of the generic API."""
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

    If getting the rows fails midway (e.g., the query fails), the stream ends with
    the error, so that readers know the results are incomplete, and the error is
    raised.

    Args:
        variables (Iterable[str]): names of the variables, in projection order.
        rows (Iterable[Sequence[Node | None]]): the rows, with one value per
            variable, None if the variable is unbound.
        out (IO[bytes]): where to write the results.
        options (SparqlOptions | None): options of the stream.
        links (Sequence[str]): IRIs of links that describe the results.

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
        out (IO[bytes]): where to write it.
        options (SparqlOptions | None): options of the stream.
        links (Sequence[str]): IRIs of links that describe the result.

    """
    stream = sparql_stream(options)
    FrameWriter(out, delimited=stream.options.delimited).write(
        stream.ask(value, links=links)
    )


#: A result in a sequence of results: the variables and the rows of a solution
#: sequence, or a boolean result
SequenceItem = tuple[Iterable[str], Iterable[Sequence[Node | None]]] | bool


def serialize_jelly_sparql_sequence(
    results: Iterable[SequenceItem],
    out: IO[bytes],
    options: SparqlOptions | None = None,
) -> None:
    """
    Write a sequence of SPARQL query results in one Jelly-SPARQL stream.

    The stream has the PUNCTUATED type: each result is a result set of its own.

    Args:
        results (Iterable[SequenceItem]): the results: for a SELECT query, a pair
            of the variables and the rows, and for an ASK query, its boolean result.
        out (IO[bytes]): where to write them.
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
