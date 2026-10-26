"""
Jelly-SPARQL for rdflib: SPARQL query results (rdflib.query.Result) in Jelly.

With pyjelly installed, rdflib reads and writes them in the "jelly" format:

    result = graph.query("SELECT ?s ?o WHERE { ?s ?p ?o }")
    result.serialize("results.jellys", format="jelly")
    result = Result.parse(open("results.jellys", "rb"), format="jelly")
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import IO, Any
from typing_extensions import override

from rdflib.query import Result, ResultParser, ResultSerializer
from rdflib.term import Identifier, Variable

from pyjelly.errors import JellyConformanceError, JellyIncompleteResultsError
from pyjelly.integrations.rdflib.decode import RDFLibAdapter
from pyjelly.integrations.rdflib.encode import RDFLibTermEncoder
from pyjelly.options import SparqlOptions
from pyjelly.parse.decode import Adapter, ParserOptions
from pyjelly.parse.sparql import (
    AdapterFactory,
    SparqlResultSet,
    read_result_set,
    read_result_sets,
)
from pyjelly.serialize.sparql import FrameWriter, SparqlStream, write_solutions


class JellySparqlResult(Result):
    """
    SPARQL query results read from Jelly-SPARQL: an rdflib Result, with more.

    Attributes:
        links: IRIs of links that describe the results, like head.link of SPARQL
            JSON results.
        truncated: whether the stream ended without saying that the results are
            complete, so some may be missing.

    """

    def __init__(self, type_: str) -> None:
        super().__init__(type_)
        self.links: list[str] = []
        self.truncated = False


def _adapter_factory(*, validate_iris: bool) -> AdapterFactory:
    def make(options: ParserOptions) -> Adapter:
        return RDFLibAdapter(options, validate_iris=validate_iris)

    return make


def _to_result(result_set: SparqlResultSet) -> JellySparqlResult:
    """Read a result set whole, into an rdflib Result."""
    if result_set.boolean is not None:
        result = JellySparqlResult("ASK")
        result.askAnswer = result_set.boolean
    else:
        result = JellySparqlResult("SELECT")
        result.vars = [Variable(name) for name in result_set.variables]
    bindings: list[Mapping[Variable, Identifier]] = []
    result.bindings = bindings
    variables = result.vars or []
    try:
        # Unbound variables are left out, as rdflib does
        bindings.extend(
            {
                variable: value
                for variable, value in zip(variables, row, strict=True)
                if value is not None
            }
            for row in result_set
        )
    except JellyIncompleteResultsError as error:
        result.links = result_set.links
        error.result = result
        raise
    result.links = result_set.links
    result.truncated = result_set.truncated
    return result


def parse_jelly_sparql(
    inp: IO[bytes], *, validate_iris: bool = False
) -> JellySparqlResult:
    """
    Read SPARQL query results from Jelly-SPARQL.

    Args:
        inp (IO[bytes]): the stream, delimited or not. It must have one result set
            (the FLAT stream type, the default).
        validate_iris (bool): whether rdflib checks the IRIs. An invalid IRI is
            still read, rdflib only logs a warning. Checking slows down parsing.

    Returns:
        JellySparqlResult: the results, an rdflib Result.

    Raises:
        JellyIncompleteResultsError: if the producer said that the results are
            incomplete, e.g., because the query failed midway. The results read
            are in its `result`.

    """
    adapter_factory = _adapter_factory(validate_iris=validate_iris)
    return _to_result(read_result_set(inp, adapter_factory))


def parse_jelly_sparql_results(
    inp: IO[bytes], *, validate_iris: bool = False
) -> Iterator[JellySparqlResult]:
    """
    Read a sequence of SPARQL query results from Jelly-SPARQL.

    A stream of the PUNCTUATED type has any number of result sets, one of the FLAT
    type has one.

    Args:
        inp (IO[bytes]): the stream, delimited.
        validate_iris (bool): whether rdflib checks the IRIs, see
            parse_jelly_sparql().

    Yields:
        JellySparqlResult: the results, an rdflib Result for each result set.

    Raises:
        JellyIncompleteResultsError: if the producer said that a result set is
            incomplete. The results after it can still be read.

    """
    adapter_factory = _adapter_factory(validate_iris=validate_iris)
    for result_set in read_result_sets(inp, adapter_factory):
        yield _to_result(result_set)


def sparql_stream(options: SparqlOptions | None = None) -> SparqlStream:
    """Return a stream that writes Jelly-SPARQL with terms of rdflib."""
    if options is None:
        options = SparqlOptions()
    encoder = RDFLibTermEncoder(lookup_preset=options.lookup_preset)
    return SparqlStream(encoder, options)


def _write_result(stream: SparqlStream, result: Result, writer: FrameWriter) -> None:
    """Write one result set: the result of a SELECT or ASK query."""
    links = getattr(result, "links", ())
    if result.type == "ASK":
        writer.write(stream.ask(bool(result.askAnswer), links=links))
        return
    if result.type != "SELECT":
        msg = (
            f"Jelly-SPARQL is for the results of SELECT and ASK queries, not "
            f"{result.type}. Write graphs in Jelly-RDF instead."
        )
        raise JellyConformanceError(msg)
    variables = result.vars or []
    rows = (
        [binding.get(variable) for variable in variables]
        for binding in _bindings(result)
    )
    write_solutions(stream, variables, rows, writer, links)


def _bindings(result: Result) -> Iterator[Mapping[Variable, Identifier]]:
    """
    Return the bindings of a result, as they are made.

    Result.bindings would make all of them before any is written, and if making
    one fails, nothing would be written at all. They are still kept in the result,
    as Result.__iter__() does.
    """
    made = getattr(result, "_genbindings", None)
    if made is None:
        yield from result.bindings
        return
    result._genbindings = None
    kept = result._bindings
    for binding in made:
        kept.append(binding)
        yield binding


def serialize_jelly_sparql(
    result: Result, out: IO[bytes], options: SparqlOptions | None = None
) -> None:
    """
    Write SPARQL query results (of a SELECT or ASK query) in Jelly-SPARQL.

    If getting the results fails midway (e.g., the query fails), the stream ends
    with the error, so that readers know the results are incomplete, and the
    error is raised.

    Args:
        result (Result): the results.
        out (IO[bytes]): where to write them.
        options (SparqlOptions | None): options of the stream.

    """
    stream = sparql_stream(options)
    _write_result(stream, result, FrameWriter(out, delimited=stream.options.delimited))


def serialize_jelly_sparql_results(
    results: Iterable[Result], out: IO[bytes], options: SparqlOptions | None = None
) -> None:
    """
    Write a sequence of SPARQL query results in one Jelly-SPARQL stream.

    The stream is of the PUNCTUATED type: each result is a result set of its own.

    Args:
        results (Iterable[Result]): the results.
        out (IO[bytes]): where to write them.
        options (SparqlOptions | None): options of the stream, which must be
            punctuated. Defaults to SparqlOptions(punctuated=True).

    """
    if options is None:
        options = SparqlOptions(punctuated=True)
    if not options.punctuated:
        msg = "a sequence of results needs SparqlOptions(punctuated=True)"
        raise JellyConformanceError(msg)
    stream = sparql_stream(options)
    writer = FrameWriter(out, delimited=True)
    for result in results:
        _write_result(stream, result, writer)


def _input_stream(source: IO[bytes] | str | Path) -> IO[bytes]:
    if isinstance(source, (str, Path)):
        return Path(source).open("rb")
    return source


class RDFLibJellySparqlParser(ResultParser):
    """Reads SPARQL query results in Jelly-SPARQL, for rdflib's Result.parse()."""

    @override
    def parse(
        self,
        source: IO[bytes] | str | Path,
        content_type: str | None = None,
        *,
        validate_iris: bool = False,
        **unused: Any,
    ) -> Result:
        """
        Read the results.

        Args:
            source (IO[bytes] | str | Path): the stream, or the path of a file.
            content_type (str | None): unused.
            validate_iris (bool): whether rdflib checks the IRIs, see
                parse_jelly_sparql().
            **unused (Any): unused arguments of Result.parse().

        """
        inp = _input_stream(source)
        if inp is source:
            return parse_jelly_sparql(inp, validate_iris=validate_iris)
        with inp:
            return parse_jelly_sparql(inp, validate_iris=validate_iris)


class RDFLibJellySparqlSerializer(ResultSerializer):
    """Writes SPARQL query results in Jelly-SPARQL, for rdflib's Result.serialize()."""

    @override
    def serialize(
        self,
        stream: IO[bytes],
        encoding: str = "utf-8",
        *,
        options: SparqlOptions | None = None,
        **unused: Any,
    ) -> None:
        """
        Write the results.

        Args:
            stream (IO[bytes]): where to write them.
            encoding (str): unused, Jelly is binary.
            options (SparqlOptions | None): options of the stream.
            **unused (Any): unused arguments of Result.serialize().

        """
        serialize_jelly_sparql(self.result, stream, options)
