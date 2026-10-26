"""
Jelly-SPARQL for rdflib: SPARQL query results (rdflib.query.Result) in Jelly.

With pyjelly installed, rdflib reads and writes them in the "jelly" format:

    result = graph.query("SELECT ?s ?o WHERE { ?s ?p ?o }")
    result.serialize("results.jellys", format="jelly")
    result = Result.parse(open("results.jellys", "rb"), format="jelly")
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import IO, Any, cast
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

#: The error that a result read from a truncated stream is passed on with
TRUNCATED_ERROR = (
    "the results were read from a stream that ended without saying that they are "
    "complete, so some may be missing"
)


class JellySparqlResult(Result):
    """
    SPARQL query results read from Jelly-SPARQL: an rdflib Result, with more.

    Attributes:
        links: IRIs of links that describe the results, like head.link of SPARQL
            JSON results.
        truncated: whether the stream ended without saying that the results are
            complete, so some may be missing.
        error: why the results are incomplete, if the writer said so. Only set by
            parse_jelly_sparql_sequence(): parse_jelly_sparql() raises
            JellyIncompleteResultsError instead.

    """

    def __init__(self, type_: str) -> None:
        super().__init__(type_)
        self.links: list[str] = []
        self.truncated = False
        self.error = ""


def _adapter_factory(*, validate_iris: bool) -> AdapterFactory:
    def make(options: ParserOptions) -> Adapter:
        return RDFLibAdapter(options, validate_iris=validate_iris)

    return make


def _to_result(
    result_set: SparqlResultSet, *, lazy: bool, raise_error: bool
) -> JellySparqlResult:
    """Return a result set as an rdflib Result."""
    if result_set.is_boolean:
        result = JellySparqlResult("ASK")
        # The rest of the stream, which must not have more of the result
        result_set.keep()
        result.links = result_set.links
        try:
            result.askAnswer = result_set.boolean
        except JellyIncompleteResultsError as error:
            if raise_error:
                error.result = result
                raise
            # The value read, which may be wrong
            result.askAnswer = result_set._boolean
            result.error = error.error
        result.truncated = result_set.truncated
        return result
    result = JellySparqlResult("SELECT")
    result.vars = [Variable(name) for name in result_set.variables]
    result.links = result_set.links
    rows = _read_bindings(result_set, result, raise_error=raise_error)
    if lazy:
        # rdflib makes the bindings as the result is iterated, as for its own
        # query results
        result.bindings = rows
        return result
    bindings: list[Mapping[Variable, Identifier]] = []
    result.bindings = bindings
    # If the results are incomplete, the result has those read before the error
    bindings.extend(rows)
    return result


def _read_bindings(
    result_set: SparqlResultSet, result: JellySparqlResult, *, raise_error: bool
) -> Iterator[Mapping[Variable, Identifier]]:
    """Yield the bindings of the rows, and when they end, what the stream said."""
    variables = result.vars or []
    try:
        for row in result_set:
            # Unbound variables are left out, as rdflib does
            yield {
                variable: value
                for variable, value in zip(variables, row, strict=True)
                if value is not None
            }
    except JellyIncompleteResultsError as error:
        result.error = error.error
        if raise_error:
            error.result = result
            raise
    finally:
        result.links = result_set.links
        result.truncated = result_set.truncated


def parse_jelly_sparql(
    inp: IO[bytes] | bytes, *, validate_iris: bool = False, lazy: bool = False
) -> JellySparqlResult:
    """
    Read SPARQL query results from Jelly-SPARQL.

    Args:
        inp (IO[bytes] | bytes): the stream, delimited or not. It must have one
            result set (the FLAT stream type, the default).
        validate_iris (bool): whether rdflib checks the IRIs. An invalid IRI is
            still read, rdflib only logs a warning. Checking slows down parsing.
        lazy (bool): whether to read the rows as the result is iterated, rather
            than all of them now. The stream must then stay open until the result
            was iterated, and the error of incomplete results is raised then.

    Returns:
        JellySparqlResult: the results, an rdflib Result.

    Raises:
        JellyIncompleteResultsError: if the writer said that the results are
            incomplete, e.g., because the query failed midway. The results read
            are in its `result`.

    """
    adapter_factory = _adapter_factory(validate_iris=validate_iris)
    try:
        result_set = read_result_set(inp, adapter_factory)
    except JellyIncompleteResultsError as error:
        # A boolean result that is incomplete
        result_set = cast("SparqlResultSet", error.result)
        error.result = _to_result(result_set, lazy=False, raise_error=False)
        raise
    return _to_result(result_set, lazy=lazy, raise_error=True)


def parse_jelly_sparql_sequence(
    inp: IO[bytes] | bytes, *, validate_iris: bool = False
) -> Iterator[JellySparqlResult]:
    """
    Read a sequence of SPARQL query results from one Jelly-SPARQL stream.

    The stream has the PUNCTUATED type, which holds any number of results (a
    stream of the FLAT type, the default, has one).

    An incomplete result does not end the sequence: it has the reason in its
    `error` attribute, so check it, and the results after it are read too.

    Args:
        inp (IO[bytes] | bytes): the stream, delimited.
        validate_iris (bool): whether rdflib checks the IRIs, see
            parse_jelly_sparql().

    Yields:
        JellySparqlResult: the results, an rdflib Result for each result set.

    """
    adapter_factory = _adapter_factory(validate_iris=validate_iris)
    for result_set in read_result_sets(inp, adapter_factory):
        yield _to_result(result_set, lazy=False, raise_error=False)


def sparql_stream(options: SparqlOptions | None = None) -> SparqlStream:
    """Return a stream that writes Jelly-SPARQL with terms of rdflib."""
    if options is None:
        options = SparqlOptions()
    encoder = RDFLibTermEncoder(lookup_preset=options.lookup_preset)
    return SparqlStream(encoder, options)


def _passed_on_error(result: Result) -> str:
    """Return the error of a result that was read from Jelly-SPARQL, if any."""
    error = getattr(result, "error", "")
    if not error and getattr(result, "truncated", False):
        return TRUNCATED_ERROR
    return str(error)


def _write_result(stream: SparqlStream, result: Result, writer: FrameWriter) -> None:
    """Write one result set: the result of a SELECT or ASK query."""
    links = getattr(result, "links", ())
    if result.type == "ASK":
        if result.askAnswer is None:
            msg = "the result of the ASK query has no answer (askAnswer is None)"
            raise JellyConformanceError(msg)
        frame = stream.ask(
            bool(result.askAnswer), links=links, error=_passed_on_error(result)
        )
        writer.write(frame)
        return
    if result.type != "SELECT":
        msg = (
            f"Jelly-SPARQL is for the results of SELECT and ASK queries, not "
            f"{result.type}. Write graphs in Jelly-RDF instead."
        )
        raise JellyConformanceError(msg)
    if result.vars is None:
        msg = "the result of the SELECT query has no variables (vars is None)"
        raise JellyConformanceError(msg)
    variables = result.vars
    rows = (_row(binding, variables) for binding in _made_bindings(result))
    write_solutions(
        stream, variables, rows, writer, links=links, error=_passed_on_error(result)
    )


def _row(
    binding: Mapping[Any, Identifier], variables: Sequence[Variable]
) -> list[Identifier | None]:
    """Return the values of the variables in a binding, None if unbound."""
    row = [binding.get(variable) for variable in variables]
    if len(binding) + row.count(None) != len(variables):
        # Not all of the binding's keys are variables: rdflib's Variable("x") is
        # not equal to "x". Look the values up by name instead.
        by_name = {str(key): value for key, value in binding.items()}
        row = [by_name.get(str(variable)) for variable in variables]
    return row


def _made_bindings(result: Result) -> Iterator[Mapping[Variable, Identifier]]:
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
    # Those made before, e.g. by an iteration of the result that stopped early
    yield from list(kept)
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
    error is raised. Results read from Jelly-SPARQL that are incomplete or
    truncated are written as incomplete.

    Args:
        result (Result): the results.
        out (IO[bytes]): where to write them.
        options (SparqlOptions | None): options of the stream.

    """
    stream = sparql_stream(options)
    _write_result(stream, result, FrameWriter(out, delimited=stream.options.delimited))


def serialize_jelly_sparql_sequence(
    results: Iterable[Result], out: IO[bytes], options: SparqlOptions | None = None
) -> None:
    """
    Write a sequence of SPARQL query results in one Jelly-SPARQL stream.

    The stream has the PUNCTUATED type: each result is a result set of its own.

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
        raise ValueError(msg)
    stream = sparql_stream(options)
    writer = FrameWriter(out, delimited=True)
    for result in results:
        _write_result(stream, result, writer)


class RDFLibJellySparqlParser(ResultParser):
    """Reads SPARQL query results in Jelly-SPARQL, for rdflib's Result.parse()."""

    @override
    def parse(  # type: ignore[override]
        self,
        source: IO[bytes] | bytes | str | Path,
        content_type: str | None = None,
        *,
        validate_iris: bool = False,
        lazy: bool = False,
    ) -> Result:
        """
        Read the results.

        Args:
            source (IO[bytes] | bytes | str | Path): the stream, its bytes, or the
                path of a file.
            content_type (str | None): unused.
            validate_iris (bool): whether rdflib checks the IRIs, see
                parse_jelly_sparql().
            lazy (bool): whether to read the rows as the result is iterated, see
                parse_jelly_sparql(). Only for streams, not paths.

        """
        if not isinstance(source, (str, Path)):
            return parse_jelly_sparql(source, validate_iris=validate_iris, lazy=lazy)
        if lazy:
            msg = "lazy reading needs a stream that stays open, not a path"
            raise ValueError(msg)
        with Path(source).open("rb") as inp:
            return parse_jelly_sparql(inp, validate_iris=validate_iris)


class RDFLibJellySparqlSerializer(ResultSerializer):
    """Writes SPARQL query results in Jelly-SPARQL, for rdflib's Result.serialize()."""

    @override
    def serialize(  # type: ignore[override]
        self,
        stream: IO[bytes],
        encoding: str = "utf-8",
        *,
        options: SparqlOptions | None = None,
    ) -> None:
        """
        Write the results.

        Args:
            stream (IO[bytes]): where to write them.
            encoding (str): unused, Jelly is binary.
            options (SparqlOptions | None): options of the stream.

        """
        serialize_jelly_sparql(self.result, stream, options)
