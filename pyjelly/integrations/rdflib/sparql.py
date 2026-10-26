"""
Jelly-SPARQL support for rdflib's SPARQL query results (rdflib.query.Result).

With pyjelly installed, rdflib reads and writes results in the "jelly" format:

    result = graph.query("SELECT ?s ?o WHERE { ?s ?p ?o }")
    result.serialize("results.jellys", format="jelly")
    result = Result.parse(open("results.jellys", "rb"), format="jelly")
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import IO, cast
from typing_extensions import override

from rdflib.query import Result, ResultParser, ResultSerializer
from rdflib.term import Identifier, Variable

from pyjelly.errors import JellyConformanceError, JellyIncompleteResultsError
from pyjelly.integrations.rdflib.decode import BindingRows, RDFLibAdapter
from pyjelly.integrations.rdflib.encode import RDFLibTermEncoder
from pyjelly.options import SparqlOptions
from pyjelly.parse.decode import Adapter, ParserOptions
from pyjelly.parse.sparql import (
    AdapterFactory,
    SparqlResultSet,
    read_result_set,
    read_result_sets,
)
from pyjelly.serialize.sparql import FrameWriter, SparqlStream, write_bindings

#: Trailer error written for results that were read from a truncated stream
TRUNCATED_ERROR = (
    "the results were read from a stream that ended without indicating that they are "
    "complete, so some rows may be missing"
)


class JellySparqlResult(Result):
    """
    An rdflib Result read from Jelly-SPARQL, with a few extra attributes.

    Attributes:
        links: link IRIs for the results, as in head.link of SPARQL JSON results.
        truncated: whether the stream ended without a trailer, so some results may
            be missing.
        error: error message from the writer, if the results are incomplete. Only
            set by parse_jelly_sparql_sequence(). parse_jelly_sparql() raises
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
    result_set: SparqlResultSet,
    binding_rows: BindingRows,
    *,
    lazy: bool,
    raise_error: bool,
) -> JellySparqlResult:
    """Convert a result set with bindings as rows to an rdflib Result."""
    if result_set.is_boolean:
        result = JellySparqlResult("ASK")
        # Read to the end of the stream: nothing may follow a boolean result
        result_set.keep()
        result.links = result_set.links
        try:
            result.askAnswer = result_set.boolean
        except JellyIncompleteResultsError as error:
            if raise_error:
                error.result = result
                raise
            # Keep the value, even though the result is incomplete
            result.askAnswer = result_set._boolean
            result.error = error.error
        result.truncated = result_set.truncated
        return result
    result = JellySparqlResult("SELECT")
    # The same Variable objects as the keys of the bindings
    result.vars = binding_rows.variables(result_set.variables)
    result.links = result_set.links
    if lazy:
        # rdflib builds the bindings during iteration, as for its own query
        # results
        result.bindings = _read_bindings(result_set, result, raise_error=raise_error)
        return result
    bindings: list[Mapping[Variable, Identifier]] = []
    result.bindings = bindings
    try:
        result_set.collect(bindings)
    except JellyIncompleteResultsError as error:
        # The result has the rows read before the error
        _incomplete(result, error, raise_error=raise_error)
    finally:
        result.links = result_set.links
        result.truncated = result_set.truncated
    return result


def _incomplete(
    result: JellySparqlResult, error: JellyIncompleteResultsError, *, raise_error: bool
) -> None:
    result.error = error.error
    if raise_error:
        error.result = result
        raise error


def _read_bindings(
    result_set: SparqlResultSet, result: JellySparqlResult, *, raise_error: bool
) -> Iterator[Mapping[Variable, Identifier]]:
    """Yield the bindings, then record the links, truncation and error."""
    try:
        yield from result_set
    except JellyIncompleteResultsError as error:
        _incomplete(result, error, raise_error=raise_error)
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
        lazy (bool): read the rows during iteration instead of all at once. The
            stream must stay open until the result has been iterated, and the
            error of incomplete results is raised at the end of the iteration.

    Returns:
        JellySparqlResult: the results, an rdflib Result.

    Raises:
        JellyIncompleteResultsError: if the writer said that the results are
            incomplete, e.g., because the query failed midway. Its `result` holds
            the results read before the error.

    """
    adapter_factory = _adapter_factory(validate_iris=validate_iris)
    binding_rows = BindingRows()
    try:
        result_set = read_result_set(inp, adapter_factory, row_factory=binding_rows)
    except JellyIncompleteResultsError as error:
        # An incomplete boolean result
        result_set = cast("SparqlResultSet", error.result)
        error.result = _to_result(
            result_set, binding_rows, lazy=False, raise_error=False
        )
        raise
    return _to_result(result_set, binding_rows, lazy=lazy, raise_error=True)


def parse_jelly_sparql_sequence(
    inp: IO[bytes] | bytes, *, validate_iris: bool = False
) -> Iterator[JellySparqlResult]:
    """
    Read a sequence of SPARQL query results from one Jelly-SPARQL stream.

    This reads PUNCTUATED streams, which hold any number of results. A FLAT
    stream (the default) holds one.

    An incomplete result does not stop the sequence. Instead, its `error`
    attribute is set, so check it. The results after it are still read.

    Args:
        inp (IO[bytes] | bytes): the stream, delimited.
        validate_iris (bool): whether rdflib checks the IRIs, see
            parse_jelly_sparql().

    Yields:
        JellySparqlResult: the results, an rdflib Result for each result set.

    """
    adapter_factory = _adapter_factory(validate_iris=validate_iris)
    binding_rows = BindingRows()
    for result_set in read_result_sets(inp, adapter_factory, row_factory=binding_rows):
        yield _to_result(result_set, binding_rows, lazy=False, raise_error=False)


def sparql_stream(options: SparqlOptions | None = None) -> SparqlStream:
    """Create a Jelly-SPARQL writer for rdflib terms."""
    if options is None:
        options = SparqlOptions()
    encoder = RDFLibTermEncoder(lookup_preset=options.lookup_preset)
    return SparqlStream(encoder, options)


def _passed_on_error(result: Result) -> str:
    """Get the error to pass on for a result read from Jelly-SPARQL, if any."""
    error = getattr(result, "error", "")
    if not error and getattr(result, "truncated", False):
        return TRUNCATED_ERROR
    return str(error)


def _write_result(stream: SparqlStream, result: Result, writer: FrameWriter) -> None:
    """Write the result of a SELECT or ASK query as one result set."""
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
    # Check for the generator first, as reading result.bindings would build all
    # bindings at once
    made = getattr(result, "_genbindings", None)
    bindings: Iterable[Mapping[Variable, Identifier]] = (
        result.bindings if made is None else _made_bindings(result, made)
    )
    write_bindings(
        stream,
        result.vars,
        bindings,
        writer,
        links=links,
        error=_passed_on_error(result),
    )


def _made_bindings(
    result: Result, made: Iterator[Mapping[Variable, Identifier]]
) -> Iterator[Mapping[Variable, Identifier]]:
    """
    Yield the bindings of a result as its generator produces them.

    Result.bindings would build all of them before anything is written, and if one
    failed, nothing would be written at all. As in Result.__iter__(), the bindings
    are also kept in the result.
    """
    result._genbindings = None
    kept = result._bindings
    # Bindings already built, e.g. by an earlier loop over the result that stopped
    yield from list(kept)
    for binding in made:
        kept.append(binding)
        yield binding


def serialize_jelly_sparql(
    result: Result, out: IO[bytes], options: SparqlOptions | None = None
) -> None:
    """
    Write SPARQL query results (of a SELECT or ASK query) in Jelly-SPARQL.

    If the results fail midway (e.g., the query fails), the stream is ended with
    the error so readers know the results are incomplete, and the exception is
    re-raised. Results read from Jelly-SPARQL that were incomplete or truncated
    are written as incomplete.

    Args:
        result (Result): the results.
        out (IO[bytes]): output stream.
        options (SparqlOptions | None): options of the stream.

    """
    stream = sparql_stream(options)
    _write_result(stream, result, FrameWriter(out, delimited=stream.options.delimited))


def serialize_jelly_sparql_sequence(
    results: Iterable[Result], out: IO[bytes], options: SparqlOptions | None = None
) -> None:
    """
    Write a sequence of SPARQL query results in one Jelly-SPARQL stream.

    The stream is PUNCTUATED, with one result set per result.

    Args:
        results (Iterable[Result]): the results.
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
        _write_result(stream, result, writer)


class RDFLibJellySparqlParser(ResultParser):
    """rdflib parser plugin for Jelly-SPARQL results, used by Result.parse()."""

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
            source (IO[bytes] | bytes | str | Path): a stream, bytes, or a file
                path.
            content_type (str | None): unused.
            validate_iris (bool): whether rdflib checks the IRIs, see
                parse_jelly_sparql().
            lazy (bool): read the rows during iteration, see parse_jelly_sparql().
                Only for streams, not paths.

        """
        if not isinstance(source, (str, Path)):
            return parse_jelly_sparql(source, validate_iris=validate_iris, lazy=lazy)
        if lazy:
            msg = "lazy reading needs a stream that stays open, not a path"
            raise ValueError(msg)
        with Path(source).open("rb") as inp:
            return parse_jelly_sparql(inp, validate_iris=validate_iris)


class RDFLibJellySparqlSerializer(ResultSerializer):
    """rdflib serializer plugin for Jelly-SPARQL, used by Result.serialize()."""

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
            stream (IO[bytes]): output stream.
            encoding (str): unused, as Jelly is binary.
            options (SparqlOptions | None): options of the stream.

        """
        serialize_jelly_sparql(self.result, stream, options)
