"""
Ways of writing and reading SPARQL query results, each through one library's API.

Each method writes the rows of a result set to a stream, and reads them back from
bytes in memory. The throughput counts cells: rows times variables.
"""

from __future__ import annotations

import io
from abc import ABC, abstractmethod
from collections import Counter
from typing import IO, Any
from typing_extensions import override

from rdflib import BNode, Literal, URIRef
from rdflib.query import Result

from benchmarks.sparql_data import SparqlBenchData
from pyjelly.integrations.generic import generic_sink
from pyjelly.integrations.generic import sparql as generic_sparql


class SparqlMethod(ABC):
    name: str

    def supports(self, data: SparqlBenchData) -> bool:  # noqa: ARG002
        return True

    @abstractmethod
    def rows(self, data: SparqlBenchData) -> list[tuple[Any, ...]]:
        """Return the rows in this method's term model, None if unbound."""

    def prepare(self, data: SparqlBenchData) -> None:
        """Make everything the writer starts from, so that it isn't measured."""
        self.rows(data)

    def write_count(self, data: SparqlBenchData) -> int:
        """Return how many cells `write` writes."""
        return data.cells

    @abstractmethod
    def write(self, data: SparqlBenchData, out: IO[bytes]) -> None:
        """Write the result set to `out`."""

    @abstractmethod
    def read(self, data: bytes, *, quads: bool = False) -> int:
        """
        Read the rows, do nothing with them, and return how many cells there were.

        `quads` is not used: it is there to read like the methods of RDF.
        """

    @abstractmethod
    def read_all(self, data: bytes) -> list[tuple[Any, ...]]:
        """Read the rows and return them, for checking."""

    def write_bytes(self, data: SparqlBenchData) -> bytes:
        out = io.BytesIO()
        self.write(data, out)
        return out.getvalue()


def _cells(rows: int, variables: int) -> int:
    return rows * max(1, variables)


class RdflibResults(SparqlMethod):
    """A result format of rdflib, written and read with Result.serialize and parse."""

    def __init__(self, name: str, rdflib_format: str) -> None:
        self.name = name
        self.format = rdflib_format

    @override
    def supports(self, data: SparqlBenchData) -> bool:
        return not data.rdf_12

    @override
    def rows(self, data: SparqlBenchData) -> list[tuple[Any, ...]]:
        result = data.rdflib
        variables = result.vars or []
        return [tuple(b.get(v) for v in variables) for b in result.bindings]

    @override
    def write(self, data: SparqlBenchData, out: IO[bytes]) -> None:
        data.rdflib.serialize(destination=out, format=self.format)

    def _parse(self, data: bytes) -> Result:
        return Result.parse(io.BytesIO(data), format=self.format)

    @override
    def read(self, data: bytes, *, quads: bool = False) -> int:
        result = self._parse(data)
        return _cells(len(result.bindings), len(result.vars or []))

    @override
    def read_all(self, data: bytes) -> list[tuple[Any, ...]]:
        result = self._parse(data)
        variables = result.vars or []
        return [tuple(b.get(v) for v in variables) for b in result.bindings]


class GenericJelly(SparqlMethod):
    """Jelly-SPARQL with pyjelly's generic API, with its own terms."""

    name = "generic-jelly"

    @override
    def rows(self, data: SparqlBenchData) -> list[tuple[Any, ...]]:
        return data.generic

    @override
    def write(self, data: SparqlBenchData, out: IO[bytes]) -> None:
        generic_sparql.serialize_jelly_sparql(data.variables, data.generic, out)

    @override
    def read(self, data: bytes, *, quads: bool = False) -> int:
        results = generic_sparql.parse_jelly_sparql(io.BytesIO(data))
        rows = 0
        for _ in results:
            rows += 1
        return _cells(rows, len(results.variables))

    @override
    def read_all(self, data: bytes) -> list[tuple[Any, ...]]:
        return list(generic_sparql.parse_jelly_sparql(io.BytesIO(data)))


SPARQL_METHODS: dict[str, SparqlMethod] = {
    method.name: method
    for method in (
        RdflibResults("rdflib-srj", "json"),
        RdflibResults("rdflib-srx", "xml"),
        RdflibResults("rdflib-csv", "csv"),
        # Jelly through rdflib's plugin API: Result.serialize and Result.parse
        RdflibResults("rdflib-jelly", "jelly"),
        GenericJelly(),
    )
}


def _normalize(term: Any) -> Any:
    """Make terms comparable. Blank node labels are not compared."""
    if term is None:
        return None
    if isinstance(term, (BNode, generic_sink.BlankNode)):
        return "_:"
    if isinstance(term, (URIRef, Literal)):
        return term.n3()
    return str(term)


def sparql_round_trip(
    method: SparqlMethod, data: SparqlBenchData
) -> tuple[bytes, list[str]]:
    """
    Write the result set with the method, read it back, and compare the rows.

    Returns the written bytes, and a list of problems found (empty if none).
    Raises the reader's exception if the method can't read its own output.
    """
    written = method.write_bytes(data)
    expected = [tuple(map(_normalize, row)) for row in method.rows(data)]
    actual = [tuple(map(_normalize, row)) for row in method.read_all(written)]
    problems = []
    if method.read(written) != method.write_count(data):
        problems.append("read() counts a different number of cells than written")
    missing = Counter(expected) - Counter(actual)
    extra = Counter(actual) - Counter(expected)
    if missing or extra:
        problems.append(
            f"{sum(missing.values())} rows missing, {sum(extra.values())} unexpected; "
            f"e.g. missing {next(iter(missing), None)}, "
            f"unexpected {next(iter(extra), None)}"
        )
    elif expected != actual:
        problems.append("the rows are in another order")
    return written, problems
