"""
Ways of writing and reading RDF statements, each through one library's own API.

Each method writes a list of statements to a stream, and reads statements back
from bytes in memory. A method that has separate formats for triples and quads
(e.g., N-Triples and N-Quads) uses the one that fits the dataset.
"""

from __future__ import annotations

import io
from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Iterable, Sequence
from typing import IO, Any
from typing_extensions import Buffer, override

from rdflib import BNode, Dataset, Graph, Literal, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.plugins.serializers.nquads import _nq_row
from rdflib.plugins.serializers.nt import _nt_row
from rdflib.store import Store
from rdflib.term import Node

from benchmarks.data import BenchData
from pyjelly.integrations.generic import generic_sink
from pyjelly.integrations.generic import parse as generic_parse
from pyjelly.integrations.generic import serialize as generic_serialize
from pyjelly.integrations.rdflib import parse as rdflib_parse
from pyjelly.integrations.rdflib import serialize as rdflib_serialize


class NullOutput(io.BytesIO):
    """
    An output stream that throws away everything written to it.

    The Python counterpart of Java's `OutputStream.nullOutputStream()`. It's a
    BytesIO only so that it has the type of a binary stream: nothing is kept.
    """

    @override
    def write(self, b: Buffer, /) -> int:
        return memoryview(b).nbytes


class SinkStore(Store):
    """
    An rdflib store that only counts the statements that a parser adds to it.

    rdflib's parsers have no API that just gives statements back: they always add
    them to a Graph or Dataset. Using a store that does nothing keeps the cost of
    indexing statements in memory out of the measurement. The only extra cost left
    is the type checks in `Graph.add`.

    With `keep=True`, the statements are also kept, for checking them.
    """

    context_aware = True
    graph_aware = True

    def __init__(self, *, keep: bool = False) -> None:
        super().__init__()
        self.count = 0
        self.kept: list[tuple[Node, ...]] | None = [] if keep else None

    @override
    def add(
        self,
        triple: tuple[Node, Node, Node],
        context: Graph | None,
        quoted: bool = False,
    ) -> None:
        self.count += 1
        if self.kept is not None:
            graph = context.identifier if context is not None else None
            self.kept.append((*triple, graph) if graph is not None else triple)

    @override
    def addN(self, quads: Iterable[tuple[Node, Node, Node, Graph]]) -> None:
        for s, p, o, context in quads:
            self.add((s, p, o), context)

    # Called by Graph and Dataset when they are created. There's nothing to do.

    @override
    def add_graph(self, graph: Graph) -> None:
        pass

    @override
    def remove_graph(self, graph: Graph) -> None:
        pass

    @override
    def bind(self, prefix: str, namespace: URIRef, override: bool = True) -> None:
        pass


def _parse_with_rdflib(
    data: bytes, rdflib_format: str, *, quads: bool, keep: bool
) -> SinkStore:
    store = SinkStore(keep=keep)
    sink = Dataset(store=store) if quads else Graph(store=store, bind_namespaces="none")
    sink.parse(data=data, format=rdflib_format)
    return store


class Method(ABC):
    name: str
    #: Whether the writer first collects all statements into a graph, which drops
    #: duplicates and changes the order. Such methods are checked with sets.
    buffers: bool = False

    def supports(self, data: BenchData) -> bool:  # noqa: ARG002
        return True

    @abstractmethod
    def statements(self, data: BenchData) -> list[Any]:
        """Return the dataset's statements in this method's term model."""

    @abstractmethod
    def write(self, data: BenchData, out: IO[bytes]) -> None:
        """Write all statements of the dataset to `out`."""

    @abstractmethod
    def read(self, data: bytes, *, quads: bool) -> int:
        """Read the statements, do nothing with them, and return how many there were."""

    @abstractmethod
    def read_all(self, data: bytes, *, quads: bool) -> list[tuple[Any, ...]]:
        """Read the statements and return them, for checking."""

    def write_bytes(self, data: BenchData) -> bytes:
        out = io.BytesIO()
        self.write(data, out)
        return out.getvalue()


class RdflibLines(Method):
    """
    N-Triples / N-Quads in rdflib.

    rdflib's serializers only write whole graphs, but these two formats need no
    graph: the serializer just writes one line per statement. The `write` method
    runs the same loop as rdflib's NTSerializer and NQuadsSerializer, with the same
    helper functions, but over the list of statements. So nothing is deduplicated
    or reordered, same as in the streaming writers of Jena and RDF4J.
    """

    name = "rdflib-nt"

    @override
    def statements(self, data: BenchData) -> list[Any]:
        return data.rdflib

    @override
    def write(self, data: BenchData, out: IO[bytes]) -> None:
        if data.quads:
            for s, p, o, g in data.rdflib:
                # _nq_row has no type hints
                row = _nq_row((s, p, o), g)  # type: ignore[no-untyped-call]
                out.write(row.encode("utf-8", "replace"))
        else:
            for triple in data.rdflib:
                out.write(_nt_row(triple).encode())

    @override
    def read(self, data: bytes, *, quads: bool) -> int:
        return _parse_with_rdflib(
            data, _nq_or_nt(quads=quads), quads=quads, keep=False
        ).count

    @override
    def read_all(self, data: bytes, *, quads: bool) -> list[tuple[Any, ...]]:
        store = _parse_with_rdflib(data, _nq_or_nt(quads=quads), quads=quads, keep=True)
        assert store.kept is not None
        return store.kept


def _nq_or_nt(*, quads: bool) -> str:
    return "nquads" if quads else "nt"


class RdflibGraph(Method):
    """
    A format that rdflib only writes from a whole Graph or Dataset.

    The graph is built inside the measurement, as in the Jelly-JVM benchmarks:
    a user who has a stream of statements has to do the same. The graph gets the
    dataset's prefixes, if the format can use them.
    """

    buffers = True

    def __init__(
        self,
        name: str,
        triples_format: str,
        quads_format: str | None,
        *,
        prefixes: bool = True,
    ) -> None:
        self.name = name
        self.triples_format = triples_format
        self.quads_format = quads_format
        self.prefixes = prefixes

    @override
    def supports(self, data: BenchData) -> bool:
        return not data.quads or self.quads_format is not None

    def _format(self, *, quads: bool) -> str:
        if quads:
            assert self.quads_format is not None
            return self.quads_format
        return self.triples_format

    @override
    def statements(self, data: BenchData) -> list[Any]:
        return data.rdflib

    @override
    def write(self, data: BenchData, out: IO[bytes]) -> None:
        graph: Graph
        if data.quads:
            graph = dataset = Dataset()
            # One Graph object for each graph name, as a user would keep them
            contexts: dict[Node, Graph] = {}
            for s, p, o, g in data.rdflib:
                context = contexts.get(g)
                if context is None:
                    context = contexts[g] = dataset.graph(g)
                context.add((s, p, o))
        else:
            graph = Graph(bind_namespaces="core")
            for triple in data.rdflib:
                graph.add(triple)
        if self.prefixes:
            for prefix, namespace in data.prefixes:
                graph.bind(prefix, namespace)
        graph.serialize(destination=out, format=self._format(quads=data.quads))

    @override
    def read(self, data: bytes, *, quads: bool) -> int:
        return _parse_with_rdflib(
            data, self._format(quads=quads), quads=quads, keep=False
        ).count

    @override
    def read_all(self, data: bytes, *, quads: bool) -> list[tuple[Any, ...]]:
        store = _parse_with_rdflib(
            data, self._format(quads=quads), quads=quads, keep=True
        )
        assert store.kept is not None
        return store.kept


class PyjellyFlat(Method):
    """
    Jelly with pyjelly's flat streaming API, using pyjelly's default options.

    `integration` is "rdflib" for statements made of rdflib terms, or "generic"
    for pyjelly's own terms.
    """

    def __init__(self, integration: str) -> None:
        self.name = f"pyjelly-{integration}"
        self.generic = integration == "generic"

    @override
    def statements(self, data: BenchData) -> list[Any]:
        return data.generic if self.generic else data.rdflib

    @override
    def write(self, data: BenchData, out: IO[bytes]) -> None:
        # The API takes a generator, so the list is wrapped in one
        statements = (statement for statement in self.statements(data))
        if self.generic:
            generic_serialize.flat_stream_to_file(statements, out)
        else:
            rdflib_serialize.flat_stream_to_file(statements, out)

    def _parse(self, data: bytes) -> Iterable[Any]:
        if self.generic:
            return generic_parse.parse_jelly_flat(io.BytesIO(data))
        return rdflib_parse.parse_jelly_flat(io.BytesIO(data))

    @override
    def read(self, data: bytes, *, quads: bool) -> int:
        # The stream has no prefix declarations (the writer is given none), so
        # every event is a statement. The round-trip check makes sure of that.
        count = 0
        for _ in self._parse(data):
            count += 1
        return count

    @override
    def read_all(self, data: bytes, *, quads: bool) -> list[tuple[Any, ...]]:
        return list(self._parse(data))


METHODS: dict[str, Method] = {
    method.name: method
    for method in (
        RdflibLines(),
        RdflibGraph("rdflib-turtle", "turtle", "trig"),
        RdflibGraph("rdflib-xml", "xml", None),
        RdflibGraph("rdflib-jsonld", "json-ld", "json-ld"),
        # Jelly through rdflib's plugin API: Graph.serialize and Graph.parse
        RdflibGraph("rdflib-jelly", "jelly", "jelly", prefixes=False),
        PyjellyFlat("rdflib"),
        PyjellyFlat("generic"),
    )
}


def _normalize(term: Any) -> Any:
    """
    Make terms from different readers comparable.

    Blank node labels are not compared, because most rdflib parsers make new
    ones. The default graph is None.
    """
    if isinstance(term, (BNode, generic_sink.BlankNode)):
        return "_:"
    if term is None or term is generic_sink.DefaultGraph:
        return None
    if term == DATASET_DEFAULT_GRAPH_ID:
        return None
    if isinstance(term, (URIRef, Literal)):
        return term.n3()
    return repr(term)


def _normalized(
    statements: Iterable[Sequence[Any]], *, quads: bool
) -> list[tuple[Any, ...]]:
    length = 4 if quads else 3
    result = []
    for statement in statements:
        terms = tuple(_normalize(term) for term in statement)
        # Triples read by rdflib carry the graph's name, which isn't compared
        result.append(terms[:length] if len(terms) > length else terms)
    return result


def round_trip(method: Method, data: BenchData) -> tuple[bytes, list[str]]:
    """
    Write the dataset with the method, read it back, and compare.

    Returns the written bytes, and a list of problems found (empty if none).
    Raises the reader's exception if the method can't read its own output.
    """
    written = method.write_bytes(data)
    expected = _normalized(method.statements(data), quads=data.quads)
    actual = _normalized(method.read_all(written, quads=data.quads), quads=data.quads)

    problems = []
    if method.read(written, quads=data.quads) != len(actual):
        problems.append(
            "read() counts a different number of statements than read_all()"
        )
    if method.buffers:
        missing, extra = set(expected) - set(actual), set(actual) - set(expected)
    else:
        missing_c, extra_c = (
            Counter(expected) - Counter(actual),
            Counter(actual) - Counter(expected),
        )
        missing, extra = set(missing_c), set(extra_c)
    if missing or extra:
        problems.append(
            f"{len(missing)} statements missing, {len(extra)} unexpected; "
            f"e.g. missing {next(iter(missing), None)}, "
            f"unexpected {next(iter(extra), None)}"
        )
    return written, problems
