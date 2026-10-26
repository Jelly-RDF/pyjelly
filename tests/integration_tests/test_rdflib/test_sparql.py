"""Jelly-SPARQL with rdflib: query results written and read through the plugins."""

from __future__ import annotations

import io
import socket
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest
from rdflib import BNode, Graph, Literal, URIRef, Variable
from rdflib.plugins.stores.sparqlstore import SPARQLStore
from rdflib.query import Result, ResultRow

from pyjelly import jelly
from pyjelly.errors import (
    JellyConformanceError,
    JellyIncompleteResultsError,
    JellyTruncatedResultsWarning,
)
from pyjelly.integrations.rdflib.sparql import (
    JellySparqlResult,
    parse_jelly_sparql,
    parse_jelly_sparql_sequence,
    serialize_jelly_sparql,
    serialize_jelly_sparql_sequence,
    sparql_stream,
)
from pyjelly.options import LookupPreset, SparqlOptions
from pyjelly.parse.sparql import sparql_frames
from pyjelly.serialize.sparql import FrameWriter

EX = "http://example.org/"

DATA = f"""
@prefix ex: <{EX}> .
ex:a ex:name "Alice" ; ex:age 42 ; ex:knows ex:b, _:c .
ex:b ex:name "Bob"@en .
_:c ex:name "Carol" .
"""

SELECT = """
SELECT ?person ?name ?age WHERE {
    ?person <http://example.org/name> ?name .
    OPTIONAL { ?person <http://example.org/age> ?age }
}
ORDER BY ?name
"""


@pytest.fixture
def graph() -> Graph:
    return Graph().parse(data=DATA, format="turtle")


def _round_trip(result: Result, **kwargs: Any) -> JellySparqlResult:
    data = result.serialize(format="jelly", **kwargs)
    assert data is not None
    parsed = Result.parse(io.BytesIO(data), format="jelly")
    assert isinstance(parsed, JellySparqlResult)
    return parsed


def test_select_round_trip(graph: Graph) -> None:
    result = graph.query(SELECT)
    parsed = _round_trip(result)
    assert parsed.type == "SELECT"
    assert parsed.vars == [Variable("person"), Variable("name"), Variable("age")]
    assert parsed == result
    assert not parsed.truncated
    rows = {row.name: row for row in parsed if isinstance(row, ResultRow)}
    assert rows[Literal("Alice")].age == Literal(42)
    assert rows[Literal("Bob", lang="en")].person == URIRef(EX + "b")
    assert isinstance(rows[Literal("Carol")].person, BNode)
    # Unbound variables are left out of the bindings, as in rdflib
    assert sum(Variable("age") in binding for binding in parsed.bindings) == 1


@pytest.mark.parametrize("query", ["ASK { ?s ?p 42 }", "ASK { ?s ?p 43 }"])
def test_ask_round_trip(graph: Graph, query: str) -> None:
    result = graph.query(query)
    parsed = _round_trip(result)
    assert parsed.type == "ASK"
    assert parsed.askAnswer is result.askAnswer
    assert bool(parsed) is bool(result)


def test_media_type(graph: Graph) -> None:
    result = graph.query(SELECT)
    data = result.serialize(format="application/x-jelly-sparql")
    assert data is not None
    parsed = Result.parse(
        io.BytesIO(data), content_type="application/x-jelly-sparql; charset=utf-8"
    )
    assert parsed == result


def test_files(graph: Graph, tmp_path: Path) -> None:
    result = graph.query(SELECT)
    path = tmp_path / "results.jellys"
    result.serialize(str(path), format="jelly")
    # Paths work too, but rdflib's types say only streams do
    assert Result.parse(str(path), format="jelly") == result  # type: ignore[arg-type]
    assert Result.parse(path, format="jelly") == result  # type: ignore[arg-type]
    with path.open("rb") as file:
        assert parse_jelly_sparql(file) == result


def test_empty_and_zero_variable_results(graph: Graph) -> None:
    empty = graph.query("SELECT ?s WHERE { ?s ?p 'nothing' }")
    parsed = _round_trip(empty)
    assert parsed.vars == [Variable("s")]
    assert len(parsed) == 0
    # One solution that binds no variables
    one = graph.query("SELECT * WHERE {}")
    parsed = _round_trip(one)
    assert parsed.vars == []
    assert parsed.bindings == [{}]


def test_options(graph: Graph) -> None:
    options = SparqlOptions(
        lookup_preset=LookupPreset(max_names=128, max_prefixes=0, max_datatypes=8),
        frame_values=2,
        stream_name="people",
    )
    result = graph.query(SELECT)
    data = result.serialize(format="jelly", options=options)
    assert data is not None
    frames = list(sparql_frames(io.BytesIO(data)))
    # 3 rows of 3 variables in frames of at most 2 values: one row per frame
    assert [frame.row_count for frame in frames] == [1, 1, 1, 0]
    assert frames[0].options.stream_name == "people"
    assert frames[0].options.max_prefix_table_size == 0
    assert Result.parse(io.BytesIO(data), format="jelly") == result


def test_non_delimited(graph: Graph) -> None:
    result = graph.query(SELECT)
    data = result.serialize(format="jelly", options=SparqlOptions(delimited=False))
    assert data is not None
    (frame,) = sparql_frames(io.BytesIO(data))
    assert frame.row_count == len(result)
    assert frame.HasField("trailer")
    assert Result.parse(io.BytesIO(data), format="jelly") == result


def test_links() -> None:
    result = JellySparqlResult("SELECT")
    result.vars = [Variable("x")]
    result.bindings = [{Variable("x"): URIRef(EX + "a")}]
    result.links = [EX + "about"]
    parsed = _round_trip(result)
    assert parsed.links == [EX + "about"]


def _failing_bindings() -> Iterator[dict[Variable, Any]]:
    yield {Variable("x"): URIRef(EX + "a")}
    msg = "the endpoint went away"
    raise RuntimeError(msg)


def test_error_midway_ends_the_stream_with_it() -> None:
    result = Result("SELECT")
    result.vars = [Variable("x")]
    result.bindings = _failing_bindings()
    out = io.BytesIO()
    with pytest.raises(RuntimeError, match="went away"):
        serialize_jelly_sparql(result, out)
    out.seek(0)
    with pytest.raises(JellyIncompleteResultsError, match="went away") as info:
        Result.parse(out, format="jelly")
    assert info.value.error == "RuntimeError: the endpoint went away"
    partial = info.value.result
    assert isinstance(partial, JellySparqlResult)
    assert partial.bindings == [{Variable("x"): URIRef(EX + "a")}]


def test_truncated(graph: Graph) -> None:
    result = graph.query(SELECT)
    data = result.serialize(format="jelly", options=SparqlOptions(frame_values=3))
    assert data is not None
    # The stream without its last frame, the one with the trailer
    frames = list(sparql_frames(io.BytesIO(data)))
    cut = data[: -(frames[-1].ByteSize() + 1)]
    parsed = Result.parse(io.BytesIO(cut), format="jelly")
    assert parsed.truncated
    assert len(parsed) == len(result)


def test_sequence_of_results(graph: Graph) -> None:
    results = [graph.query(SELECT), graph.query("ASK { ?s ?p ?o }")]
    out = io.BytesIO()
    serialize_jelly_sparql_sequence(results, out)
    out.seek(0)
    parsed = list(parse_jelly_sparql_sequence(out))
    assert parsed == results
    out.seek(0)
    with pytest.raises(JellyConformanceError, match="PUNCTUATED"):
        parse_jelly_sparql(out)


def test_sequence_needs_punctuated_options(graph: Graph) -> None:
    with pytest.raises(ValueError, match="punctuated"):
        serialize_jelly_sparql_sequence(
            [graph.query(SELECT)], io.BytesIO(), SparqlOptions()
        )


def test_graph_results_are_not_jelly_sparql(graph: Graph) -> None:
    result = graph.query("CONSTRUCT WHERE { ?s ?p ?o }")
    with pytest.raises(JellyConformanceError, match="CONSTRUCT"):
        serialize_jelly_sparql(result, io.BytesIO())


def test_rdf_12_terms_are_not_supported() -> None:
    from pyjelly.integrations.generic.generic_sink import IRI, Triple  # noqa: PLC0415
    from pyjelly.integrations.generic.sparql import (  # noqa: PLC0415
        serialize_jelly_sparql as generic_serialize,
    )

    out = io.BytesIO()
    iri = IRI(EX + "a")
    generic_serialize(["t"], [(Triple(iri, iri, iri),)], out)
    out.seek(0)
    with pytest.raises(NotImplementedError, match="triple terms"):
        Result.parse(out, format="jelly")


class _Endpoint(BaseHTTPRequestHandler):
    """A SPARQL endpoint that answers every query with the same Jelly-SPARQL."""

    body = b""
    accept = ""

    def do_GET(self) -> None:
        type(self).accept = self.headers["Accept"]
        self.send_response(200)
        self.send_header("Content-Type", "application/x-jelly-sparql")
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, *args: object) -> None:
        pass


def test_sparql_store(graph: Graph) -> None:
    result = graph.query(SELECT)
    body = result.serialize(format="jelly")
    assert body is not None
    _Endpoint.body = body
    server = HTTPServer(("127.0.0.1", 0), _Endpoint)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        store = SPARQLStore(
            f"http://127.0.0.1:{server.server_port}/sparql",
            returnFormat="application/x-jelly-sparql",
        )
        remote = Graph(store=store).query(SELECT)
    finally:
        server.shutdown()
    assert _Endpoint.accept == "application/x-jelly-sparql"
    assert remote == result


def test_validate_iris_is_passed_on(caplog: pytest.LogCaptureFixture) -> None:
    result = Result("SELECT")
    result.vars = [Variable("x")]
    result.bindings = [{Variable("x"): URIRef(EX + "a b")}]
    data = result.serialize(format="jelly")
    assert data is not None
    caplog.clear()
    Result.parse(io.BytesIO(data), format="jelly")
    assert not caplog.records
    Result.parse(io.BytesIO(data), format="jelly", validate_iris=True)
    assert "does not look like a valid URI" in caplog.text


def test_frames_of_many_terms() -> None:
    # More distinct IRIs than the name lookup has: frames end early
    options = SparqlOptions(
        lookup_preset=LookupPreset(max_names=128, max_prefixes=8, max_datatypes=8)
    )
    result = Result("SELECT")
    result.vars = [Variable("s"), Variable("o")]
    result.bindings = [
        {Variable("s"): URIRef(f"{EX}s{i}"), Variable("o"): Literal(i)}
        for i in range(1000)
    ]
    out = io.BytesIO()
    serialize_jelly_sparql(result, out, options)
    out.seek(0)
    frames = list(sparql_frames(out))
    max_names = options.lookup_preset.max_names
    assert len(frames) > len(result) // max_names
    assert all(frame.row_count <= max_names for frame in frames)
    out.seek(0)
    assert Result.parse(out, format="jelly") == result


def test_stream_options_are_written_once(graph: Graph) -> None:
    data = graph.query(SELECT).serialize(
        format="jelly", options=SparqlOptions(frame_values=3)
    )
    assert data is not None
    frames = list(sparql_frames(io.BytesIO(data)))
    has_options = [frame.HasField("options") for frame in frames]
    assert has_options == [True] + [False] * (len(frames) - 1)
    assert frames[0].options.version == 1
    assert frames[0].options.stream_type == jelly.SPARQL_STREAM_TYPE_FLAT
    assert list(frames[0].variables) == ["person", "name", "age"]
    assert all(not frame.variables for frame in frames[1:])


def test_lazy(graph: Graph) -> None:
    result = graph.query(SELECT)
    data = result.serialize(format="jelly", options=SparqlOptions(frame_values=3))
    assert data is not None
    inp = io.BytesIO(data)
    parsed = Result.parse(inp, format="jelly", lazy=True)
    # Only the first frame was read
    assert inp.tell() < len(data)
    assert parsed.vars == result.vars
    assert len(list(parsed)) == len(result)
    assert inp.tell() == len(data)
    assert parsed == result
    assert isinstance(parsed, JellySparqlResult)
    assert not parsed.truncated


def test_lazy_incomplete() -> None:
    result = Result("SELECT")
    result.vars = [Variable("x")]
    result.bindings = _failing_bindings()
    out = io.BytesIO()
    with pytest.raises(RuntimeError):
        serialize_jelly_sparql(result, out)
    out.seek(0)
    parsed = parse_jelly_sparql(out, lazy=True)
    rows: list[object] = []
    with pytest.raises(JellyIncompleteResultsError):  # noqa: PT012
        # Not rows.extend(parsed): it would call len(parsed), which reads all rows
        for row in parsed:
            rows.append(row)  # noqa: PERF402
    assert len(rows) == 1


def test_lazy_needs_a_stream(tmp_path: Path) -> None:
    path = tmp_path / "results.jellys"
    with pytest.raises(ValueError, match="stream"):
        Result.parse(path, format="jelly", lazy=True)  # type: ignore[arg-type]


def test_partly_iterated_result(graph: Graph) -> None:
    result = graph.query(SELECT)
    for _ in result:
        break
    parsed = _round_trip(result)
    assert len(parsed) == len(graph.query(SELECT))


def test_bindings_with_names_as_keys() -> None:
    result = Result("SELECT")
    result.vars = [Variable("x"), Variable("y")]
    result.bindings = [{"x": URIRef(EX + "a")}, {Variable("y"): Literal(1)}]  # type: ignore[dict-item]
    parsed = _round_trip(result)
    assert parsed.bindings == [
        {Variable("x"): URIRef(EX + "a")},
        {Variable("y"): Literal(1)},
    ]


def test_results_without_variables_or_answer() -> None:
    with pytest.raises(JellyConformanceError, match="no variables"):
        serialize_jelly_sparql(Result("SELECT"), io.BytesIO())
    with pytest.raises(JellyConformanceError, match="no answer"):
        serialize_jelly_sparql(Result("ASK"), io.BytesIO())


def test_sequence_goes_on_after_incomplete_results(graph: Graph) -> None:
    failing = Result("SELECT")
    failing.vars = [Variable("x")]
    failing.bindings = _failing_bindings()
    out = io.BytesIO()
    with pytest.raises(RuntimeError):
        serialize_jelly_sparql_sequence([graph.query("ASK { ?s ?p ?o }"), failing], out)
    serialize_jelly_sparql_sequence([graph.query(SELECT)], out)
    out.seek(0)
    first, second, third = parse_jelly_sparql_sequence(out)
    assert first.askAnswer is True
    assert not first.error
    assert second.error == "RuntimeError: the endpoint went away"
    assert len(second) == 1
    # Not equal: the blank nodes of a result set in a sequence are its own
    assert len(third) == len(graph.query(SELECT))


def test_lazy_links() -> None:
    result = JellySparqlResult("SELECT")
    result.vars = [Variable("x")]
    result.bindings = [{Variable("x"): URIRef(EX + "a")}]
    result.links = [EX + "about"]
    data = result.serialize(format="jelly")
    assert data is not None
    parsed = Result.parse(io.BytesIO(data), format="jelly", lazy=True)
    assert isinstance(parsed, JellySparqlResult)
    assert parsed.links == [EX + "about"]


def test_incomplete_and_truncated_results_are_passed_on(graph: Graph) -> None:
    data = graph.query(SELECT).serialize(
        format="jelly", options=SparqlOptions(frame_values=3)
    )
    assert data is not None
    frames = list(sparql_frames(io.BytesIO(data)))
    cut = data[: -(frames[-1].ByteSize() + 1)]
    with pytest.warns(JellyTruncatedResultsWarning):
        parsed = Result.parse(io.BytesIO(cut), format="jelly")
    assert parsed.truncated
    # Written again, the results say that they may be incomplete
    again = parsed.serialize(format="jelly")
    assert again is not None
    with pytest.raises(JellyIncompleteResultsError, match="ended without"):
        Result.parse(io.BytesIO(again), format="jelly")


def test_cut_frame() -> None:
    result = Result("SELECT")
    result.vars = [Variable("x")]
    result.bindings = [{Variable("x"): URIRef(EX + "a")}]
    data = result.serialize(format="jelly")
    assert data is not None
    with pytest.raises(JellyConformanceError, match="cut short"):
        Result.parse(io.BytesIO(data[:-3]), format="jelly")


def test_jelly_rdf_is_not_jelly_sparql(graph: Graph) -> None:
    data = graph.serialize(format="jelly", encoding="utf-8")
    with pytest.raises(JellyConformanceError):
        Result.parse(io.BytesIO(data), format="jelly")


def test_bytes(graph: Graph) -> None:
    data = graph.query(SELECT).serialize(format="jelly")
    assert data is not None
    assert parse_jelly_sparql(data) == graph.query(SELECT)


def test_unknown_arguments(graph: Graph) -> None:
    data = graph.query(SELECT).serialize(format="jelly")
    assert data is not None
    with pytest.raises(TypeError, match="lazzy"):
        Result.parse(io.BytesIO(data), format="jelly", lazzy=True)
    with pytest.raises(TypeError, match="option"):
        graph.query(SELECT).serialize(format="jelly", option=SparqlOptions())


def test_lazy_reading_from_a_socket() -> None:
    reader_socket, writer_socket = socket.socketpair()
    first_row_read = threading.Event()
    waited: list[bool] = []

    def produce() -> None:
        with writer_socket, writer_socket.makefile("wb") as out:
            stream = sparql_stream(SparqlOptions(frame_values=1))
            writer = FrameWriter(out, delimited=True)
            stream.start(["x"])
            frame = stream.row([URIRef(EX + "a")])
            assert frame is not None
            writer.write(frame)
            # The reader has the first row before the rest is written
            waited.append(first_row_read.wait(timeout=10))
            writer.write(stream.end())

    producer = threading.Thread(target=produce)
    producer.start()
    with reader_socket, reader_socket.makefile("rb") as inp:
        parsed = parse_jelly_sparql(inp, lazy=True)
        for _ in parsed:
            first_row_read.set()
    producer.join()
    assert waited == [True]
