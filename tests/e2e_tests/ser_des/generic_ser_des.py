import io
import re
from typing import Iterable

from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    GenericStatementSink,
    Literal,
    Triple,
    Quad,
    DEFAULT_GRAPH_IDENTIFIER,
)
from tests.e2e_tests.ser_des.base_ser_des import (
    BaseSerDes,
    QuadGraphType,
    TripleGraphType,
)


_R_LITERAL = re.compile(r'^"((?:[^"\\]|\\.)*)"(?:@([a-zA-Z]+(?:-[a-zA-Z0-9]+)*))?(?:\^\^<([^>]*)>)?$')


def _parse_literal(token: str) -> Literal:
    m = _R_LITERAL.match(token)
    if not m:
        raise ValueError(f"Invalid literal: {token}")
    lex, lang, dtype = m.groups()
    lex = bytes(lex, "utf-8").decode("unicode_escape")
    return Literal(lex, langtag=lang, datatype=dtype)


def _parse_iri_or_bnode(token: str):
    if token.startswith("<") and token.endswith(">"):
        return IRI(token[1:-1])
    if token.startswith("_:"):
        return BlankNode(token[2:])
    raise ValueError(f"Invalid IRI/BlankNode: {token}")


def _parse_term(token: str):
    if token.startswith("<") or token.startswith("_:"):
        return _parse_iri_or_bnode(token)
    if token.startswith('"'):
        return _parse_literal(token)
    if token == DEFAULT_GRAPH_IDENTIFIER:
        return token
    raise ValueError(f"Unrecognized term: {token}")


def _tokenize_nt_nq(line: str) -> list[str]:
    out, buf, in_str, esc, angle_depth = [], [], False, False, 0
    for ch in line:
        if in_str:
            buf.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == "<":
                angle_depth += 1
                buf.append(ch)
            elif ch == ">":
                angle_depth -= 1
                buf.append(ch)
            elif ch == '"':
                in_str = True
                buf.append(ch)
            elif ch.isspace() and not angle_depth:
                if buf:
                    out.append("".join(buf))
                    buf = []
            else:
                buf.append(ch)
    if buf:
        out.append("".join(buf))
    return out


def _read_lines(data: bytes) -> Iterable[str]:
    for raw in data.decode("utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if not line.endswith("."):
            raise ValueError("Line must end with '.'")
        yield line[:-1].strip()


def _parse_nt_to_sink(data: bytes) -> GenericStatementSink:
    sink = GenericStatementSink()
    for line in _read_lines(data):
        toks = _tokenize_nt_nq(line)
        if len(toks) != 3:
            raise ValueError("N-Triples line must have 3 terms")
        s, p, o = map(_parse_term, toks)
        sink.add(Triple(s, p, o))
    return sink


def _parse_nq_to_sink(data: bytes) -> GenericStatementSink:
    sink = GenericStatementSink()
    for line in _read_lines(data):
        toks = _tokenize_nt_nq(line)
        if len(toks) == 3:
            s, p, o = map(_parse_term, toks)
            sink.add(Quad(s, p, o, DEFAULT_GRAPH_IDENTIFIER))
        elif len(toks) == 4:
            s, p, o = map(_parse_term, toks[:3])
            g_tok = toks[3]
            if g_tok.startswith('"'):
                raise ValueError("Graph term cannot be a literal")
            g = _parse_term(g_tok)
            sink.add(Quad(s, p, o, g))
    return sink


def _serialize_triple(t: Triple) -> str:
    return f"{_str_node(t.s)} {_str_node(t.p)} {_str_node(t.o)} .\n"


def _serialize_quad(q: Quad) -> str:
    if q.g == DEFAULT_GRAPH_IDENTIFIER:
        return _serialize_triple(Triple(q.s, q.p, q.o))
    return f"{_str_node(q.s)} {_str_node(q.p)} {_str_node(q.o)} {_str_node(q.g)} .\n"


def _str_node(n) -> str:
    if isinstance(n, (IRI, BlankNode, Literal)):
        return str(n)
    if isinstance(n, str) and n == DEFAULT_GRAPH_IDENTIFIER:
        return n
    return str(n)


class GenericSerDes(BaseSerDes):
    name = "generic"

    def __init__(self) -> None:
        super().__init__(name=self.name)

    def read_quads(self, in_bytes: bytes) -> QuadGraphType:
        return _parse_nq_to_sink(in_bytes)

    def write_quads(self, in_graph: QuadGraphType) -> bytes:
        buf = io.StringIO()
        for st in in_graph.store:
            if isinstance(st, Quad):
                buf.write(_serialize_quad(st))
            else:
                buf.write(_serialize_triple(st))
        return buf.getvalue().encode("utf-8")

    def read_quads_jelly(self, in_bytes: bytes) -> QuadGraphType:
        sink = GenericStatementSink()
        sink.parse(io.BytesIO(in_bytes))
        return sink

    def write_quads_jelly(self, in_graph: QuadGraphType) -> bytes:
        out = io.BytesIO()
        in_graph.serialize(out)
        return out.getvalue()

    def read_triples(self, in_bytes: bytes) -> TripleGraphType:
        return _parse_nt_to_sink(in_bytes)

    def write_triples(self, in_graph: TripleGraphType) -> bytes:
        buf = io.StringIO()
        for st in in_graph.store:
            if isinstance(st, Triple):
                buf.write(_serialize_triple(st))
            elif isinstance(st, Quad):
                if st.g == DEFAULT_GRAPH_IDENTIFIER:
                    buf.write(_serialize_triple(Triple(st.s, st.p, st.o)))
        return buf.getvalue().encode("utf-8")

    def read_triples_jelly(self, in_bytes: bytes) -> TripleGraphType:
        sink = GenericStatementSink()
        sink.parse(io.BytesIO(in_bytes))
        if len(sink) == 0:
            return sink
        if sink.is_triples_sink:
            return sink
        triples_only = GenericStatementSink()
        for st in sink.store:
            if isinstance(st, Triple):
                triples_only.add(st)
            elif isinstance(st, Quad) and st.g == DEFAULT_GRAPH_IDENTIFIER:
                triples_only.add(Triple(st.s, st.p, st.o))
        return triples_only

    def write_triples_jelly(self, in_graph: TripleGraphType, preset, frame_size: int) -> bytes:
        out = io.BytesIO()
        in_graph.serialize(out)
        return out.getvalue()
