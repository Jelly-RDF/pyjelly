from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import IO, Any, Iterable, Union

import rdflib
from rdflib.term import BNode, Literal, URIRef

from pyjelly.parse.decode import Adapter, Decoder
from pyjelly.parse.ioutils import get_options_and_frames

class KGTKNode:
    kind: str 
    value: str
    lang: str | None = None
    datatype: str | None = None

    def to_kgtk_text(self) -> str:
        pass

class KGTK_Triple:
    s: KGTKNode
    p: KGTKNode
    o: KGTKNode

class KGTK_Quad:
    s: KGTKNode
    p: KGTKNode
    o: KGTKNode
    g: KGTKNode


def _split_iri(iri: str) -> tuple[str, str]:
    for sep in ("#", "/", ":"):
        i = iri.rfind(sep)
        if i >= 0 and i + 1 < len(iri):
            return iri[: i + 1], iri[i + 1 :]
    return iri, ""

def _quote_double(text: KGTKNode) -> str:
    pass


def _quote_lang(KGTKNode) -> str:
    pass


def _is_int(dt: KGTKNode) -> bool:
    pass


def _is_float(dt: KGTKNode) -> bool:
    pass


def _row_id(node1: str, label: str, node2: str) -> str:
    pass


def _write_prefix_expansions(out: IO[str], ns2code: dict[str, str]) -> None:
    pass


def _rdflib_term_to_kgtk_node(term: Any) -> KGTKNode:
    pass


class KgtkAdapter(Adapter):
    def iri(self, iri: str) -> IRI:
        pass
    def bnode(self, bnode: str) -> BNode:
        return BNode(bnode)
    def default_graph(self) -> None:
        return None
    def literal(self, lex: str, language: str | None = None, datatype: str | None = None) -> Literal:
        return Literal(lex, lang=language, datatype=URIRef(datatype) if datatype else None)
    def namespace_declaration(self, name: str, iri: str) -> tuple[str, URIRef]:
        return (name, URIRef(iri))
    def triple(self, terms: Iterable[Any]) -> tuple[Any, Any, Any]:
        s, p, o = terms
        return (s, p, o)
    def quad(self, terms: Iterable[Any]) -> tuple[Any, Any, Any, Any]:
        s, p, o, g = terms
        return (s, p, o, g)
    def graph_start(self, graph_id: str) -> None:
        return None
    def graph_end(self) -> None:
        return None


class KGTKJellyParser:
    def parse(
        self,
        source: IO[bytes],
        sink: IO[str],
        *,
        include_graph_edges: bool = False,
    ) -> None:
        pass


def load_kgtk_from_jelly_file(jelly_path: Union[str, Path]) -> KgtkReader:
    pass


def load_kgtk_from_jelly_stream(jelly_bytes: IO[bytes]) -> KgtkReader:
    pass
