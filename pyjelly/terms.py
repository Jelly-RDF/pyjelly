from __future__ import annotations

from collections import defaultdict, deque
from functools import partial
from typing import TYPE_CHECKING, Any, Literal

from pyjelly.pb2 import RdfIri, RdfLiteral

if TYPE_CHECKING:
    from typing_extensions import Self, TypeAlias

    from pyjelly.rdf import AnyTerm

Lookup: TypeAlias = "defaultdict[str, int]"
LookupFactory = partial(defaultdict, int)
List4 = partial(list["AnyTerm | None"], ([None] * 4))


class GeneralizedStatement(deque[Any]):
    def __init__(self, *, maxlen: Literal[3, 4], use_prefix_lookup: bool) -> None:
        super().__init__(maxlen=maxlen)
        self.name_lookup: Lookup = LookupFactory()
        self.dt_lookup: Lookup = LookupFactory()
        self.known_terms: list[AnyTerm | None] = List4()
        self.prefix_lookup: Lookup | None = None

    @classmethod
    def future_triple(cls, *, use_prefix_lookup: bool = True) -> Self:
        return cls(maxlen=3, use_prefix_lookup=use_prefix_lookup)

    @classmethod
    def future_quad(cls, *, use_prefix_lookup: bool = True) -> Self:
        return cls(maxlen=4, use_prefix_lookup=use_prefix_lookup)

    def _clear(self) -> None:
        self.name_lookup.clear()
        self.dt_lookup.clear()
        self.known_terms.clear()
        if self.prefix_lookup:
            self.prefix_lookup.clear()

    @staticmethod
    def _split_iri(iri: str) -> tuple[str, str]:
        for sep in "#", "/":
            prefix, _, name = iri.rpartition(sep)
            if prefix:
                return prefix, name
        return "", iri

    def iri(self, iri_string: str) -> RdfIri:
        name = iri_string
        prefix = ""
        if self.prefix_lookup is not None:
            prefix, name = self._split_iri(iri_string)
            self.prefix_lookup[prefix] += 1
        self.name_lookup[name] += 1
        return RdfIri(
            name_id=self.name_lookup[name],
            prefix_id=self.prefix_lookup[prefix]
            if self.prefix_lookup is not None
            else None,
        )

    def blank_node(self, node_id: str) -> str:
        return node_id

    def simple_literal(self, lexical: str) -> RdfLiteral:
        return RdfLiteral(lexical)

    def language_tagged_literal(self, lexical: str, langtag: str) -> RdfLiteral:
        return RdfLiteral(lexical, langtag=langtag)

    def typed_literal(self, lexical: str, datatype: str) -> RdfLiteral:
        self.dt_lookup[datatype] = (datatype_id := self.dt_lookup[datatype]) + 1
        return RdfLiteral(lexical, datatype=datatype_id)
