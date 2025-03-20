from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial, partialmethod, singledispatch
from typing import TYPE_CHECKING, Any, ClassVar, Generic, NamedTuple, TypeVar

from pyjelly.pb2 import RdfIri, RdfLiteral, RdfQuad, RdfTriple
from pyjelly.terms import (
    add_blank_node,
    add_iri,
    add_language_tagged_literal,
    add_typed_literal,
    last_row,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias


__all__ = (
    "IRI",
    "BlankNode",
    "SimpleLiteral",
    "LanguageTaggedLiteral",
    "TypedLiteral",
    "AnyLiteral",
    "AnyTerm",
    "Subject",
    "Object",
    "Predicate",
    "GraphLabel",
)


class IRI(str):
    typename: ClassVar[str] = "iri"

    def push(self) -> RdfIri:
        add_iri(self)
        return last_row()


class BlankNode(str):
    id: str
    typename: ClassVar[str] = "bnode"

    def push(self) -> str:
        add_blank_node(self)
        return self


class SimpleLiteral(str):
    typename: ClassVar[str] = "literal"

    def push(self) -> RdfLiteral:
        return RdfLiteral(self)


class LanguageTaggedLiteral(NamedTuple):
    lexical: str
    langtag: str

    typename = "literal"

    def push(self) -> RdfLiteral:
        add_language_tagged_literal(self.lexical, self.langtag)
        return last_row()


class TypedLiteral(NamedTuple):
    lexical: str
    datatype: str

    typename = "literal"

    def push(self) -> RdfLiteral:
        add_typed_literal(self.lexical, self.datatype)
        return last_row()


AnyLiteral: TypeAlias = "SimpleLiteral | LanguageTaggedLiteral | TypedLiteral"

AnyTerm: TypeAlias = "IRI | BlankNode | AnyLiteral"


Subject = TypeVar("Subject", "IRI", "BlankNode")
Predicate = TypeVar("Predicate", bound="IRI")
Object = TypeVar("Object", "IRI", "BlankNode", "AnyLiteral")
GraphLabel = TypeVar("GraphLabel", "IRI", "BlankNode")


class Triple(NamedTuple, Generic[Subject, Predicate, Object]):
    subject: Subject
    predicate: Predicate
    object: Object

    def push(self) -> RdfTriple:
        args: dict[str, Any] = {
            f"{char}_{term.typename}": term.push() for char, term in zip("spo", self)
        }
        return RdfTriple(**args)


class Quad(NamedTuple, Generic[Subject, Predicate, Object, GraphLabel]):
    subject: Subject
    predicate: Predicate
    object: Object
    graph: GraphLabel

    def __jelly__(self) -> RdfQuad:
        args: dict[str, Any] = {
            f"{char}_{term.typename}": term.push() for char, term in zip("spog", self)
        }
        return RdfQuad(**args)
