from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, ClassVar, NamedTuple, TypeAlias, TypedDict, final

from pyjelly.pb2 import rdf_pb2 as pb

PbBlankNode: TypeAlias = str
PbTerm: TypeAlias = pb.RdfIri | PbBlankNode | pb.RdfLiteral
PbGraphTerm: TypeAlias = PbTerm | pb.RdfDefaultGraph
PbEntry: TypeAlias = pb.RdfNameEntry | pb.RdfPrefixEntry | pb.RdfDatatypeEntry


@final
class StringLookup:
    idents: OrderedDict[str, int]

    def __init__(self, *, size: int, next_offset: int = 1) -> None:
        self.size = size
        self.idents = OrderedDict()
        self.last_ident = 0
        self.next_offset = next_offset

    def find(self, value: str, *, compress: bool = False) -> tuple[int, bool]:
        ident = self.idents.get(value)
        if ident is not None:
            is_new = False
            self.idents.move_to_end(value)
        else:
            is_new = True
            if len(self.idents) == self.size:
                _, ident = self.idents.popitem(last=False)
            else:
                ident = len(self.idents) + 1
            self.idents[value] = ident
        self.last_ident = ident
        final = 0 if compress and ident == self.last_ident + self.next_offset else ident
        return final, is_new


class Options(NamedTuple):
    name_lookup_size: int
    prefix_lookup_size: int
    datatype_lookup_size: int
    name: str | None = None

    def create_lookups(self) -> dict[type[PbEntry], StringLookup]:
        return {
            pb.RdfNameEntry: StringLookup(size=self.name_lookup_size),
            pb.RdfPrefixEntry: StringLookup(
                size=self.prefix_lookup_size, next_offset=0
            ),
            pb.RdfDatatypeEntry: StringLookup(size=self.datatype_lookup_size),
        }


class TripleTerms(TypedDict):
    subject: Any | None
    predicate: Any | None
    object: Any | None


class QuadTerms(TripleTerms):
    graph: Any | None


@dataclass(init=False)
class StreamContext:
    _pb_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_UNSPECIFIED

    options: Options
    lookups: dict[type[PbEntry], StringLookup]

    def __init__(self, options: Options) -> None:
        self.options = options
        self.lookups = options.create_lookups()


@dataclass(init=False)
class TripleContext(StreamContext):
    _pb_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_TRIPLES

    repeated_terms: TripleTerms

    def __init__(self, options: Options) -> None:
        super().__init__(options)
        self.repeated_terms = TripleTerms(
            subject=None,
            predicate=None,
            object=None,
        )


@dataclass(init=False)
class QuadContext(StreamContext):
    _pb_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_QUADS

    repeated_terms: QuadTerms

    def __init__(self, options: Options) -> None:
        super().__init__(options)
        self.repeated_terms = QuadTerms(
            subject=None,
            predicate=None,
            object=None,
            graph=None,
        )

    @property
    def current_graph(self) -> Any | None:
        return self.repeated_terms["graph"]


@dataclass(init=False)
class GraphContext(StreamContext):
    _pb_type: ClassVar[pb.PhysicalStreamType] = pb.PHYSICAL_STREAM_TYPE_GRAPHS

    current_graph: Any | None

    def __init__(self, options: Options) -> None:
        super().__init__(options)
        self.repeated_terms = TripleTerms(
            subject=None,
            predicate=None,
            object=None,
        )
        self.current_graph = None
