from collections import OrderedDict
from collections.abc import Callable
from typing import Generic, NamedTuple, TypeVar

from pyjelly.pb2.rdf_pb2 import RdfDatatypeEntry, RdfNameEntry, RdfPrefixEntry


class LookupEntry(NamedTuple):
    get_ident: int
    set_ident: int
    is_new: bool


Entry = TypeVar("Entry", RdfDatatypeEntry, RdfNameEntry, RdfPrefixEntry)


class Lookup(Generic[Entry]):
    _row_factory: Callable[..., Entry]
    _cache: OrderedDict[str, int]

    def __init__(
        self,
        size: int,
        on_new_entry: Callable[[Entry], object],
    ) -> None:
        self._cache = OrderedDict()
        self._on_new_entry = on_new_entry
        self.size = size

    def _new_entry(self, ident: int) -> None: ...

    @property
    def full(self) -> int:
        return len(self._cache) == self.size

    def get(self, key: str) -> LookupEntry: ...
