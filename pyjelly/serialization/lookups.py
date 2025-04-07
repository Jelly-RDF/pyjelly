from __future__ import annotations

from abc import abstractmethod
from collections import OrderedDict
from typing import NamedTuple, final
from typing_extensions import override


class StringLookup:
    idents: OrderedDict[str, int]

    def __init__(self, *, size: int) -> None:
        self.size = size
        self.idents = OrderedDict()
        self.last_entry_id = 0
        self.last_term_id = 0

    def find(self, value: str) -> tuple[int, bool]:
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
            self.last_entry_id = ident
        return ident, is_new

    @abstractmethod
    def for_term(self, value: str) -> int | None: ...

    @abstractmethod
    def for_entry(self, value: str) -> int | None: ...


@final
class PrefixLookup(StringLookup):
    @override
    def for_entry(self, value: str) -> int | None:
        previous_ident = self.last_entry_id
        ident, is_new = super().find(value)
        if not is_new:
            return None
        if not value:
            return 0
        if ident == previous_ident + 1:
            return 0
        return ident

    @override
    def for_term(self, value: str) -> int | None:
        previous_id = self.last_term_id
        ident, is_new = super().find(value)
        self.last_term_id = ident
        assert not is_new
        if previous_id == 0:
            return ident
        if ident == previous_id:
            return None
        return ident


@final
class NameLookup(StringLookup):
    @override
    def for_entry(self, value: str) -> int | None:
        previous_id = self.last_entry_id
        ident, is_new = super().find(value)
        if not is_new:
            return None
        if ident == previous_id + 1:
            return 0
        return ident

    @override
    def for_term(self, value: str) -> int | None:
        previous_id = self.last_term_id
        ident, is_new = super().find(value)
        assert not is_new
        self.last_term_id = ident
        if ident == previous_id + 1:
            return 0
        return ident


@final
class DatatypeLookup(StringLookup):
    SKIP = "http://www.w3.org/2001/XMLSchema#string"

    def find(self, value: str) -> tuple[int, bool]:
        if value == self.SKIP:
            return 0, False
        return super().find(value)

    @override
    def for_entry(self, value: str) -> int | None:
        if value == self.SKIP:
            return None
        previous_ident = self.last_entry_id
        ident, is_new = super().find(value)
        if ident == previous_ident + 1:
            return 0
        return ident if is_new else None

    @override
    def for_term(self, value: str) -> int | None:
        if value == self.SKIP:
            return None
        self.last_term_id = value
        return super().find(value)[0]


@final
class Options(NamedTuple):
    name_lookup_size: int
    prefix_lookup_size: int
    datatype_lookup_size: int
    name: str | None = None

    @staticmethod
    def default() -> Options:
        return Options(
            name_lookup_size=128,
            prefix_lookup_size=16,
            datatype_lookup_size=16,
        )

    @staticmethod
    def big() -> Options:
        return Options(
            name_lookup_size=4000,
            prefix_lookup_size=150,
            datatype_lookup_size=32,
        )
