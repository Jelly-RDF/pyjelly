"""Pure-Python version of encoder lookup inspired by Jelly-JVM."""

from __future__ import annotations

from typing import NamedTuple


class LookupEntry(NamedTuple):
    get_ident: int
    set_ident: int
    is_new: bool


class Lookup:
    """Single-threaded lookup for indexing datatypes, IRI prefixes, and IRI names."""

    __slots__ = (
        "__size",
        "__used",
        "__table",
        "__entries",
        "__names",
        "__serials",
        "__full",
        "__head",
        "__entry",
        "__current_get_ident",
        "__current_set_ident",
        "__last_set_ident",
    )

    def __init__(self, size: int) -> None:
        self.__size = size
        self.__used = 0
        self.__table = [0] * (size + 1) * 2
        self.__entries: dict[str, LookupEntry] = {}
        self.__names: list[str] = [..., *("",) * size]
        self.__serials = [-1, *(0,) * size]
        self.__head: int = 0
        self.__current_get_ident: int = 0
        self.__current_set_ident: int = 0
        self.__last_set_ident: int = 0

    resize = __init__

    def _allocate_entry(self, key: str, ident: int) -> None:
        head = self.__head
        table = self.__table
        current = ident * 2
        table[current] = head
        table[head + 1] = current
        self.__head = current
        self.__names[ident] = key
        self.__entries[key] = LookupEntry(ident, ident, False)

    def _overwrite_entry(self, key: str, ident: int) -> None:
        entries = self.__entries
        old_entry = entries.pop(self.__names[ident])
        self.__names[ident] = key
        entries[key] = old_entry
        self.set_head_to(ident)
        self.__current_set_ident = 0 if self.__last_set_ident + 1 == ident else ident
        self.__last_set_ident = ident

    def set_head_to(self, ident: int) -> None:
        head = self.__head
        table = self.__table
        current = ident * 2
        if current == head:
            return
        left = table[current]
        right = table[current + 1]
        table[current] = head
        table[left + 1] = right
        table[right] = left
        table[head + 1] = current
        self.__head = current

    def enter(self, key: str) -> LookupEntry:
        if item := self.__entries.get(key):
            self.set_head_to(item.get_ident)
            return item

        if self.__used == self.__size:
            self._overwrite_entry(key, ident := self.__table[1] // 2)
        else:
            self.__used += 1
            self._allocate_entry(key, ident := self.__used)

        self.__serials[ident] += 1
        return LookupEntry(self.__current_get_ident, self.__current_set_ident, True)
