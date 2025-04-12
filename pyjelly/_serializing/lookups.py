from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import NamedTuple, final
from typing_extensions import override


@final
class Lookup(OrderedDict[str, int]):  # not a nonimal subtype!
    """
    Fixed-size 1-based string-to-ID mapping with LRU eviction.

    - Assigns incrementing IDs starting from 1.
    - After reaching `size`, evicts the least-recently-used entry and reuses its ID.
    - ID 0 is reserved for delta encoding in Jelly streams.

    To check if a key exists, use `.move_to_end(key)` and catch `KeyError`.
    If `KeyError` is raised, the key can be inserted with `.insert(key)`.

    If `size == 0`, the lookup is disabled and `.insert()` always returns 0.
    """

    def __init__(self, size: int) -> None:
        """
        Parameters
        ----------
        size
            Maximum number of entries. Zero disables lookup.
        """
        self.size = size
        self.insert = self._insert_sequential if size else self._insert_noop

    def _insert_noop(self, key: str) -> int:
        """
        No-op.

        Used when the lookup is disabled. Always returns 0.
        """
        return 0

    def _insert_sequential(self, key: str) -> int:
        """
        Assigns the next available sequential ID to `key`.

        Cannot be called when the key is already present.
        Switches to eviction mode when capacity is reached.
        """
        ident = len(self) + 1
        self[key] = ident
        if ident == self.size:
            self.insert = self._insert_evicting
        return ident

    def _insert_evicting(self, key: str) -> int:
        """
        Evict the least-recently-used key and reuse its ID for the new key.

        Cannot be called when the key is already present.
        """
        _, ident = self.popitem(last=False)
        self[key] = ident
        return ident


@dataclass
class LookupEncoder:
    """
    Shared encoder logic for Jelly lookup tables.

    - Tracks last assigned and accessed indices.
    - Emits 0 for sequential IDs (`previous + 1`) to enable zero-byte delta encoding.
    - Used by prefix, name, and datatype encoders.
    """

    __slots__ = ("lookup", "last_assigned_index", "last_accessed_index")

    last_assigned_index: int
    last_accessed_index: int

    def __init__(self, *, lookup: Lookup) -> None:
        """
        Parameters
        ----------
        size
            Maximum lookup size.
        """
        self.lookup = lookup
        self.last_assigned_index = 0
        self.last_accessed_index = 0

    def index_for_entry(self, key: str) -> int | None:
        """
        Inserts a new key or returns None if already present.

        Returns
        -------
        int | None
            - 0 if the new index is sequential (`last_assigned_index + 1`)
            - actual assigned/reused index otherwise
            - None if the key already exists

        If the return value is None, the entry is already in the lookup and does not need to be emitted.
        Any integer value (including 0) means the entry is new and should be emitted.
        """
        try:
            self.lookup.move_to_end(key)
            return None
        except KeyError:
            previous_index = self.last_assigned_index
            index = self.lookup.insert(key)
            self.last_assigned_index = index
            # > If the index is set to 0 in any other lookup entry, it MUST be interpreted
            #   as previous_index + 1, that is, the index of the previous entry incremented by one.
            # > If the index is set to 0 in the first entry of the lookup in the stream,
            #   it MUST be interpreted as the value 1.
            # Because the first value (stream-wise) of previous index is 0, two requirements are met.
            if index == previous_index + 1:
                return 0
        return index

    def index_for_term(self, value: str) -> int:
        """
        Access current index for a previously inserted value.

        Updates `last_accessed_index`.
        """
        self.lookup.move_to_end(value)
        current_index = self.lookup[value]
        self.last_accessed_index = current_index
        return current_index


@final
class PrefixLookupEncoder(LookupEncoder):
    """
    Optional Jelly encoder for RDF prefixes.

    Emits ID only when changed. Reuses 0 as a delta marker.
    """

    @override
    def index_for_term(self, value: str) -> int:
        previous_index = self.last_accessed_index
        current_index = super().index_for_term(value)
        if previous_index == 0:
            return current_index
        if current_index == previous_index:
            return None
        return current_index


@final
class NameLookupEncoder(LookupEncoder):
    """
    Required Jelly encoder for RDF local names.

    Emits 0 when the index is contiguous (`prev + 1`) for Jelly's zero-byte optimization.
    """

    @override
    def index_for_term(self, value: str) -> int:
        """
        Get ID to use in RDF term for this name.

        Parameters
        ----------
        value
            Local name string.

        Returns
        -------
        int or None
            Assigned ID or None if unchanged.
        """
        previous_index = self.last_accessed_index
        current_index = super().index_for_term(value)
        if current_index == previous_index + 1:
            return 0
        return current_index


@final
class DatatypeLookupEncoder(LookupEncoder):
    """
    Required Jelly encoder for RDF datatypes.

    Skips `xsd:string` (default). All other IRIs use standard lookup.
    """

    STRING_DATATYPE_IRI = "http://www.w3.org/2001/XMLSchema#string"

    @override
    def index_for_entry(self, value: str) -> int | None:
        if value == self.STRING_DATATYPE_IRI:
            return None
        return super().index_for_entry(value)

    @override
    def index_for_term(self, value: str) -> int:
        if value == self.STRING_DATATYPE_IRI:
            return None
        return super().index_for_term(value)


class Options(NamedTuple):
    name_lookup_size: int
    prefix_lookup_size: int
    datatype_lookup_size: int
    delimited: bool | None = None
    name: str | None = None

    @staticmethod
    def small() -> Options:
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
