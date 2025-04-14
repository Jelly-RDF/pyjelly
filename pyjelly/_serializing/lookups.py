from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import NamedTuple, final
from typing_extensions import override


@final
class Lookup:
    """
    Fixed-size 1-based string-to-index mapping with LRU eviction.

    - Assigns incrementing indices starting from 1.
    - After reaching the maximum size, reuses the existing indices from evicting the least-recently-used entries.
    - Index 0 is reserved for delta encoding in Jelly streams.

    To check if a key exists, use `.move(key)` and catch `KeyError`.
    If `KeyError` is raised, the key can be inserted with `.insert(key)`.

    If `size == 0`, the lookup is disabled and `.insert()` always returns 0.
    """

    def __init__(self, max_size: int) -> None:
        """
        Parameters
        ----------
        size
            Maximum number of entries. Zero disables lookup.
        """
        self.data = OrderedDict[str, int]()
        self._max_size = max_size
        self._evicting = False

    def move(self, key: str) -> None:
        self.data.move_to_end(key)

    def insert(self, key: str) -> int:
        if not self._max_size:
            return 0
        assert key not in self.data, f"key {key!r} already present"
        if self._evicting:
            _, index = self.data.popitem(last=False)
            self.data[key] = index
        else:
            index = len(self.data) + 1
            self.data[key] = index
            self._evicting = index == self._max_size
        return index

    def __repr__(self) -> str:
        max_size, data = self._max_size, self.data
        return f"Lookup({max_size=!r}, {data=!r})"


@dataclass
class LookupEncoder:
    """
    Shared base for RDF lookup encoders using Jelly compression.

    Tracks the last assigned and last reused index.

    Parameters
    ----------
    lookup_size
        Maximum lookup size.
    """

    last_assigned_index: int
    last_reused_index: int

    def __init__(self, *, lookup_size: int) -> None:
        self.lookup = Lookup(max_size=lookup_size)
        self.last_assigned_index = 0
        self.last_reused_index = 0

    def index_for_entry(self, key: str) -> int | None:
        """
        Inserts a new key or returns None if already present.

        Returns
        -------
        int or None
            - 0 if the new index is sequential (`last_assigned_index + 1`)
            - actual assigned/reused index otherwise
            - None if the key already exists

        If the return value is None, the entry is already in the lookup and does not need to be emitted.
        Any integer value (including 0) means the entry is new and should be emitted.
        """
        try:
            self.lookup.move(key)
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

        Updates `last_reused_index` _always_ returns a valid index
        from the lookup, which is _never_ 0.
        """
        self.lookup.move(value)
        current_index = self.lookup.data[value]
        self.last_reused_index = current_index
        return current_index


@final
class PrefixEncoder(LookupEncoder):
    """
    Optional Jelly encoder for RDF prefixes.

    Emits ID only when changed. Reuses 0 as a delta marker.
    """

    @override
    def index_for_term(self, value: str) -> int:
        previous_index = self.last_reused_index
        current_index = super().index_for_term(value)
        if value and previous_index == 0:
            return current_index
        if current_index == previous_index:
            return 0
        return current_index


@final
class NameEncoder(LookupEncoder):
    """
    Required Jelly encoder for RDF local names.

    Emits 0 when the index for term is contiguous (previous + 1).
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
        previous_index = self.last_reused_index
        current_index = super().index_for_term(value)
        if current_index == previous_index + 1:
            return 0
        return current_index


@final
class DatatypeEncoder(LookupEncoder):
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
            return 0
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
