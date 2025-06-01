from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import final


@final
class Lookup:
    """
    Fixed-size 1-based string-to-index mapping with LRU eviction.

    - Assigns incrementing indices starting from 1.
    - After reaching the maximum size, reuses the existing indices from evicting
      the least-recently-used entries.
    - Index 0 is reserved for delta encoding in Jelly streams.

    To check if a key exists, use `.move(key)` and catch `KeyError`.
    If `KeyError` is raised, the key can be inserted with `.insert(key)`.

    Parameters
    ----------
    max_size
        Maximum number of entries. Zero disables lookup.

    """

    def __init__(self, max_size: int) -> None:
        self.data = OrderedDict[str, int]()
        self.max_size = max_size
        self._evicting = False

    def make_last_to_evict(self, key: str) -> None:
        self.data.move_to_end(key)

    def insert(self, key: str) -> int:
        if not self.max_size:
            msg = "lookup is zero, cannot insert"
            raise IndexError(msg)
        assert key not in self.data, f"key {key!r} already present"
        if self._evicting:
            _, index = self.data.popitem(last=False)
            self.data[key] = index
        else:
            index = len(self.data) + 1
            self.data[key] = index
            self._evicting = index == self.max_size
        return index

    def __repr__(self) -> str:
        max_size, data = self.max_size, self.data
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

    def encode_entry_index(self, key: str) -> int | None:
        """
        Get or assign the index to use in an entry.

        Returns
        -------
        int or None
            - 0 if the new index is sequential (`last_assigned_index + 1`)
            - actual assigned/reused index otherwise
            - None if the key already exists

        If the return value is None, the entry is already in the lookup and does not
        need to be emitted. Any integer value (including 0) means the entry is new
        and should be emitted.

        """
        try:
            self.lookup.make_last_to_evict(key)
            return None  # noqa: TRY300
        except KeyError:
            previous_index = self.last_assigned_index
            index = self.lookup.insert(key)
            self.last_assigned_index = index
            if index == previous_index + 1:
                return 0
            return index

    def encode_term_index(self, value: str) -> int:
        self.lookup.make_last_to_evict(value)
        current_index = self.lookup.data[value]
        self.last_reused_index = current_index
        return current_index

    def encode_prefix_term_index(self, value: str) -> int:
        if self.lookup.max_size == 0:
            return 0
        previous_index = self.last_reused_index
        if not value and previous_index == 0:
            return 0
        current_index = self.encode_term_index(value)
        if previous_index == 0:
            return current_index
        if current_index == previous_index:
            return 0
        return current_index

    def encode_name_term_index(self, value: str) -> int:
        previous_index = self.last_reused_index
        current_index = self.encode_term_index(value)
        if current_index == previous_index + 1:
            return 0
        return current_index

    def encode_datatype_term_index(self, value: str) -> int:
        if self.lookup.max_size == 0:
            return 0
        return self.encode_term_index(value)
