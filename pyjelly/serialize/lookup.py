from __future__ import annotations

from dataclasses import dataclass
from typing import Final, final

from mypy_extensions import mypyc_attr

from pyjelly.errors import JellyConformanceError

# epoch_start until the first epoch starts: no entry is pinned
_NO_EPOCH: Final = 1 << 62

# How many indices Lookup.victim() looks at when the next index in order is not cold
_SAMPLES: Final = 4


@mypyc_attr(allow_interpreted_subclasses=True)
@final
class Lookup:
    """
    Fixed-size 1-based string-to-index mapping.

    - Assigns incrementing indices starting from 1.
    - When full, evicts entries in index order, skipping those used recently. This
      way new entries mostly take the previous index + 1, which Jelly writes as 0,
      making files smaller. The same strategy as in Jelly-JVM.
    - Index 0 is reserved for delta encoding in Jelly streams.

    An entry is *cold* if it was not used in the last `max_size // 2` uses. An entry
    is *pinned* if it was used in the current epoch (see `new_epoch()`): what is
    being encoded still refers to it, so it must not be evicted.

    Parameters
    ----------
    max_size
        Maximum number of entries. Zero disables lookup.

    """

    def __init__(self, max_size: int) -> None:
        self.data: dict[str, int] = {}
        self.max_size = max_size
        # By index, starting with an unused index 0: the key, and when the entry
        # was last used or added, counted in uses of this lookup (`now`)
        self.keys: list[str] = [""]
        self.last_use: list[int] = [0]
        self.now = 0
        self.epoch_start = _NO_EPOCH
        self.cold_age = max_size >> 1
        # The index of the last entry that replaced an evicted one, 0 if none yet
        self.last_evicted = 0

    def new_epoch(self) -> None:
        """Start a new epoch: only the entries used from now on are pinned."""
        self.epoch_start = self.now

    def touch(self, index: int) -> None:
        """Mark the entry at the index as used."""
        self.now += 1
        self.last_use[index] = self.now

    def use(self, key: str) -> int:
        """Mark the entry as used, and return its index, or 0 if there is none."""
        index = self.data.get(key, 0)
        if index:
            self.now += 1
            self.last_use[index] = self.now
        return index

    def insert(self, key: str) -> int:
        if not self.max_size:
            msg = "lookup is zero, cannot insert"
            raise IndexError(msg)
        assert key not in self.data, f"key {key!r} already present"
        index = len(self.keys)
        if index <= self.max_size:
            self.keys.append(key)
            self.last_use.append(0)
        else:
            index = self.victim()
            del self.data[self.keys[index]]
            self.keys[index] = key
            self.last_evicted = index
        self.data[key] = index
        self.now += 1
        self.last_use[index] = self.now
        return index

    def victim(self) -> int:
        """
        Return the index of the entry to evict from the full lookup.

        That is the next index in order (after the last evicted one), if it is cold
        and not pinned. Else, the least recently used one that is not pinned among
        a few indices spread evenly over the table, starting with that next index.
        The next eviction then goes on from the index picked, which is usually
        among a run of other old entries.
        """
        size = self.max_size
        last_use = self.last_use
        epoch_start = self.epoch_start
        index = self.last_evicted + 1 if 1 <= self.last_evicted < size else 1
        last = last_use[index]
        if self.now - last >= self.cold_age and last <= epoch_start:
            return index
        best = 0
        best_last = _NO_EPOCH
        step = max(1, size // _SAMPLES)
        for _ in range(min(_SAMPLES, size)):
            last = last_use[index]
            if last < best_last and last <= epoch_start:
                best = index
                best_last = last
            index += step
            if index > size:
                index -= size
        if best:
            return best
        # The epoch pins every index sampled: the first index that it does not pin
        for _ in range(size):
            if last_use[index] <= epoch_start:
                return index
            index = 1 if index == size else index + 1
        msg = (
            f"a lookup of size {size} is too small to encode a single statement: "
            "the statement uses every one of its entries"
        )
        raise JellyConformanceError(msg)

    def __repr__(self) -> str:
        max_size, data = self.max_size, self.data
        return f"Lookup({max_size=!r}, {data=!r})"


@mypyc_attr(allow_interpreted_subclasses=True)
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
        Get or assign the index to use in an entry, and mark the entry as used.

        Call it once for every use of the key, before encode_*_term_index().

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
        _, set_index = self.encode_entry(key)
        return None if set_index < 0 else set_index

    def encode_entry(self, key: str) -> tuple[int, int]:
        """
        Get or add the entry for the key, and mark it as used.

        Returns
        -------
        tuple[int, int]
            The entry's index, and the index to write in a new entry row: -1 if
            the entry was already there (no row needed), 0 if the new index is
            sequential (`last_assigned_index + 1`), else the index.

        """
        index = self.lookup.use(key)
        if index:
            return index, -1
        previous_index = self.last_assigned_index
        index = self.lookup.insert(key)
        self.last_assigned_index = index
        if index == previous_index + 1:
            return index, 0
        return index, index

    def name_index_to_write(self, index: int) -> int:
        """Return how to write a reference to the name entry at the index."""
        previous_index = self.last_reused_index
        self.last_reused_index = index
        if index == previous_index + 1:
            return 0
        return index

    def prefix_index_to_write(self, index: int, *, empty: bool) -> int:
        """Return how to write a reference to the prefix entry at the index."""
        previous_index = self.last_reused_index
        if empty and previous_index == 0:
            return 0
        self.last_reused_index = index
        if previous_index == 0:
            return index
        if index == previous_index:
            return 0
        return index

    def encode_term_index(self, value: str) -> int:
        # The entry was already marked as used by encode_entry_index()
        current_index = self.lookup.data[value]
        self.last_reused_index = current_index
        return current_index

    def encode_datatype_term_index(self, value: str) -> int:
        if self.lookup.max_size == 0:
            return 0
        return self.encode_term_index(value)
