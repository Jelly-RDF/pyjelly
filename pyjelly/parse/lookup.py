from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from pyjelly.errors import JellyAssertionError, JellyConformanceError
from pyjelly.options import MAX_LOOKUP_SIZE


@dataclass
class LookupDecoder:
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
        if lookup_size > MAX_LOOKUP_SIZE:
            msg = f"lookup size must be less than {MAX_LOOKUP_SIZE}"
            raise JellyAssertionError(msg)
        self.lookup_size = lookup_size
        placeholders = (None,) * lookup_size
        self.data: deque[str | None] = deque(placeholders, maxlen=lookup_size)
        self.last_assigned_index = 0
        self.last_reused_index = 0

    def assign_entry(self, index: int, value: str) -> None:
        previous_index = self.last_assigned_index
        if index == 0:
            index = previous_index + 1
        assert index > 0
        self.data[index - 1] = value
        self.last_assigned_index = index

    def at(self, index: int) -> str:
        self.last_reused_index = index
        value = self.data[index - 1]
        if value is None:
            msg = f"invalid resolved index {index}"
            raise IndexError(msg)
        return value

    def decode_prefix_term_index(self, index: int) -> str:
        actual_index = index or self.last_reused_index
        if actual_index == 0:
            return ""
        return self.at(actual_index)

    def decode_name_term_index(self, index: int) -> str:
        actual_index = index or self.last_reused_index + 1
        if actual_index == 0:
            msg = "0 is not a valid name term index"
            raise JellyConformanceError(msg)
        return self.at(actual_index)

    def decode_datatype_term_index(self, index: int) -> str | None:
        if index == 0:
            msg = "0 is not a valid datatype term index"
            raise JellyConformanceError(msg)
        return self.at(index)
