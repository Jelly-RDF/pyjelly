from __future__ import annotations

from collections import deque
from dataclasses import dataclass


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
        self.lookup_size = lookup_size
        placeholders = (None,) * lookup_size
        self.data: deque[str | None] = deque(placeholders, maxlen=lookup_size)
        self.last_assigned_index = 0
        self.last_reused_index = 0

    def assign_entry(self, index: int, value: str) -> str:
        previous_index = self.last_assigned_index
        if index == 0:
            index = previous_index + 1
        assert index > 0
        self.data[index - 1] = value
        self.last_assigned_index = index
        return value

    def at(self, index: int) -> str:
        if index == 0:
            return ""
        self.last_reused_index = index
        value = self.data[index - 1]
        if value is None:
            msg = f"invalid resolved index {index}"
            raise IndexError(msg)
        return value

    def decode_prefix_term_index(self, index: int) -> str:
        return self.at(index or self.last_reused_index)

    def decode_name_term_index(self, index: int) -> str:
        return self.at(index or self.last_reused_index + 1)

    def decode_datatype_term_index(self, index: int) -> str:
        return self.at(index)
