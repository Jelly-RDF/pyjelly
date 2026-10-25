from __future__ import annotations

from dataclasses import dataclass

from pyjelly.errors import JellyConformanceError


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
        # A list, not a deque: indexing a deque walks through its blocks
        self.data: list[str | None] = [None] * lookup_size
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
