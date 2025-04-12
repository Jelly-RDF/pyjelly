from __future__ import annotations

from abc import abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from typing import NamedTuple, final

from typing_extensions import override


@dataclass
class StringLookup:
    """
    LRU-based string-to-ID lookup for RDF prefixes, names, and datatypes.

    Provides insertion-order-based eviction once size is exceeded.
    Tracks last assigned and last accessed IDs to support delta encoding.

    Subclasses implement term vs. entry lookup semantics.
    """

    __slots__ = ("idents", "last_assigned_id", "last_accessed_id", "size")

    last_assigned_id: int
    last_accessed_id: int
    size: int
    idents: OrderedDict[str, int]

    def __init__(self, *, size: int) -> None:
        """
        Parameters
        ----------
        size
            Maximum lookup size.
        """
        self.size = size
        self.idents = OrderedDict()
        self.last_assigned_id = 0
        self.last_accessed_id = 0

    def lookup(self, value: str) -> tuple[int, bool]:
        """
        Look up or insert a value in the LRU table.

        Parameters
        ----------
        value
            String to look up or insert.

        Returns
        -------
        tuple
            (Assigned ID, whether the value is new in the table).
        """
        ident = self.idents.get(value)
        if ident is not None:
            self.idents.move_to_end(value)
            return ident, False
        if len(self.idents) == self.size:
            _, ident = self.idents.popitem(last=False)
        else:
            ident = len(self.idents) + 1
        self.idents[value] = ident
        self.last_assigned_id = ident
        return ident, True

    @abstractmethod
    def lookup_for_term(self, value: str) -> int | None: ...

    @abstractmethod
    def lookup_for_entry(self, value: str) -> int | None: ...


@final
class PrefixLookup(StringLookup):
    """
    LRU table for RDF prefix strings with Jelly-specific ID assignment.

    Entry IDs are only emitted when a new prefix is added and the ID is non-contiguous.
    Reuses 0 as a sentinel to indicate "no new entry needed".
    """

    @override
    def lookup_for_entry(self, value: str) -> int | None:
        """
        Check if a prefix needs to be sent as an entry.

        Parameters
        ----------
        value
            Prefix string.

        Returns
        -------
        int or None
            Entry ID or None if not required.
        """
        if not value:
            return 0
        previous_id = self.last_assigned_id
        ident, is_new = super().lookup(value)
        if not is_new:
            return None
        if ident == previous_id + 1:
            return 0
        return ident

    @override
    def lookup_for_term(self, value: str) -> int | None:
        """
        Get ID to use in an RDF term for this prefix.

        Parameters
        ----------
        value
            Prefix string.

        Returns
        -------
        int or None
            Assigned ID if it changed, None if reused.
        """
        previous_id = self.last_accessed_id
        ident, is_new = super().lookup(value)
        self.last_accessed_id = ident
        assert not is_new
        if previous_id == 0:
            return ident
        if ident == previous_id:
            return None
        return ident


@final
class NameLookup(StringLookup):
    """
    LRU table for RDF name components with Jelly-specific ID emission logic.

    Behaves similarly to `PrefixLookup`, optimized for local names.
    """

    @override
    def lookup_for_entry(self, value: str) -> int | None:
        """
        Check if a name component should be emitted as an entry row.

        Parameters
        ----------
        value
            Local name string.

        Returns
        -------
        int or None
            Entry ID or None if not emitted.
        """
        previous_id = self.last_assigned_id
        ident, is_new = super().lookup(value)
        if not is_new:
            return None
        if ident == previous_id + 1:
            return 0
        return ident

    @override
    def lookup_for_term(self, value: str) -> int | None:
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
        previous_id = self.last_accessed_id
        ident, is_new = super().lookup(value)
        assert not is_new
        self.last_accessed_id = ident
        if ident == previous_id + 1:
            return 0
        return ident


@final
class DatatypeLookup(StringLookup):
    """
    LRU table for RDF datatype IRIs with hardcoded skip behavior.

    Skips the default XSD string datatype for encoding efficiency.
    """

    SKIP = "http://www.w3.org/2001/XMLSchema#string"

    def lookup(self, value: str) -> tuple[int, bool]:
        """
        Override to skip default XSD string datatype.

        Parameters
        ----------
        value
            Datatype IRI.

        Returns
        -------
        tuple
            ID and new-entry flag.
        """
        if value == self.SKIP:
            return 0, False
        return super().lookup(value)

    @override
    def lookup_for_entry(self, value: str) -> int | None:
        """
        Check if a datatype should be emitted as an entry.

        Parameters
        ----------
        value
            Datatype IRI.

        Returns
        -------
        int or None
            Entry ID or None if not emitted.
        """
        if value == self.SKIP:
            return None
        previous_ident = self.last_assigned_id
        ident, is_new = super().lookup(value)
        if ident == previous_ident + 1:
            return 0
        return ident if is_new else None

    @override
    def lookup_for_term(self, value: str) -> int | None:
        """
        Get datatype ID to use in RDF literal.

        Parameters
        ----------
        value
            Datatype IRI.

        Returns
        -------
        int or None
            Assigned ID or None if skipped or unchanged.
        """
        if value == self.SKIP:
            return None
        ident, is_new = super().lookup(value)
        self.last_accessed_id = ident
        assert not is_new
        return ident


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
