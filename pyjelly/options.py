from __future__ import annotations

import mimetypes
from dataclasses import dataclass
from typing import Any, Final

from pyjelly import jelly
from pyjelly.errors import JellyAssertionError, JellyConformanceError

MIN_NAME_LOOKUP_SIZE: Final[int] = 8

DEFAULT_NAME_LOOKUP_SIZE: Final[int] = 4000
DEFAULT_PREFIX_LOOKUP_SIZE: Final[int] = 150
DEFAULT_DATATYPE_LOOKUP_SIZE: Final[int] = 32

STRING_DATATYPE_IRI = "http://www.w3.org/2001/XMLSchema#string"

INTEGRATION_SIDE_EFFECTS: bool = True
"""
Whether to allow integration module imports to trigger side effects.

These side effects are cheap and may include populating some registries
for guessing the defaults for external integrations that work with Jelly.
"""

MIMETYPES = ("application/x-jelly-rdf",)


def register_mimetypes(extension: str = ".jelly") -> None:
    """
    Associate files that have Jelly extension with Jelly MIME types.

    >>> register_mimetypes()
    >>> mimetypes.guess_type("out.jelly")
    ('application/x-jelly-rdf', None)
    """
    for mimetype in MIMETYPES:
        mimetypes.add_type(mimetype, extension)


@dataclass(frozen=True)
class StreamOptions:
    name_lookup_size: int
    prefix_lookup_size: int
    datatype_lookup_size: int
    generalized_statements: bool = False
    rdf_star: bool = False
    version: int = 1
    delimited: bool = True
    stream_name: str | None = None

    def __post_init__(self) -> None:
        if self.name_lookup_size < MIN_NAME_LOOKUP_SIZE:
            msg = "name lookup size must be at least 8"
            raise JellyConformanceError(msg)

    @staticmethod
    def small() -> StreamOptions:
        return StreamOptions(
            name_lookup_size=128,
            prefix_lookup_size=32,
            datatype_lookup_size=32,
        )

    @staticmethod
    def big() -> StreamOptions:
        return StreamOptions(
            name_lookup_size=DEFAULT_NAME_LOOKUP_SIZE,
            prefix_lookup_size=DEFAULT_PREFIX_LOOKUP_SIZE,
            datatype_lookup_size=DEFAULT_DATATYPE_LOOKUP_SIZE,
        )


class ConsumerStreamOptions(StreamOptions):
    physical_type: jelly.PhysicalStreamType
    logical_type: jelly.LogicalStreamType

    def __init__(
        self,
        *,
        physical_type: jelly.PhysicalStreamType,
        logical_type: jelly.LogicalStreamType,
        **kwds: Any,
    ) -> None:
        super().__init__(**kwds)
        self.physical_type = physical_type
        self.logical_type = logical_type
        validate_type_compatibility(physical_type, logical_type)


# Logical stream types that are compatible only with the triples physical stream type.
TRIPLES_ONLY_LOGICAL_TYPES = {
    jelly.LOGICAL_STREAM_TYPE_GRAPHS,
    jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS,
    jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
}


def validate_type_compatibility(
    physical_type: jelly.PhysicalStreamType,
    logical_type: jelly.LogicalStreamType,
) -> None:
    triples_physical_type = physical_type is jelly.PHYSICAL_STREAM_TYPE_TRIPLES
    triples_logical_type = logical_type in TRIPLES_ONLY_LOGICAL_TYPES
    if triples_physical_type != triples_logical_type:
        msg = (
            f"physical type {physical_type} is not compatible "
            f"with logical type {logical_type}"
        )
        raise JellyAssertionError(msg)
