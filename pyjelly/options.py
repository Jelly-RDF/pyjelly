from __future__ import annotations

import mimetypes
from contextlib import suppress
from dataclasses import dataclass
from typing import Final
from typing_extensions import Self

from pyjelly import jelly
from pyjelly.errors import (
    JellyAssertionError,
    JellyConformanceError,
)

MIN_NAME_LOOKUP_SIZE: Final[int] = 8

MAX_LOOKUP_SIZE: Final[int] = 4096
MAX_VERSION: Final[int] = 2

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
class LookupPreset:
    max_names: int = DEFAULT_NAME_LOOKUP_SIZE
    max_prefixes: int = DEFAULT_PREFIX_LOOKUP_SIZE
    max_datatypes: int = DEFAULT_DATATYPE_LOOKUP_SIZE

    def __post_init__(self) -> None:
        if self.max_names < MIN_NAME_LOOKUP_SIZE:
            msg = "name lookup size must be at least 8"
            raise JellyConformanceError(msg)

    @classmethod
    def small(cls) -> Self:
        return cls(max_names=128, max_prefixes=32, max_datatypes=32)


@dataclass(frozen=True)
class StreamTypes:
    physical_type: jelly.PhysicalStreamType = jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED
    logical_type: jelly.LogicalStreamType = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED

    @property
    def flat(self) -> bool:
        return self.logical_type in (
            jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
            jelly.LOGICAL_STREAM_TYPE_FLAT_QUADS,
        )

    def __repr__(self) -> str:
        with suppress(ValueError):
            physical_type_name = jelly.PhysicalStreamType.Name(self.physical_type)
            logical_type_name = jelly.LogicalStreamType.Name(self.logical_type)
            return f"StreamTypes({physical_type_name}, {logical_type_name})"
        return f"StreamTypes({self.physical_type}, {self.logical_type})"

    def __post_init__(self) -> None:
        validate_type_compatibility(
            physical_type=self.physical_type,
            logical_type=self.logical_type,
        )


@dataclass(frozen=True)
class StreamParameters:
    generalized_statements: bool = False
    rdf_star: bool = False
    version: int = MAX_VERSION
    delimited: bool = True
    namespace_declarations: bool = False
    stream_name: str = ""


TRIPLES_ONLY_LOGICAL_TYPES = {
    jelly.LOGICAL_STREAM_TYPE_GRAPHS,
    jelly.LOGICAL_STREAM_TYPE_SUBJECT_GRAPHS,
    jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
}


def validate_type_compatibility(
    physical_type: jelly.PhysicalStreamType,
    logical_type: jelly.LogicalStreamType,
) -> None:
    if (
        physical_type == jelly.PHYSICAL_STREAM_TYPE_UNSPECIFIED
        or logical_type == jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED
    ):
        return
    triples_physical_type = physical_type == jelly.PHYSICAL_STREAM_TYPE_TRIPLES
    triples_logical_type = logical_type in TRIPLES_ONLY_LOGICAL_TYPES
    if triples_physical_type != triples_logical_type:
        physical_type_name = jelly.PhysicalStreamType.Name(physical_type)
        logical_type_name = jelly.LogicalStreamType.Name(logical_type)
        msg = f"{physical_type_name} is not compatible with {logical_type_name}"
        raise JellyAssertionError(msg)
