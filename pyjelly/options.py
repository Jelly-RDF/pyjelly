from __future__ import annotations

import mimetypes
from contextlib import suppress
from dataclasses import dataclass
from typing import Final
from typing_extensions import Self

from mypy_extensions import mypyc_attr

from pyjelly import jelly
from pyjelly.errors import (
    JellyAssertionError,
    JellyConformanceError,
)

MIN_NAME_LOOKUP_SIZE: Final[int] = 8
#: The smallest name lookup of a Jelly-RDF 1.2 stream
MIN_COLUMN_NAME_LOOKUP_SIZE: Final[int] = 128

# The largest lookups the parser accepts, as recommended by the specification
MAX_NAME_LOOKUP_SIZE: Final[int] = 16384
MAX_PREFIX_LOOKUP_SIZE: Final[int] = 4096
MAX_DATATYPE_LOOKUP_SIZE: Final[int] = 256

# Protocol versions. Jelly-RDF 1.0 and 1.1 streams use the row layout, Jelly-RDF 1.2
# streams the column layout.
VERSION_1_0: Final[int] = 1
VERSION_1_1: Final[int] = 2
VERSION_1_2: Final[int] = 3
MIN_VERSION: Final[int] = VERSION_1_0
MAX_VERSION: Final[int] = VERSION_1_2

DEFAULT_NAME_LOOKUP_SIZE: Final[int] = 4000
DEFAULT_PREFIX_LOOKUP_SIZE: Final[int] = 150
DEFAULT_DATATYPE_LOOKUP_SIZE: Final[int] = 32

STRING_DATATYPE_IRI = "http://www.w3.org/2001/XMLSchema#string"
LANG_STRING_DATATYPE_IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
DIR_LANG_STRING_DATATYPE_IRI = (
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#dirLangString"
)

INTEGRATION_SIDE_EFFECTS: bool = True
"""
Whether to allow integration module imports to trigger side effects.

These side effects are cheap and may include populating some registries
for guessing the defaults for external integrations that work with Jelly.
"""

MIMETYPES = ("application/x-jelly-rdf",)


def is_row_layout(version: int) -> bool:
    """
    Return whether streams of a protocol version use the row layout.

    >>> is_row_layout(VERSION_1_1)
    True
    >>> is_row_layout(VERSION_1_2)
    False
    """
    return version < VERSION_1_2


def register_mimetypes(extension: str = ".jelly") -> None:
    """
    Associate files that have Jelly extension with Jelly MIME types.

    >>> register_mimetypes()
    >>> mimetypes.guess_type("out.jelly")
    ('application/x-jelly-rdf', None)
    """
    for mimetype in MIMETYPES:
        mimetypes.add_type(mimetype, extension)


@mypyc_attr(allow_interpreted_subclasses=True)
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
        """
        Return the representation of StreamTypes.

        >>> repr(StreamTypes(9999, 8888))
        'StreamTypes(9999, 8888)'
        """
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
    """
    Parameters of a stream, other than its types and lookup sizes.

    By default, streams are written in Jelly-RDF 1.2. Set `version` to 1 or 2 to
    write Jelly-RDF 1.0 or 1.1 instead, which older parsers can read.

    Attributes:
        generalized_statements: Jelly-RDF 1.0 and 1.1 only: whether the stream
            may have generalized statements.
        rdf_star: Jelly-RDF 1.0 and 1.1 only: whether the stream may have
            RDF-star statements.
        version: protocol version.
        delimited: whether the frames are delimited.
        namespace_declarations: whether the stream has namespace declarations.
            Not available in Jelly-RDF 1.0.
        stream_name: name of the stream.
        stream_type: Jelly-RDF 1.2 only: whether the stream is one graph or
            dataset (FLAT), or a sequence of RDF Messages (MESSAGES).
        rdf_version: Jelly-RDF 1.2 only: the version of RDF whose terms may occur
            in the stream. RDF 1.1 does not allow base directions or triple terms,
            RDF 1.2 Basic does not allow triple terms.

    """

    generalized_statements: bool = False
    rdf_star: bool = False
    version: int = MAX_VERSION
    delimited: bool = True
    namespace_declarations: bool = False
    stream_name: str = ""
    stream_type: jelly.RdfStreamType = jelly.RDF_STREAM_TYPE_FLAT
    rdf_version: jelly.RdfVersion = jelly.RDF_VERSION_UNSPECIFIED

    def __post_init__(self) -> None:
        if not MIN_VERSION <= self.version <= MAX_VERSION:
            msg = (
                f"unsupported protocol version {self.version}, "
                f"expected {MIN_VERSION} to {MAX_VERSION}"
            )
            raise JellyConformanceError(msg)
        if self.version == VERSION_1_0 and self.namespace_declarations:
            msg = "namespace declarations need protocol version 2 or newer"
            raise JellyConformanceError(msg)


def stream_type_for(
    params: StreamParameters, logical_type: jelly.LogicalStreamType
) -> jelly.RdfStreamType:
    """
    Return the Jelly-RDF 1.2 stream type of a stream that is written.

    As in Jelly-JVM, a stream of graphs or datasets (the logical types GRAPHS and
    DATASETS, and their subtypes) is a stream of RDF Messages, one per graph or
    dataset.

    >>> stream_type_for(StreamParameters(), jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES)
    0
    >>> stream_type_for(StreamParameters(), jelly.LOGICAL_STREAM_TYPE_NAMED_GRAPHS)
    1
    """
    if params.stream_type == jelly.RDF_STREAM_TYPE_MESSAGES or logical_type % 10 in (
        jelly.LOGICAL_STREAM_TYPE_GRAPHS,
        jelly.LOGICAL_STREAM_TYPE_DATASETS,
    ):
        return jelly.RDF_STREAM_TYPE_MESSAGES
    return jelly.RDF_STREAM_TYPE_FLAT


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
