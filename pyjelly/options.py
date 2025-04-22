from __future__ import annotations

import mimetypes
from dataclasses import dataclass
from typing import Final
from typing_extensions import Self

MIN_NAME_LOOKUP_SIZE: Final[int] = 8
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
    version: int
    delimited: bool = True
    stream_name: str | None = None

    def __post_init__(self) -> None:
        assert self.name_lookup_size >= MIN_NAME_LOOKUP_SIZE, (
            "name lookup size must be at least 8"
        )

    @classmethod
    def small(cls) -> Self:
        return cls(
            name_lookup_size=128,
            prefix_lookup_size=32,
            datatype_lookup_size=32,
            version=1,
        )

    @classmethod
    def big(cls) -> Self:
        return cls(
            name_lookup_size=4000,
            prefix_lookup_size=150,
            datatype_lookup_size=32,
            version=1,
        )
