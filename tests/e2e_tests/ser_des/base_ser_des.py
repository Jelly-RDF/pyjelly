from abc import ABC, abstractmethod
from typing import Union

from rdflib import Dataset, Graph

from pyjelly.options import LookupPreset

TripleGraphType = Union[Graph]
QuadGraphType = Union[Dataset]


class BaseSerDes(ABC):
    """
    Base class for serialization and deserialization tests.

    This class provides a common interface for reading and writing
    jelly/nquads/ntriples from and to byte streams. It is intended to be subclassed
    for specific libraries used for serialization and deserialization.

    Attributes:
        name (str): The name of the serialization/deserialization library.

    """

    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    def read_quads(self, in_bytes: bytes) -> QuadGraphType:
        """
        Read quads from bytes to a graph-like structure.

        Args:
            in_bytes (bytes): The input bytes to read from.

        Returns:
            QuadGraphType: The graph-like structure containing the quads.

        """

    @abstractmethod
    def write_quads(self, in_graph: QuadGraphType) -> bytes:
        """
        Write serialized quads to bytes.

        Args:
            in_graph (QuadGraphType): The graph-like structure containing the quads.

        Returns:
            bytes: The output bytes containing the serialized quads.

        """

    @abstractmethod
    def read_quads_jelly(self, in_bytes: bytes) -> QuadGraphType:
        """
        Read quads from jelly bytes.

        Args:
            in_bytes (bytes): The input bytes to read from.

        Returns:
            QuadGraphType: The graph-like structure containing the quads.

        """

    @abstractmethod
    def write_quads_jelly(
        self, in_graph: QuadGraphType, preset: LookupPreset, frame_size: int
    ) -> bytes:
        """
        Write quads to jelly bytes.

        Args:
            in_graph (QuadGraphType): The graph-like structure containing the quads.
            preset (LookupPreset): The stream options lookup preset for serialization.
            frame_size (int): The size of the frame for serialization.

        Returns:
            bytes: The output bytes containing the serialized quads.

        """

    @abstractmethod
    def read_triples(self, in_bytes: bytes) -> TripleGraphType:
        """
        Read triples from bytes to a graph-like structure.

        Args:
            in_bytes (bytes): The input bytes to read from.

        Returns:
            TripleGraphType: The graph-like structure containing the triples.

        """

    @abstractmethod
    def write_triples(self, in_graph: TripleGraphType) -> bytes:
        """
        Write triples to bytes.

        Args:
            in_graph (TripleGraphType): The graph-like structure containing the triples.

        Returns:
            bytes: The output bytes containing the serialized triples.

        """

    @abstractmethod
    def read_triples_jelly(self, in_bytes: bytes) -> TripleGraphType:
        """

        Read triples from jelly bytes.

        Args:
            in_bytes (bytes): The input bytes to read from.

        Returns:
            TripleGraphType: The graph-like structure containing the triples.

        """

    @abstractmethod
    def write_triples_jelly(
        self, in_graph: TripleGraphType, preset: LookupPreset, frame_size: int
    ) -> bytes:
        """
        Write triples to jelly bytes.

        Args:
            in_graph (TripleGraphType): The graph-like structure containing the triples.
            preset (LookupPreset): The stream options lookup preset for serialization.
            frame_size (int): The size of the frame for serialization.

        Returns:
            bytes: The output bytes containing the serialized triples.

        """
