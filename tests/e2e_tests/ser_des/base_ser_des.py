
import io
from abc import ABC, abstractmethod
from typing import Union

from rdflib import Dataset, Graph

from pyjelly.options import StreamOptions

# TODO: Remember to add more types to this list with more integrations  # noqa: TD002
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
    def read_quads(self, in_stream: io.BytesIO) -> QuadGraphType:
        """
        Read quads from a byte stream to a graph-like structure.

        Args:
            in_stream (io.BytesIO): The input byte stream to read from.

        Returns:
            QuadGraphType: The graph-like structure containing the quads.

        """

    @abstractmethod
    def write_quads(self, in_graph: QuadGraphType) -> io.BytesIO:
        """
        Write quads to a byte stream.

        Args:
            in_graph (QuadGraphType): The graph-like structure containing the quads.

        Returns:
            io.BytesIO: The output byte stream containing the serialized quads.

        """

    @abstractmethod
    def read_quads_jelly(self, in_stream: io.BytesIO) -> QuadGraphType:  # noqa: E501
        """
        Read quads from a jelly byte stream.

        Args:
            in_stream (io.BytesIO): The input byte stream to read from.

        Returns:
            QuadGraphType: The graph-like structure containing the quads.

        """

    @abstractmethod
    def write_quads_jelly(self, in_graph: QuadGraphType, options: StreamOptions) -> io.BytesIO:  # noqa: E501
        """
        Write quads to a jelly byte stream.

        Args:
            in_graph (QuadGraphType): The graph-like structure containing the quads.
            options (StreamOptions): The stream options for serialization.

        Returns:
            io.BytesIO: The output byte stream containing the serialized quads.

        """

    @abstractmethod
    def read_triples(self, in_stream: io.BytesIO) -> TripleGraphType:
        """
        Read triples from a byte stream to a graph-like structure.

        Args:
            in_stream (io.BytesIO): The input byte stream to read from.

        Returns:
            TripleGraphType: The graph-like structure containing the triples.

        """

    @abstractmethod
    def write_triples(self, in_graph: TripleGraphType) -> io.BytesIO:
        """
        Write triples to a byte stream.

        Args:
            in_graph (TripleGraphType): The graph-like structure containing the triples.

        Returns:
            io.BytesIO: The output byte stream containing the serialized triples.

        """

    @abstractmethod
    def read_triples_jelly(self, in_stream: io.BytesIO) -> TripleGraphType:  # noqa: E501
        """

        Read triples from a jelly byte stream.
        
        Args:
            in_stream (io.BytesIO): The input byte stream to read from.

        Returns:
            TripleGraphType: The graph-like structure containing the triples.

        """

    @abstractmethod
    def write_triples_jelly(self, in_graph: TripleGraphType, options: StreamOptions) -> io.BytesIO:  # noqa: E501
        """
        Write triples to a jelly byte stream.

        Args:
            in_graph (TripleGraphType): The graph-like structure containing the triples.
            options (StreamOptions): The stream options for serialization.

        Returns:
            io.BytesIO: The output byte stream containing the serialized triples.

        """
