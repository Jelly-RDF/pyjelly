from __future__ import annotations

from pathlib import Path

from pyjelly.integrations.generic.generic_sink import (
    GenericStatementSink,
    Node,
    Triple,
)


class GenericSinkSerializer:
    def __init__(self, sink: GenericStatementSink) -> None:
        self._sink = sink

    def _serialize_node(self, node: Node) -> str:
        """
        Serialize node to its string representation.

        Args:
            node (Node): Node to convert - RDF term, str, or Triple.

        Returns:
            str: string representation of Node.

        """
        if isinstance(node, Triple):
            quoted_triple = [self._serialize_node(t) for t in node]
            return "<< " + " ".join(quoted_triple) + " >>"
        return str(node)

    def serialize(self, output_filename: Path, encoding: str = "utf-8") -> None:
        """
        Serialize sink's store content to a simple N-triples/N-quads format.

        Args:
            output_filename (Path): path to the output file.
            encoding (str): encoding of output. Defaults to utf-8.

        """
        with output_filename.open("w", encoding=encoding) as output_file:
            for statement in self._sink._store:
                output_file.write(
                    " ".join(self._serialize_node(t) for t in statement) + " .\n"
                )
