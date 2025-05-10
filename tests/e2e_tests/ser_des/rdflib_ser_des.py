from rdflib import Dataset, Graph
import io
from pyjelly.options import StreamOptions
from pyjelly.producing.producers import FlatFrameProducer
from tests.e2e_tests.ser_des.base_ser_des import (
    BaseSerDes,
    QuadGraphType,
    TripleGraphType,
)


class RdflibSerDes(BaseSerDes):
    """
    Serialization and deserialization using rdflib.

    Args:
        BaseSerDes (_type_): _description_

    Returns:
        _type_: _description_

    """

    name = "rdflib"

    def __init__(self) -> None:
        super().__init__(name=self.name)

    def len_quads(self, graph: QuadGraphType) -> int:
        return len(list(graph.quads()))

    def len_triples(self, graph: TripleGraphType) -> int:
        return len(list(graph.triples((None, None, None))))

    def read_quads(self, in_bytes: bytes) -> QuadGraphType:
        g = Dataset()
        g.parse(data=in_bytes, format="nquads")
        return g

    def write_quads(self, in_graph: QuadGraphType) -> bytes:
        destination = io.BytesIO()
        out = in_graph.serialize(destination=destination, format="nquads")
        return destination.getvalue()

    def read_quads_jelly(self, in_bytes: bytes) -> QuadGraphType:
        g = Dataset()
        g.parse(data=in_bytes, format="jelly")
        return g

    def write_quads_jelly(
        self, in_graph: QuadGraphType, options: StreamOptions, frame_size: int
    ) -> bytes:
        destination = io.BytesIO()
        producer = FlatFrameProducer(quads=True, frame_size=frame_size)
        in_graph.serialize(
            destination=destination, format="jelly", options=options, producer=producer
        )
        return destination.getvalue()

    def read_triples(self, in_bytes: bytes) -> TripleGraphType:
        g = Graph()
        g.parse(data=in_bytes, format="nt")
        return g

    def write_triples(self, in_graph: TripleGraphType) -> bytes:
        destination = io.BytesIO()
        out = in_graph.serialize(destination=destination, format="nt")
        return destination.getvalue()

    def read_triples_jelly(self, in_bytes: bytes) -> TripleGraphType:
        g = Graph()
        g.parse(data=in_bytes, format="jelly")
        return g

    def write_triples_jelly(
        self, in_graph: TripleGraphType, options: StreamOptions, frame_size: int
    ) -> bytes:
        destination = io.BytesIO()
        producer = FlatFrameProducer(quads=False, frame_size=frame_size)
        in_graph.serialize(
            destination = destination, format="jelly", options=options, producer=producer
        )
        return destination.getvalue()
