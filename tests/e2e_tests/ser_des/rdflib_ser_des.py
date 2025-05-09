import io

from rdflib import Dataset, Graph

from pyjelly.options import StreamOptions
from pyjelly.producing.producers import FlatFrameProducer
from tests.e2e_tests.ser_des.base_ser_des import (
    BaseSerDes,
    QuadGraphType,
    TripleGraphType,
)
from tests.utils.ordered_memory import OrderedMemory


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

    def read_quads(self, in_stream: io.BytesIO) -> QuadGraphType:
        g = Dataset()
        g.parse(data=in_stream.getvalue(), format="nquads")
        return g

    def write_quads(self, in_graph: QuadGraphType) -> io.BytesIO:
        out_stream = io.BytesIO()
        in_graph.serialize(destination=out_stream, format="nquads")
        return out_stream

    def read_quads_jelly(self, in_stream: io.BytesIO) -> QuadGraphType:
        g = Dataset()
        g.parse(data=in_stream.getvalue(), format="jelly")
        return g

    def write_quads_jelly(
        self, in_graph: QuadGraphType, options: StreamOptions, frame_size: int
    ) -> io.BytesIO:
        out_stream = io.BytesIO()
        producer = FlatFrameProducer(quads=True, frame_size=frame_size)
        in_graph.serialize(
            destination=out_stream, format="jelly", options=options, producer=producer
        )
        return out_stream

    def read_triples(self, in_stream: io.BytesIO) -> TripleGraphType:
        g = Graph()
        g.parse(data=in_stream.getvalue(), format="nt")
        return g

    def write_triples(self, in_graph: TripleGraphType) -> io.BytesIO:
        out_stream = io.BytesIO()
        in_graph.serialize(destination=out_stream, format="nt")
        return out_stream

    def read_triples_jelly(self, in_stream: io.BytesIO) -> TripleGraphType:
        g = Graph()
        g.parse(data=in_stream.getvalue(), format="jelly")
        return g

    def write_triples_jelly(
        self, in_graph: TripleGraphType, options: StreamOptions, frame_size: int
    ) -> io.BytesIO:
        out_stream = io.BytesIO()
        producer = FlatFrameProducer(quads=False, frame_size=frame_size)
        in_graph.serialize(
            destination=out_stream, format="jelly", options=options, producer=producer
        )
        return out_stream
