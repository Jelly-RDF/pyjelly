

import io

from rdflib import Dataset, Graph

from pyjelly.options import StreamOptions
from tests.e2e_tests.ser_des.base_ser_des import (
    BaseSerDes,
    QuadGraphType,
    TripleGraphType,
)
from tests.utils.ordered_memory import OrderedMemory


class RdflibSerDes(BaseSerDes):

    name = "rdflib"
    
    def __init__(self) -> None:
        super().__init__(name=self.name)

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

    def write_quads_jelly(self, in_graph: QuadGraphType, options: StreamOptions) -> io.BytesIO:
        out_stream = io.BytesIO()
        in_graph.serialize(destination=out_stream, format="jelly", options=options)
        return out_stream
    
    def read_triples(self, in_stream):
        g = Graph(store=OrderedMemory())
        g.parse(data=in_stream.getvalue(), format="nt")
        return g
    
    def write_triples(self, in_graph):
        out_stream = io.BytesIO()
        in_graph.serialize(destination=out_stream, format="nt")
        return out_stream
    
    def read_triples_jelly(self, in_stream: io.BytesIO) -> TripleGraphType:
        g = Graph(store=OrderedMemory())
        g.parse(data=in_stream.getvalue(), format="jelly")
        return g
    
    def write_triples_jelly(self, in_graph: TripleGraphType, options: StreamOptions) -> io.BytesIO:
        out_stream = io.BytesIO()
        in_graph.serialize(destination=out_stream, format="jelly", options=options)
        return out_stream
    