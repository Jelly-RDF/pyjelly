import gzip
import urllib.request
from pathlib import Path
from rdflib import Graph
from rdflib.plugins.parsers.ntriples import W3CNTriplesParser, NTGraphSink

NT_URL = "https://w3id.org/riverbench/datasets/lod-katrina/dev/files/flat_full.nt.gz"
OUTPUT = Path("Iterator_output.jelly")


class SingleTripleSink(NTGraphSink):
    def __init__(self):
        super().__init__(Graph())
        self.last_graph = None

    def triple(self, s, p, o):
        super().triple(s, p, o)
        self.last_graph = self.g
        self.g = Graph()


def stream_graphs_from_nt(url):
    sink = SingleTripleSink()
    parser = W3CNTriplesParser(sink=sink)
    with urllib.request.urlopen(url) as resp, gzip.GzipFile(fileobj=resp) as nt_stream:

        for raw in nt_stream:
            line = raw.decode("utf-8", errors="ignore")
            if not line.strip():
                continue
            try:
                parser.parsestring(line)
            except Exception:
                continue
            if sink.last_graph is not None:
                yield sink.last_graph
                sink.last_graph = None


graphs = stream_graphs_from_nt(NT_URL)
print(f"Loaded generator of graphs from the file {NT_URL} file succesfuly!")


print(f"Writing Jelly frames to {OUTPUT!r}…")
g = Graph()
for idx, triple in enumerate(graphs):
    g.add(*triple)
g.serialize(destination=OUTPUT, format="jelly")

print("Done, finished saving the file.")
