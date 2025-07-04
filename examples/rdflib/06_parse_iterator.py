from rdflib import Dataset, Graph
import gzip
import urllib.request
from typing import cast, IO

from pyjelly.integrations.rdflib.parse import parse_jelly_grouped

URL = "https://w3id.org/riverbench/datasets/lod-katrina/dev/files/jelly_10K.jelly.gz"
OUTPUT = "Streamed_Output.jelly"

# load, uncompress .gz file, and pass to jelly parser
with (
    urllib.request.urlopen(URL) as resp,
    cast(IO[bytes], gzip.GzipFile(fileobj=resp)) as data_stream,
):
    graphs = parse_jelly_grouped(
        data_stream,
        graph_factory=lambda: Graph(),
        dataset_factory=lambda: Dataset(),
    )

    print(f"Writing Jelly frames to {OUTPUT!r}…")
    g = Graph()
    # writing graph data onto a given path
    for _, graph in enumerate(graphs):
        for triple in graph:  
            g.add(triple)
    g.serialize(destination=OUTPUT, format="jelly")

print("Done.")
