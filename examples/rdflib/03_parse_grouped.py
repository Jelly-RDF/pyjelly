from rdflib import Dataset, Graph

# libraries to load example jelly stream data
import gzip
import urllib.request

from pyjelly.integrations.rdflib.parse import parse_jelly_grouped

url = "https://w3id.org/riverbench/datasets/dbpedia-live/dev/files/jelly_10K.jelly.gz"

# load, uncompress .gz file, and pass to jelly parser
with urllib.request.urlopen(url) as resp, gzip.GzipFile(fileobj=resp) as jelly_stream:
    graphs = parse_jelly_grouped(
            jelly_stream,
            graph_factory=lambda: Graph(), 
            dataset_factory=lambda: Dataset(),
        )
    for graph in graphs:
        print(f"Number of triples in graph: {len(graph)}")