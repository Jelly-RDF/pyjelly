import gzip
import urllib.request

from pyjelly.integrations.rdflib.parse import parse_jelly_grouped

# Dataset: Katrina weather measurements (10k graphs)
# Documentation: https://w3id.org/riverbench/datasets/lod-katrina/dev
url = "https://w3id.org/riverbench/datasets/lod-katrina/dev/files/jelly_10K.jelly.gz"

# Load, uncompress .gz file, and pass to Jelly parser, all in a streaming manner
with (
    urllib.request.urlopen(url) as response,
    gzip.open(response) as jelly_stream,
):
    graphs = parse_jelly_grouped(jelly_stream)
    for i, graph in enumerate(graphs):
        print(f"Graph {i} in the stream has {len(graph)} triples")
        # Limit to 50 graphs for demonstration -- the rest will not be parsed
        if i >= 50:
            break
