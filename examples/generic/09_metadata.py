from contextvars import ContextVar
from google.protobuf.internal.containers import ScalarMap
import urllib.request
from pyjelly.integrations.rdflib.parse import parse_jelly_grouped

frame_metadata: ContextVar[ScalarMap[str, bytes]] = ContextVar("frame_metadata")

url = "https://registry.petapico.org/nanopubs.jelly"
with urllib.request.urlopen(url) as response:
    graphs = parse_jelly_grouped(response, frame_metadata=frame_metadata)
    for i, graph in enumerate(graphs):
        print(f"Graph {i} in the stream has {len(graph)} triples.")
        metadata = frame_metadata.get()
        print(f"Graph #{i}: {metadata}")
        if i == 50:  # Show first 50
            break
