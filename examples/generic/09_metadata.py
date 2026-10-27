from collections.abc import MutableMapping
from contextvars import ContextVar
import urllib.request

from pyjelly.integrations.generic.parse import parse_jelly_grouped

frame_metadata: ContextVar[MutableMapping[str, bytes]] = ContextVar("frame_metadata")

url = "https://registry.petapico.org/nanopubs.jelly"
with urllib.request.urlopen(url) as response:
    graphs = parse_jelly_grouped(response, frame_metadata=frame_metadata)
    # Just show the first graph
    graph = next(graphs)
    print(f"The first graph in the stream has {len(graph)} triples.")
    print(f"Its frame metadata: {frame_metadata.get()}")
