import urllib.request, gzip, tempfile, shutil, os

import networkx as nx
import matplotlib.pyplot as plt
from rdflib import Graph, URIRef, BNode, Literal
from rdflib.extras.external_graph_libs import rdflib_to_networkx_graph

# Filter predicates for people
TARGET_PREDICATES = {
    URIRef("http://www.politiquices.pt/ent2_str"),
    URIRef("http://www.politiquices.pt/ent1_str"),
}

# URL to the dataset
url = "https://w3id.org/riverbench/datasets/politiquices/1.0.3/files/jelly_10K.jelly.gz"

# Load example jelly file
with urllib.request.urlopen(url) as r:
    fd, example_file = tempfile.mkstemp(suffix=".jelly")
    os.close(fd)
    with gzip.GzipFile(fileobj=r) as g, open(example_file, "wb") as out:
        shutil.copyfileobj(g, out)

# Parse RDF from the Jelly format
rdf_g = Graph()
rdf_g.parse(example_file, format="jelly")
print(f"Loaded graph with {len(rdf_g)} instances.")

# First step for filtering the graph
rdf_g_filtered = Graph()
for s, p, o in rdf_g.triples((None, None, None)):
    if p in TARGET_PREDICATES:
        rdf_g_filtered.add((s, p, o))

# Just filtering the graph for clarity of visualization, can be omitted
related = URIRef("http://www.politiquices.pt/related")
for b in list(rdf_g_filtered.subjects()):
    if isinstance(b, BNode):
        ent1 = ent2 = None
        for p, o in rdf_g_filtered.predicate_objects(b):
            if p == URIRef("http://www.politiquices.pt/ent1_str") and isinstance(
                o, Literal
            ):
                ent1 = o
            elif p == URIRef("http://www.politiquices.pt/ent2_str") and isinstance(
                o, Literal
            ):
                ent2 = o
        if ent1 and ent2:
            rdf_g_filtered.add((ent1, related, ent2))
            rdf_g_filtered.add((ent2, related, ent1))
        for triple in list(rdf_g_filtered.triples((b, None, None))):
            rdf_g_filtered.remove(triple)

# Convert to a NetworkX graph
nx_g = rdflib_to_networkx_graph(rdf_g_filtered)

# Example calculation, get the number of connected components in a graph
num_components = nx.number_connected_components(nx_g)
print(f"Connected components: {num_components}")

# Example calculation, get top 5 objects with highest degrees, simple in NetworkX
top5 = sorted(nx_g.degree, key=lambda x: x[1], reverse=True)[:5]
print("Top 5 nodes sorted by degree:")
for node, deg in top5:
    print(f"{node}: {deg}")

# Helper function
norm = (
    lambda n: str(n.value).strip().lower()
    if isinstance(n, Literal)
    else str(n).strip().lower()
)

# Example calculation, shortest path between two nodes (provided at least two nodes)
source = next(n for n in nx_g if norm(n) == "socrates")
target = next(n for n in nx_g if norm(n) == "obama")
path = nx.shortest_path(nx_g, source=source, target=target)
print(f"Shortest path from {source} to {target}: {' -> '.join(path)}")

# Take first 10 nodes
nodes = list(nx_g)[:10]
subg = nx_g.subgraph(nodes)

# Draw and display the graph
pos_sub = nx.spring_layout(subg, k=5, iterations=200, scale=3, seed=24)
plt.figure(figsize=(10, 10))

# Introduce your own settings for display
nx.draw_networkx(subg, pos_sub, font_size=14, node_size=220, linewidths=0.7)
plt.axis("off")
plt.show()

print("All done.")
