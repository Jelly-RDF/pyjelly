import urllib.request, gzip, tempfile, shutil, os
from rdflib import Graph
from rdflib.extras.external_graph_libs import rdflib_to_networkx_graph
import networkx as nx
import matplotlib.pyplot as plt

# URL to the dataset
url = "https://w3id.org/riverbench/datasets/politiquices/1.0.3/files/jelly_10K.jelly.gz"

# Load example jelly file
with urllib.request.urlopen(url) as r:
        fd, example_file = tempfile.mkstemp(suffix=".jelly"); os.close(fd)
        with gzip.GzipFile(fileobj=r) as g, open(example_file, "wb") as out:
            shutil.copyfileobj(g, out)

# Parse RDF from the Jelly format
rdf_g = Graph()
rdf_g.parse(example_file, format="jelly")
print(f"Loaded graph with {len(rdf_g)} instances.")

# Convert to a NetworkX graph from RDFLib
nx_g = rdflib_to_networkx_graph(rdf_g)

# Example calculation, get the number of connected components in a graph
num_components = nx.number_connected_components(nx_g)
print(f"Connected components: {num_components}")

# Example calculation, get top 5 objects with highest degrees, simple in NetworkX
top5 = sorted(nx_g.degree, key=lambda x: x[1], reverse=True)[:35]
print("Top 5 nodes sorted by degree:")
for node, deg in top5:
    print(f"{node}: {deg}")

#Helper function
norm = lambda x: str(x).strip().lower()

# Example calculation, shortest path between two nodes (provided at least two nodes)
source = next(n for n in nx_g if norm(n) == "putin")
target = next(n for n in nx_g if norm(n) == "obama")
source_id, target_id = list(nx_g.nodes)[200], list(nx_g.nodes)[100]
path = nx.shortest_path(nx_g, source=list(nx_g.nodes)[200], target=list(nx_g.nodes)[40])
print(f"Shortest path from {source} to {target}: {' -> '.join(path)}")

# Take first 15 nodes
nodes = list(nx_g)[:15]
subg = nx_g.subgraph(nodes)

# Draw and display the graph
pos_sub = nx.spring_layout(subg, k=5, iterations=100, scale=4)
plt.figure(figsize=(10, 10))

# Introduce your own settings for display
nx.draw_networkx(
    subg, pos_sub, font_size=12, node_size=250, linewidths=0.7
)
plt.axis("off")
plt.show()

print("All done.")
