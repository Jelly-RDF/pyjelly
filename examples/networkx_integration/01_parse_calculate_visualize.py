import urllib.request
from rdflib import Graph
from rdflib.extras.external_graph_libs import rdflib_to_networkx_graph
import networkx as nx
import matplotlib.pyplot as plt

# Example Jelly file
example_file, _ = urllib.request.urlretrieve("https://w3id.org/riverbench/v/dev.jelly")

# Parse RDF from the Jelly format
rdf_g = Graph()
rdf_g.parse(example_file, format="jelly")

# Convert to a NetworkX graph from RDFLib
nx_g = rdflib_to_networkx_graph(rdf_g)

# Example calculation, get the number of connected components in a graph
num_components = nx.number_connected_components(nx_g)

# Example calculation, get top 5 objects with highest degrees, simple in NetworkX
top5 = sorted(nx_g.degree, key=lambda x: x[1], reverse=True)[:5]

# Example calculation, shortest path between two nodes (provided at least two nodes)
source, target = list(nx_g.nodes)[0], list(nx_g.nodes)[-1]
path = nx.shortest_path(nx_g, source=source, target=target)

# Take first 8 nodes
nodes = list(nx_g)[:8]
subg = nx_g.subgraph(nodes)

# Draw and display the graph
pos_sub = nx.spring_layout(subg, k=5, iterations=100, scale=4)
plt.figure(figsize=(10, 10))

# strip the unwanted prefix from each label
labels = {n: n.removeprefix("https://w3id.org/") for n in subg}

# Introduce your own settings for display
nx.draw_networkx(
    subg, pos_sub, labels=labels, font_size=7, node_size=300, linewidths=0.6
)
plt.axis("off")
plt.show()

print(f"Connected components: {num_components}")
print("Top 5 nodes sorted by degree:")
for node, deg in top5:
    print(f"{node}: {deg}")
print(f"Shortest path from {source} to {target}: {' -> '.join(path)}")
print("All done.")
