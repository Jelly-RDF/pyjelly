import urllib.request
from rdflib import Graph
from rdflib.extras.external_graph_libs import rdflib_to_networkx_graph
import networkx as nx
import matplotlib.pyplot as plt

# Example file for .jelly file
example_file, _ = urllib.request.urlretrieve("https://w3id.org/riverbench/v/dev.jelly")

# Parse RDF from the .jelly
g = Graph()
g.parse(example_file, format="jelly")

# Convert to a NetworkX graph from rdflib
nx_g = rdflib_to_networkx_graph(g)

# Draw and display the graph
pos = nx.spring_layout(nx_g)
plt.figure(figsize=(10, 10))

# Introduce your own settings for display
nx.draw_networkx(
    nx_g, pos, with_labels=True, font_size=4, node_size=300, linewidths=0.6
)
plt.axis("off")
plt.show()
print("All done.")
