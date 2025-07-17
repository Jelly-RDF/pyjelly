import urllib.request
from rdflib import Graph
from rdflib.extras.external_graph_libs import rdflib_to_networkx_graph
import networkx as nx

# Example file for .jelly file
example_file, _ = urllib.request.urlretrieve("https://w3id.org/riverbench/v/dev.jelly")

# Parse data into a RDFLib graph instance
g = Graph()
g.parse(example_file, format="jelly")

# Convert graph into its networkx object
nx_g = rdflib_to_networkx_graph(g)

# Example calculation, get the number of connected components in a graph
num_components = nx.number_connected_components(nx_g)

# Example calculation, get top 5 objects with highest degrees, something simple in NetworkX
top5 = sorted(nx_g.degree, key=lambda x: x[1], reverse=True)[:5]

# Output the calculated characteristics
print(f"Connected components: {num_components}")
print("Top 5 nodes by degree:")
for node, deg in top5:
    print(f"{node}: {deg}")
print("All done.")
