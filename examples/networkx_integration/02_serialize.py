import networkx as nx
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF

# An example NetworkX graph
nx_g = nx.Graph()
nx_g.add_node("http://example.org/A")
nx_g.add_node("http://example.org/B")
nx_g.add_edge("http://example.org/A", "http://example.org/B")

# We define RDFLib graph for further conversion
rdf_g = Graph()

# Example namespace
ex = Namespace("http://example.org/ns#")

# Add triples through node information in NetworkX graph
for node_uri, data in nx_g.nodes(data=True):
    subj = URIRef(node_uri)
    rdf_g.add((subj, RDF.type, ex.Node))

# Add triples through edge information in NetworkX graph
for u, v, attr in nx_g.edges(data=True):
    rdf_g.add((URIRef(u), ex.connectedTo, URIRef(v)))

# Serialize graph into a .jelly file
rdf_g.serialize(destination="networkx_graph.jelly", format="jelly")
print("All done.")
