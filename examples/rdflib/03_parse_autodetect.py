from rdflib import Graph
import pyjelly.integrations.rdflib

g = Graph()
g.parse("sunspots.jelly")
print(f"Parsed {len(g)} triples")
