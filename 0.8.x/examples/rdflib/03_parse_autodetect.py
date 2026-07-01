from rdflib import Graph
import pyjelly.integrations.rdflib

g = Graph()
g.parse("foaf.jelly")
