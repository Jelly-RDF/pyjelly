from rdflib import Graph

g = Graph()
g.parse("http://xmlns.com/foaf/spec/index.rdf")
g.serialize(destination="foaf.jelly", format="jelly")
