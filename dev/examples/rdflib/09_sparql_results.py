from rdflib import Graph
from rdflib.query import Result

graph = Graph()
graph.parse(
    data="""
        @prefix foaf: <http://xmlns.com/foaf/0.1/> .
        <http://example.org/alice> foaf:name "Alice" ; foaf:knows <http://example.org/bob> .
        <http://example.org/bob> foaf:name "Bob" .
    """,
    format="turtle",
)

# Run a query and write its results in Jelly-SPARQL
results = graph.query("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person ?name WHERE { ?person foaf:name ?name }
""")
results.serialize(destination="results.jellys", format="jelly")

# Read them back
with open("results.jellys", "rb") as file:
    parsed = Result.parse(file, format="jelly")

for row in parsed:
    print(f"{row.person} is called {row.name}")

# Results of ASK queries work the same way
answer = graph.query("ASK { ?person ?p 'Alice' }")
answer.serialize(destination="answer.jellys", format="jelly")
with open("answer.jellys", "rb") as file:
    print("Is anyone called Alice?", Result.parse(file, format="jelly").askAnswer)
