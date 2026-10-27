from pyjelly.integrations.generic.generic_sink import IRI, Literal
from pyjelly.integrations.generic.sparql import (
    parse_jelly_sparql,
    serialize_jelly_sparql,
)

alice = IRI("http://example.org/alice")
bob = IRI("http://example.org/bob")

# The rows of the results: one value per variable, None if it is unbound
rows = [
    (alice, Literal("Alice"), bob),
    (bob, Literal("Bob"), None),
]
with open("results.jellys", "wb") as file:
    serialize_jelly_sparql(["person", "name", "knows"], rows, file)

with open("results.jellys", "rb") as file:
    results = parse_jelly_sparql(file)
    print("Variables:", results.variables)
    # The rows are read as they are iterated, so before the file is closed
    for person, name, knows in results:
        print(person, name, knows)
