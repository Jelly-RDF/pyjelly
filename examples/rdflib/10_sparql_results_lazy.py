from rdflib import Literal, URIRef, Variable
from rdflib.query import Result

from pyjelly.errors import JellyIncompleteResultsError
from pyjelly.integrations.rdflib.sparql import (
    parse_jelly_sparql,
    serialize_jelly_sparql,
)

# Many results, made one by one
results = Result("SELECT")
results.vars = [Variable("item"), Variable("number")]
results.bindings = (
    {
        Variable("item"): URIRef(f"http://example.org/item/{i}"),
        Variable("number"): Literal(i),
    }
    for i in range(100_000)
)
with open("many_results.jellys", "wb") as file:
    serialize_jelly_sparql(results, file)

# Read the rows as they are needed, not all at once
with open("many_results.jellys", "rb") as file:
    parsed = parse_jelly_sparql(file, lazy=True)
    try:
        total = sum(row.number.toPython() for row in parsed)
    except JellyIncompleteResultsError as error:
        # The writer said that the results are incomplete, e.g., the query failed
        print(f"Some results are missing: {error.error}")
        raise

print(f"The sum of the numbers is {total}")
