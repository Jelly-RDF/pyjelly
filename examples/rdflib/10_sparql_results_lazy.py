from rdflib import Literal, URIRef, Variable
from rdflib.query import Result

from pyjelly.errors import JellyIncompleteResultsError

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
results.serialize(destination="many_results.jellys", format="jelly")

# Read the rows as they are needed, not all at once
with open("many_results.jellys", "rb") as file:
    parsed = Result.parse(file, format="jelly", lazy=True)
    try:
        total = sum(row.number.toPython() for row in parsed)
    except JellyIncompleteResultsError as error:
        # The writer said that the results are incomplete, e.g., the query failed
        print(f"Some results are missing: {error.error}")
        raise

print(f"The sum of the numbers is {total}")
