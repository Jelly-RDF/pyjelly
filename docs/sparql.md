# SPARQL query results

pyjelly reads and writes SPARQL query results in [Jelly-SPARQL]({{ proto_link("specification/sparql/") }}): the results of `SELECT` queries (solution sequences) and `ASK` queries (boolean results). Like Jelly-RDF, it is a compact binary format that can be written and read as a stream.

The media type of Jelly-SPARQL is `application/x-jelly-sparql`, and the file extension is `.jellys`.

## Usage with RDFLib

With pyjelly installed, RDFLib reads and writes query results in the `jelly` format, like it does `json` or `xml`:

{{ code_example('rdflib/09_sparql_results.py') }}

`Result.parse` returns a `JellySparqlResult` (from `pyjelly.integrations.rdflib.sparql`), which is an RDFLib `Result` with more attributes. RDFLib's types say that `Result.parse` returns a `Result`, so check `isinstance(result, JellySparqlResult)` to use them:

- `links` – the IRIs of links that describe the results, like `head.link` in [SPARQL JSON results](https://www.w3.org/TR/sparql12-results-json/). They are written too, if the result you write has them.
- `truncated` – `True` if the stream ended without saying that the results are complete, so some may be missing. pyjelly also issues a `JellyTruncatedResultsWarning` then. A stream cut in the middle of a frame can't be read at all, and raises `JellyConformanceError`.
- `error` – why the results are incomplete, in a [sequence of results](#sequences-of-results), see [incomplete results](#incomplete-results).

RDFLib does not support RDF 1.2 yet, so triple terms and literals with a base direction can't be read into RDFLib. Use the [generic API](#generic-api) for them.

### Querying a SPARQL endpoint

RDFLib's `SPARQLStore` can ask a SPARQL endpoint for Jelly-SPARQL, if the endpoint offers it (e.g., Apache Jena Fuseki with the [Jelly-JVM plugin](https://w3id.org/jelly/jelly-jvm/dev/getting-started-plugins/)):

```python
from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLStore

store = SPARQLStore(
    "http://localhost:3030/dataset/sparql",
    returnFormat="application/x-jelly-sparql",
)
for row in Graph(store=store).query("SELECT * WHERE { ?s ?p ?o } LIMIT 10"):
    print(row)
```

!!! warning

    Use the media type, `application/x-jelly-sparql`, as `returnFormat`, not `jelly`. With `jelly`, RDFLib doesn't ask for any format, so the endpoint answers in its default one.

### Large results

By default, `Result.parse` reads all rows at once. With `lazy=True`, the rows are read as the result is iterated, like RDFLib does with the results of its own queries. The stream must then stay open until you are done with the result.

When writing, the rows of an RDFLib result are written as the query makes them, so the first frames are written before the query is done. Each frame is flushed when it's written, so a reader at the other end of a connection gets it right away.

{{ code_example('rdflib/10_sparql_results_lazy.py') }}

!!! note

    Iterate a lazy result with a `for` loop. `list(result)`, `len(result)` and `bool(result)` read all of its rows first, as for any RDFLib result.

### Incomplete results

A query can fail after some of its results were already written: a timeout, or a broken backend. Jelly-SPARQL then ends the stream with an error, so that readers know that the results are incomplete. pyjelly does this for you: if getting a row fails while results are being written, the stream ends with the error, and the error is raised.

When reading such results, pyjelly raises `JellyIncompleteResultsError` after the rows that were read. Its `error` attribute is the message of the writer, and its `result` attribute holds the results read before the error:

```python
from pyjelly.errors import JellyIncompleteResultsError

try:
    result = Result.parse(file, format="jelly")
except JellyIncompleteResultsError as error:
    print(f"The query failed: {error.error}")
    partial_result = error.result
```

If you write results that you read from Jelly-SPARQL, and they were incomplete or truncated, pyjelly writes them as incomplete too.

### Options

To change how the results are written, pass `SparqlOptions` to `serialize`:

```python
from pyjelly.options import LookupPreset, SparqlOptions

options = SparqlOptions(
    # Sizes of the lookups, see below
    lookup_preset=LookupPreset(max_names=4096, max_prefixes=256, max_datatypes=32),
    # How many values (rows times variables) a frame has, at most
    frame_values=4096,
)
results.serialize(destination="results.jellys", format="jelly", options=options)
```

The default lookup sizes are larger than for Jelly-RDF (8192 names, 1024 prefixes and 64 datatypes), as in Jelly-JVM: a row of results tends to have more distinct terms than an RDF statement. Readers accept lookups of up to 16384 names, 4096 prefixes and 256 datatypes.

With `delimited=False`, the results are written as one frame without its size, e.g., to be one message of a message broker. Then the lookups must be large enough to hold all terms of the results. Files and HTTP responses must be delimited, which is the default. To send the frames of the results as separate messages, see [writing frame by frame](#writing-frame-by-frame).

### Sequences of results

A Jelly-SPARQL stream can also hold a sequence of results, e.g., the answers to a stream of queries, or to a query evaluated again and again. This is the `PUNCTUATED` stream type of Jelly-SPARQL. To write and read such a stream, use the functions in `pyjelly.integrations.rdflib.sparql`:

```python
from pyjelly.integrations.rdflib.sparql import (
    parse_jelly_sparql_sequence,
    serialize_jelly_sparql_sequence,
)

with open("answers.jellys", "wb") as file:
    serialize_jelly_sparql_sequence([result_1, result_2, ask_result], file)

with open("answers.jellys", "rb") as file:
    for result in parse_jelly_sparql_sequence(file):
        print(result.type, len(result))
```

The lookups are shared by the results of the stream, so the terms they have in common are written once. Blank nodes are not: each result has its own.

An incomplete result doesn't end a sequence, as there may be more results after it. `parse_jelly_sparql_sequence` doesn't raise an error for it, but sets its `error` attribute, so check it.

## Generic API

The [generic API](generic-sink.md) reads and writes results with its own terms (`IRI`, `Literal`, `BlankNode`, and `Triple` for triple terms), so it supports all of RDF 1.2. A row is a tuple with one value per variable, or `None` if the variable is unbound:

{{ code_example('generic/10_sparql_results.py') }}

`parse_jelly_sparql` returns a `SparqlResultSet`, with the `variables`, the `boolean` result of an `ASK` query (`None` for a `SELECT` query, see also `is_boolean`), the `links`, and whether it is `truncated`. Its rows are read as you iterate it, so the input must stay open until then, and the rows can be iterated only once. If the writer said that the results are incomplete, iterating the rows raises `JellyIncompleteResultsError` after the last one, and its `error` attribute says why. For an `ASK` query, `parse_jelly_sparql` raises it right away.

`serialize_jelly_sparql_ask` writes the result of an `ASK` query. `parse_jelly_sparql_sequence` and `serialize_jelly_sparql_sequence` read and write [sequences of results](#sequences-of-results): each result written is a pair of the variables and the rows, or a boolean result.

### Writing frame by frame

To write results frame by frame, e.g., to send each frame as a message over Kafka, use `SparqlStream` directly. It works with the terms of RDFLib (`pyjelly.integrations.rdflib.sparql.sparql_stream()`) or of the generic API (`pyjelly.integrations.generic.sparql.sparql_stream()`):

```python
from pyjelly.integrations.generic.sparql import sparql_stream

stream = sparql_stream()
stream.start(["person", "name"])
for row in rows:
    # A frame, when it is full
    if frame := stream.row(row):
        send(frame.SerializeToString())
# The last frame of the results, which says they are complete
send(stream.end().SerializeToString())
```

If the results can't be completed, end them with `stream.end("why")` instead.

## Conformance

pyjelly passes the [conformance tests of Jelly-SPARQL]({{ proto_link("conformance/sparql-test-cases/") }}), with the generic API. The RDFLib integration passes all tests except those of RDF 1.2 terms.
