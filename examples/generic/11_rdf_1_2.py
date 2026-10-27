from pyjelly import jelly
from pyjelly.integrations.generic.generic_sink import IRI, Literal, Triple
from pyjelly.integrations.generic.parse import parse_jelly_flat
from pyjelly.integrations.generic.serialize import flat_stream_to_file
from pyjelly.options import StreamParameters
from pyjelly.serialize.streams import SerializerOptions

alice = IRI("http://example.org/alice")


def generate_statements():
    # A triple term in the object of a statement
    yield Triple(
        alice,
        IRI("http://example.org/says"),
        Triple(alice, IRI("http://example.org/knows"), IRI("http://example.org/bob")),
    )
    # A language-tagged string with a base direction
    yield Triple(
        alice, IRI("http://example.org/name"), Literal("Alice", "en", direction="ltr")
    )


# Declare that the stream uses RDF 1.2
options = SerializerOptions(params=StreamParameters(rdf_version=jelly.RDF_VERSION_1_2))
with open("rdf12.jelly", "wb") as out_file:
    flat_stream_to_file(generate_statements(), out_file, options=options)

with open("rdf12.jelly", "rb") as in_file:
    for statement in parse_jelly_flat(in_file):
        print(statement)
