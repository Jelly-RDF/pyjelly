import gzip
import urllib.request

from pyjelly.integrations.generic.generic_sink import *
from pyjelly.integrations.generic.parse import parse_jelly_flat

# Dataset: OpenStreetMap data for Denmark (first 10k objects)
# Documentation: https://w3id.org/riverbench/datasets/osm2rdf-denmark/dev
url = (
    "https://w3id.org/riverbench/datasets/osm2rdf-denmark/dev/files/jelly_10K.jelly.gz"
)

# We are looking for city names in the dataset
predicate_to_look_for = IRI("https://www.openstreetmap.org/wiki/Key:addr:city")
city_names = set()

with (
    urllib.request.urlopen(url) as response,
    gzip.open(response) as jelly_stream,
):
    for event in parse_jelly_flat(jelly_stream):
        if isinstance(event, Triple):
            if str(event.p) == str(predicate_to_look_for):
                city_names.add(str(event.o))

print(f"Found {len(city_names)} unique city names in the dataset.")
print("Sample city names:")
for city in list(city_names)[:10]:
    print(f"- {city}")
