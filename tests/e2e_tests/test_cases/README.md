Additional positive test cases, similar to those found in Jelly-JVM integration tests.

These should be round-tripped by pyjelly (N-Triples -> memory -> Jelly -> memory) and compared to the original RDF data. Multiple combinations of settings (stream types, lookup sizes, stream frame sizes) and implementations (e.g., rdflib, oxigraph) should be tried and ensured all work fine.

### Triples

- `nt-syntax-subm-01.nt` – N-Triples test cases taken from the [N-Triples test suite](https://www.w3.org/2013/N-TriplesReports/index.html).
- `p2_ontology.nt` – OWL ontology for ASSIST-IoT Pilot 2. Includes blank nodes and language literals.
- `rdf-stax-1-1-2.nt` – RDF-STaX ontology, version 1.1.2.
- `riverbench-assist-iot-weather-1-0-2` – RiverBench metadata of the `assist-iot-weather` dataset.
- `weather.nt` – weather data collected with the ASSIST-IoT ontology from the pilot site. Includes datatype literals.

### Quads

- `nanopub-rdf-stax.nq` – a Nanopublication containing an RDF-STaX annotation of a paper.
- `nq-syntax-tests.nq` – N-Quads test cases taken from the [N-Quads test suite](https://www.w3.org/2013/N-QuadsReports/index.html). The file includes tests named `nq-syntax-uri-*` and `nq-syntax-bnode-*`. It also includes all tests from `nt-syntax-subm-01.nt`.
- `weather-quads.nq` – several named graphs and a default graph describing mock measurements from a weather station.
