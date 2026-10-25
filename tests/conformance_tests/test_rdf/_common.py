from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from rdflib import Graph, Namespace, Node
from rdflib.namespace import RDF

JELLYT = Namespace("https://w3id.org/jelly/dev/tests/vocab#")
MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")

# Requirements of the tests that the rdflib integration can't pass: rdflib has no
# generalized statements, quoted triples, triple terms, or base directions
NOT_RDFLIB = {
    JELLYT.requirementRdfStar,
    JELLYT.requirementGeneralizedRdf,
    JELLYT.requirementRdf12Basic,
    JELLYT.requirementRdf12,
}


@dataclass
class ConformanceCase:
    uri: str
    name: str
    positive: bool
    #: The suite of the test: "v1_1" or "v1_2"
    suite: str
    actions: list[Path]
    results: list[Path]
    requirements: set[Node]
    id: str = field(init=False)

    def __post_init__(self) -> None:
        # Shown by pytest: suite, category and test, e.g. v1_2-messages-pos_001
        self.id = "-".join(self.uri.split("/")[-4::2] + self.uri.split("/")[-1:])

    @property
    def rdflib(self) -> bool:
        """Whether the rdflib integration can run the test."""
        return not self.requirements & NOT_RDFLIB

    @property
    def quads(self) -> bool:
        return JELLYT.requirementPhysicalTypeTriples not in self.requirements

    @property
    def graphs(self) -> bool:
        return JELLYT.requirementPhysicalTypeGraphs in self.requirements

    @property
    def messages(self) -> bool:
        return JELLYT.requirementMessages in self.requirements

    @property
    def options(self) -> Path | None:
        """The stream options of a to_jelly test."""
        for path in self.actions:
            if path.name == "stream_options.jelly":
                return path
        return None

    @property
    def inputs(self) -> list[Path]:
        """The input files, without the stream options."""
        return [path for path in self.actions if path.name != "stream_options.jelly"]


def load_cases(manifest_path: Path) -> list[ConformanceCase]:
    """Read the test cases of a manifest."""
    graph = Graph()
    graph.parse(manifest_path, format="turtle")
    manifest = next(iter(graph.subjects(RDF.type, MF.Manifest)))
    base = str(manifest).rsplit("/", 1)[0] + "/"
    suite = manifest_path.parent.parent.name

    def paths(node: Node | None) -> list[Path]:
        if node is None:
            return []
        items = graph.items(node) if (node, RDF.first, None) in graph else [node]
        return [manifest_path.parent / str(item).removeprefix(base) for item in items]

    cases = [
        ConformanceCase(
            uri=str(uri),
            name=str(graph.value(uri, MF.name) or ""),
            positive=positive,
            suite=suite,
            actions=paths(graph.value(uri, MF.action)),
            results=paths(graph.value(uri, MF.result)),
            requirements=set(graph.objects(uri, MF.requires)),
        )
        for test_class, positive in (
            (JELLYT.TestPositive, True),
            (JELLYT.TestNegative, False),
        )
        for uri in graph.subjects(RDF.type, test_class)
    ]
    return sorted(cases, key=lambda case: case.uri)
