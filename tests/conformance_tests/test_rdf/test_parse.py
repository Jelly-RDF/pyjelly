from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

import pytest
from rdflib import Dataset, Graph, Node, URIRef
from rdflib import Literal as RdfLiteral
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.namespace import RDF
from rdflib.plugins.serializers.nt import _quoteLiteral

from pyjelly.integrations.generic.parse import (
    parse_jelly_grouped as generic_parse_jelly_grouped,
)
from pyjelly.integrations.rdflib.parse import parse_jelly_grouped
from tests.conformance_tests.test_rdf._common import (
    JELLYT,
    MF,
    categorize_by_requires,
)
from tests.meta import RDF_FROM_JELLY_MANIFEST, TEST_OUTPUTS_DIR
from tests.utils.generic_sink_test_serializer import GenericSinkSerializer
from tests.utils.ordered_memory import OrderedMemory
from tests.utils.rdf_test_cases import jelly_validate, needs_jelly_cli

# Manifest path for from_jelly tests. This is where all test cases are defined.
FROM_JELLY_MANIFEST = RDF_FROM_JELLY_MANIFEST


@dataclass
class FromJellyTestCase:
    uri: str
    name: str
    action_path: Path
    result_paths: list[Path] | None
    test_type: str
    category: str
    id: str = field(init=False)

    def __post_init__(self) -> None:
        # Custom id for pytest output to easily see type/category/case
        self.id = f"{self.test_type}-{self.category}-{self.action_path.parent.name}"


# Load all test cases from the manifest file
# Important: This function translates the RDF manifest
# into Python test cases.
# If manifests change their structure (e.g. new predicates), update logic here.
def load_from_jelly_manifest_cases(manifest_path: Path) -> list[FromJellyTestCase]:
    if not manifest_path.exists():
        return []
    graph = Graph()
    graph.parse(manifest_path, format="turtle")
    manifest_dir = manifest_path.parent
    base_uri_from_manifest = "https://w3id.org/jelly/dev/tests/rdf/from_jelly/"
    test_cases = []
    test_type_map = {
        JELLYT.TestPositive: "positive",
        JELLYT.TestNegative: "negative",
    }
    for test_class, test_type_str in test_type_map.items():
        for test_uri in graph.subjects(RDF.type, test_class):
            if not isinstance(test_uri, URIRef):
                continue
            # Map MF.action to the actual input file path
            action_uri = graph.value(test_uri, MF.action)
            action_rel_path = str(action_uri).replace(base_uri_from_manifest, "")
            action_path = manifest_dir / action_rel_path

            # MF.result can be a single file or an RDF list of files
            result_paths = None
            result_node = graph.value(test_uri, MF.result)
            if result_node:
                if (result_node, RDF.first, None) in graph:
                    # Handle list of result files
                    result_uris = graph.items(result_node)
                    result_paths = [
                        manifest_dir / str(uri).replace(base_uri_from_manifest, "")
                        for uri in result_uris
                    ]
                else:
                    # Single result file
                    result_rel_path = str(result_node).replace(
                        base_uri_from_manifest, ""
                    )
                    result_paths = [manifest_dir / result_rel_path]

            # Each test case knows its category (rdf11/generalized/etc.)
            # from categorize_by_requires
            test_cases.append(
                FromJellyTestCase(
                    uri=str(test_uri),
                    name=str(graph.value(test_uri, MF.name) or ""),
                    action_path=action_path,
                    result_paths=result_paths,
                    test_type=test_type_str,
                    category=categorize_by_requires(graph, test_uri),
                )
            )
    return test_cases


# Patch rdflib nquads serializer to avoid writing default graph id
# Without this patch rdflib includes the default graph id explicitly,
# which breaks comparisons.
def _new_nq_row(triple: tuple[Node, Node, Node], context: Graph) -> str:
    template = "%s " * (3 + (context != DATASET_DEFAULT_GRAPH_ID)) + ".\n"
    args = (
        triple[0].n3(),
        triple[1].n3(),
        _quoteLiteral(triple[2])
        if isinstance(triple[2], RdfLiteral)
        else triple[2].n3(),
        *((context.n3(),) if context != DATASET_DEFAULT_GRAPH_ID else ()),
    )
    return template % args


workaround_rdflib_serializes_default_graph_id = patch(
    "rdflib.plugins.serializers.nquads._nq_row",
    new=_new_nq_row,
)
workaround_rdflib_serializes_default_graph_id.start()

# Collect and categorize test cases for pytest parameterization
ALL_CASES = load_from_jelly_manifest_cases(FROM_JELLY_MANIFEST)

RDF11_POSITIVE_CASES = [
    pytest.param(case, id=case.id)
    for case in ALL_CASES
    if case.test_type == "positive" and case.category == "rdf11"
]
ALL_POSITIVE_CASES = [
    pytest.param(case, id=case.id) for case in ALL_CASES if case.test_type == "positive"
]
RDF11_NEGATIVE_CASES = [
    pytest.param(case, id=case.id)
    for case in ALL_CASES
    if case.test_type == "negative" and case.category == "rdf11"
]
ALL_NEGATIVE_CASES = [
    pytest.param(case, id=case.id) for case in ALL_CASES if case.test_type == "negative"
]


# RDF11 positive tests using rdflib parser.
# Ensures rdflib correctly parses RDF 1.1 cases.
@needs_jelly_cli
@pytest.mark.parametrize("case", RDF11_POSITIVE_CASES)
def test_rdflib_parses_rdf11_positive(case: FromJellyTestCase) -> None:
    test_id = case.action_path.parent.name
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True, parents=True)
    with case.action_path.open("rb") as input_file:
        for frame_no, graph in enumerate(
            parse_jelly_grouped(
                input_file,
                graph_factory=lambda: Graph(store=OrderedMemory()),
                dataset_factory=lambda: Dataset(store=OrderedMemory()),
            )
        ):
            output_format = "nquads" if isinstance(graph, Dataset) else "ntriples"
            file_extension = ".nq" if isinstance(graph, Dataset) else ".nt"
            output_filename = output_dir / f"out_{frame_no:03}{file_extension}"
            graph.serialize(
                destination=output_filename, encoding="utf-8", format=output_format
            )
            # Validate against reference result
            jelly_validate(
                case.action_path,
                "--compare-ordered",
                "--compare-frame-indices",
                frame_no,
                "--compare-to-rdf-file",
                output_filename,
                hint=f"Test ID: {test_id}, output file: {output_filename}",
            )


# Generic integration positive tests.
# Ensure our generic integration behaves the same as reference.
@needs_jelly_cli
@pytest.mark.parametrize("case", ALL_POSITIVE_CASES)
def test_generic_parses_positive(case: FromJellyTestCase) -> None:
    test_id = case.action_path.parent.name
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True, parents=True)
    with case.action_path.open("rb") as input_file:
        for frame_no, graph in enumerate(generic_parse_jelly_grouped(input_file)):
            file_extension = case.result_paths[0].suffix if case.result_paths else ".nt"
            output_filename = output_dir / f"out_{frame_no:03}{file_extension}"
            serializer = GenericSinkSerializer(graph)
            serializer.serialize(output_filename=output_filename, encoding="utf-8")
            # Validate against reference result
            jelly_validate(
                case.action_path,
                "--compare-ordered",
                "--compare-frame-indices",
                frame_no,
                "--compare-to-rdf-file",
                output_filename,
                hint=f"Test ID: {test_id}, output file: {output_filename}",
            )


# RDF11 negative tests using rdflib parser. These ensure parser fails where it should.
@needs_jelly_cli
@pytest.mark.parametrize("case", RDF11_NEGATIVE_CASES)
def test_rdflib_parsing_fails_rdf11_negative(case: FromJellyTestCase) -> None:
    test_id = case.action_path.parent.name
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True, parents=True)
    dataset = Dataset(store=OrderedMemory())
    # We expect an error for negative cases
    with pytest.raises(Exception, match=".*"):
        dataset.parse(location=str(case.action_path), format="jelly")


# Generic integration negative tests. Same idea: must fail consistently.
@needs_jelly_cli
@pytest.mark.parametrize("case", ALL_NEGATIVE_CASES)
def test_generic_parsing_fails_negative(case: FromJellyTestCase) -> None:
    test_id = case.action_path.parent.name
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True, parents=True)
    with (
        pytest.raises(Exception, match=".*"),
        case.action_path.open("rb") as input_file,
    ):
        list(generic_parse_jelly_grouped(input_file))
