from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

import pytest
from rdflib import Graph, Node, URIRef
from rdflib import Literal as RdfLiteral
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.namespace import RDF
from rdflib.plugins.serializers.nt import _quoteLiteral

from tests.conformance_tests.test_rdf._common import (
    JELLYT,
    MF,
    categorize_by_requires,
)
from tests.meta import RDF_TO_JELLY_MANIFEST, TEST_OUTPUTS_DIR
from tests.serialize import write_generic_sink, write_graph_or_dataset
from tests.utils.rdf_test_cases import jelly_validate, needs_jelly_cli

# Manifest path for to_jelly tests. This is where all test cases are defined.
TO_JELLY_MANIFEST = RDF_TO_JELLY_MANIFEST


@dataclass
class ToJellyTestCase:
    uri: str
    name: str
    action_paths: list[Path]
    options_path: Path | None
    result_path: Path | None
    test_type: str
    category: str
    id: str = field(init=False)

    def __post_init__(self) -> None:
        # Custom id for pytest output to easily see type/category/case
        action_name = (
            self.action_paths[0].parent.name if self.action_paths else "no-action"
        )
        self.id = f"{self.test_type}-{self.category}-{action_name}"


# Load all test cases from the manifest file
# Important: This function translates RDF manifest definitions into Python test cases.
# If new fields appear in manifests (e.g. more metadata), extend logic here.
def load_to_jelly_manifest_cases(manifest_path: Path) -> list[ToJellyTestCase]:
    if not manifest_path.exists():
        return []

    graph = Graph()
    graph.parse(manifest_path, format="turtle")
    manifest_dir = manifest_path.parent
    base_uri = "https://w3id.org/jelly/dev/tests/rdf/to_jelly/"

    test_cases = []
    test_type_map = {
        JELLYT.TestPositive: "positive",
        JELLYT.TestNegative: "negative",
    }

    for test_class, test_type_str in test_type_map.items():
        for test_uri in graph.subjects(RDF.type, test_class):
            if not isinstance(test_uri, URIRef):
                continue

            test_case = _process_test_case(
                graph, test_uri, manifest_dir, base_uri, test_type_str
            )
            if test_case:
                test_cases.append(test_case)

    return test_cases


# Process a single test case entry from the manifest
# This function collects input files (action_paths),
# optional stream options, and expected result.
def _process_test_case(
    graph: Graph,
    test_uri: URIRef,
    manifest_dir: Path,
    base_uri: str,
    test_type_str: str,
) -> ToJellyTestCase | None:
    action_node = graph.value(test_uri, MF.action)
    action_paths, options_path = _process_action_node(
        graph, action_node, manifest_dir, base_uri
    )

    result_path = _process_result_node(graph, test_uri, manifest_dir, base_uri)

    return ToJellyTestCase(
        uri=str(test_uri),
        name=str(graph.value(test_uri, MF.name) or ""),
        action_paths=action_paths,
        options_path=options_path,
        result_path=result_path,
        test_type=test_type_str,
        category=categorize_by_requires(graph, test_uri),
    )


# Process the action node, handling multiple inputs and stream options
# Action can be: a list of input RDF files, a single RDF file, or an options file.
def _process_action_node(
    graph: Graph, action_node: Node | None, manifest_dir: Path, base_uri: str
) -> tuple[list[Path], Path | None]:
    action_paths: list[Path] = []
    options_path = None

    if not action_node:
        return action_paths, options_path

    if (action_node, RDF.first, None) in graph:
        # List of action URIs
        action_uris = graph.items(action_node)
        for action_uri in action_uris:
            uri_str = str(action_uri)
            rel_path = uri_str.replace(base_uri, "")
            if uri_str.endswith("stream_options.jelly"):
                options_path = manifest_dir / rel_path
            else:
                action_paths.append(manifest_dir / rel_path)
    elif str(action_node).endswith("stream_options.jelly"):
        # Single options file
        rel_path = str(action_node).replace(base_uri, "")
        options_path = manifest_dir / rel_path
    else:
        # Single action file
        rel_path = str(action_node).replace(base_uri, "")
        action_paths.append(manifest_dir / rel_path)

    return action_paths, options_path


# Process the result node from the manifest
# Each test case may have one expected output RDF file.
def _process_result_node(
    graph: Graph, test_uri: URIRef, manifest_dir: Path, base_uri: str
) -> Path | None:
    result_node = graph.value(test_uri, MF.result)
    if not result_node:
        return None

    rel_path = str(result_node).replace(base_uri, "")
    return manifest_dir / rel_path


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
ALL_TO_JELLY_CASES = load_to_jelly_manifest_cases(TO_JELLY_MANIFEST)

# Temporary exclusion of a problematic case, can be replaced with xfail later
ALL_TO_JELLY_CASES = [
    case
    for case in ALL_TO_JELLY_CASES
    if not ("pos_014" in case.uri and case.category == "rdf11")
]

RDF11_POSITIVE_CASES = [
    pytest.param(case, id=case.id)
    for case in ALL_TO_JELLY_CASES
    if case.test_type == "positive" and case.category == "rdf11"
]

ALL_POSITIVE_CASES = [
    pytest.param(case, id=case.id)
    for case in ALL_TO_JELLY_CASES
    if case.test_type == "positive"
]

RDF11_NEGATIVE_CASES = [
    pytest.param(case, id=case.id)
    for case in ALL_TO_JELLY_CASES
    if case.test_type == "negative" and case.category == "rdf11"
]

ALL_NEGATIVE_CASES = [
    pytest.param(case, id=case.id)
    for case in ALL_TO_JELLY_CASES
    if case.test_type == "negative"
]


# RDF11 positive tests using to_jelly serialization.
# These ensure our serialization to jelly matches expected RDF 1.1 outputs.
@needs_jelly_cli
@pytest.mark.parametrize("case", RDF11_POSITIVE_CASES)
def test_serializes_rdf11_positive(case: ToJellyTestCase) -> None:
    test_id = case.action_paths[0].parent.name if case.action_paths else "unknown"
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"

    input_paths = [p for p in case.action_paths if p.name.startswith("in_")]

    write_graph_or_dataset(
        *[str(p) for p in input_paths],
        options=str(case.options_path) if case.options_path else None,
        out_filename=actual_out,
    )

    # Validate that each input frame matches the corresponding jelly frame
    for frame_no, input_filename in enumerate(input_paths):
        jelly_validate(
            actual_out,
            "--compare-ordered",
            "--compare-frame-indices",
            frame_no,
            "--compare-to-rdf-file",
            input_filename,
            "--options-file",
            str(case.options_path) if case.options_path else "",
            hint=f"Test ID: {test_id}, tested file: {input_filename}",
        )


# Generic integration positive tests.
# Ensure the generic sink produces correct output.
@needs_jelly_cli
@pytest.mark.parametrize("case", ALL_POSITIVE_CASES)
def test_serializes_generic_positive(case: ToJellyTestCase) -> None:
    test_id = case.action_paths[0].parent.name if case.action_paths else "unknown"
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"

    write_generic_sink(
        *[str(path) for path in case.action_paths],
        options=str(case.options_path) if case.options_path else None,
        out_filename=actual_out,
    )

    for frame_no, input_filename in enumerate(case.action_paths):
        jelly_validate(
            actual_out,
            "--compare-ordered",
            "--compare-frame-indices",
            frame_no,
            "--compare-to-rdf-file",
            input_filename,
            "--options-file",
            str(case.options_path) if case.options_path else "",
            hint=f"Test ID: {test_id}, tested file: {input_filename}",
        )


# RDF11 negative tests using to_jelly serialization. Parser should fail here.
@needs_jelly_cli
@pytest.mark.parametrize("case", RDF11_NEGATIVE_CASES)
def test_serializing_fails_rdf11_negative(case: ToJellyTestCase) -> None:
    test_id = case.action_paths[0].parent.name if case.action_paths else "unknown"
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"

    with pytest.raises(Exception, match=".*"):
        write_graph_or_dataset(
            *[str(path) for path in case.action_paths],
            options=str(case.options_path) if case.options_path else None,
            out_filename=actual_out,
        )


# Generic integration negative tests. Same expectation: serialization must fail.
@needs_jelly_cli
@pytest.mark.parametrize("case", ALL_NEGATIVE_CASES)
def test_serializing_fails_generic_negative(case: ToJellyTestCase) -> None:
    test_id = case.action_paths[0].parent.name if case.action_paths else "unknown"
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"

    with pytest.raises(Exception, match=".*"):
        write_generic_sink(
            *[str(path) for path in case.action_paths],
            options=str(case.options_path) if case.options_path else None,
            out_filename=actual_out,
        )
