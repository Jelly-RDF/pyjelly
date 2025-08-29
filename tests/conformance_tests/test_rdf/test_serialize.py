from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

import pytest
from rdflib import Graph, Namespace, Node, URIRef
from rdflib import Literal as RdfLiteral
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.namespace import RDF
from rdflib.plugins.serializers.nt import _quoteLiteral

from tests.meta import RDF_TO_JELLY_MANIFEST, TEST_OUTPUTS_DIR
from tests.serialize import write_generic_sink, write_graph_or_dataset
from tests.utils.rdf_test_cases import jelly_validate, needs_jelly_cli

JELLYT = Namespace("https://w3id.org/jelly/dev/tests/vocab#")
MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
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
        action_name = (
            self.action_paths[0].parent.name if self.action_paths else "no-action"
        )
        self.id = f"{self.test_type}-{self.category}-{action_name}"


def categorize_by_requires(graph: Graph, test_uri: URIRef) -> str:
    reqs = set(graph.objects(test_uri, MF.requires))
    has_star = JELLYT.requirementRdfStar in reqs
    has_gen = JELLYT.requirementGeneralizedRdf in reqs
    if has_star and has_gen:
        return "rdf_star_generalized"
    if has_star:
        return "rdf_star"
    if has_gen:
        return "generalized"
    return "rdf11"


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


def _process_action_node(
    graph: Graph, action_node: Node | None, manifest_dir: Path, base_uri: str
) -> tuple[list[Path], Path | None]:
    action_paths: list[Path] = []
    options_path = None

    if not action_node:
        return action_paths, options_path

    if (action_node, RDF.first, None) in graph:
        action_uris = graph.items(action_node)
        for action_uri in action_uris:
            uri_str = str(action_uri)
            rel_path = uri_str.replace(base_uri, "")
            if uri_str.endswith("stream_options.jelly"):
                options_path = manifest_dir / rel_path
            else:
                action_paths.append(manifest_dir / rel_path)
    elif str(action_node).endswith("stream_options.jelly"):
        rel_path = str(action_node).replace(base_uri, "")
        options_path = manifest_dir / rel_path
    else:
        rel_path = str(action_node).replace(base_uri, "")
        action_paths.append(manifest_dir / rel_path)

    return action_paths, options_path


def _process_result_node(
    graph: Graph, test_uri: URIRef, manifest_dir: Path, base_uri: str
) -> Path | None:
    result_node = graph.value(test_uri, MF.result)
    if not result_node:
        return None

    rel_path = str(result_node).replace(base_uri, "")
    return manifest_dir / rel_path


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

ALL_TO_JELLY_CASES = load_to_jelly_manifest_cases(TO_JELLY_MANIFEST)

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

GENERIC_NEGATIVE_CASES = [
    pytest.param(case, id=case.id)
    for case in ALL_TO_JELLY_CASES
    if case.test_type == "negative"
]


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


@needs_jelly_cli
@pytest.mark.parametrize("case", GENERIC_NEGATIVE_CASES)
def test_serializing_fails_generic_negative(case: ToJellyTestCase) -> None:
    test_id = case.action_paths[0].parent.name if case.action_paths else "unknown"
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"

    with pytest.raises(Exception, match=".*"):
        write_generic_sink(
            *[str(path) for path in case.action_paths],
            options=str(case.options_path) if case.options_path else None,
            out_filename=actual_out,
        )
