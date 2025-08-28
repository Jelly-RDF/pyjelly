from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from unittest.mock import patch

import pytest
from rdflib import Graph, Literal as RdfLiteral, Node, Namespace, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.plugins.serializers.nt import _quoteLiteral
from rdflib.namespace import RDF

from tests.meta import TEST_OUTPUTS_DIR
from tests.serialize import write_generic_sink, write_graph_or_dataset
from tests.utils.rdf_test_cases import jelly_validate, needs_jelly_cli

JELLYT = Namespace("https://w3id.org/jelly/dev/tests/vocab#")
MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
PROTOBUF_SUBMODULE_DIR = Path(__file__).parent.parent.parent.parent / "submodules" / "protobuf"
TO_JELLY_MANIFEST = PROTOBUF_SUBMODULE_DIR / "test" / "rdf" / "to_jelly" / "manifest.ttl"


@dataclass
class ToJellyTestCase:
    uri: str
    name: str
    action_paths: List[Path]
    options_path: Optional[Path]
    result_path: Optional[Path]
    test_type: str
    category: str
    id: str = field(init=False)

    def __post_init__(self):
        self.id = f"{self.test_type}-{self.category}-{self.action_paths[0].parent.name if self.action_paths else 'no-action'}"


def categorize_test(uri: str) -> str:
    if "rdf_star_generalized" in uri:
        return "rdf_star_generalized"
    if "rdf_star" in uri:
        return "rdf_star"
    if "generalized" in uri:
        return "generalized"
    return "physical"


def load_to_jelly_manifest_cases(manifest_path: Path) -> list[ToJellyTestCase]:
    if not manifest_path.exists():
        return []

    graph = Graph()
    graph.parse(manifest_path, format="turtle")
    manifest_dir = manifest_path.parent
    BASE_URI_FROM_MANIFEST = "https://w3id.org/jelly/dev/tests/rdf/to_jelly/"

    test_cases = []
    test_type_map = {
        JELLYT.TestPositive: "positive",
        JELLYT.TestNegative: "negative",
    }

    for test_class, test_type_str in test_type_map.items():
        for test_uri in graph.subjects(RDF.type, test_class):
            if not isinstance(test_uri, URIRef):
                continue

            action_node = graph.value(test_uri, MF.action)
            action_paths = []
            options_path = None

            if action_node:
                if (action_node, RDF.first, None) in graph:
                    action_uris = graph.items(action_node)
                    for action_uri in action_uris:
                        if str(action_uri).endswith("stream_options.jelly"):
                            options_path = manifest_dir / str(action_uri).replace(BASE_URI_FROM_MANIFEST, "")
                        else:
                            action_rel_path = str(action_uri).replace(BASE_URI_FROM_MANIFEST, "")
                            action_paths.append(manifest_dir / action_rel_path)
                else:
                    if str(action_node).endswith("stream_options.jelly"):
                        options_path = manifest_dir / str(action_node).replace(BASE_URI_FROM_MANIFEST, "")
                    else:
                        action_rel_path = str(action_node).replace(BASE_URI_FROM_MANIFEST, "")
                        action_paths.append(manifest_dir / action_rel_path)

            result_path = None
            result_node = graph.value(test_uri, MF.result)
            if result_node:
                result_rel_path = str(result_node).replace(BASE_URI_FROM_MANIFEST, "")
                result_path = manifest_dir / result_rel_path

            test_cases.append(ToJellyTestCase(
                uri=str(test_uri),
                name=str(graph.value(test_uri, MF.name) or ''),
                action_paths=action_paths,
                options_path=options_path,
                result_path=result_path,
                test_type=test_type_str,
                category=categorize_test(str(test_uri)),
            ))

    return test_cases


def _new_nq_row(triple: tuple[Node, Node, Node], context: Graph) -> str:
    template = "%s " * (3 + (context != DATASET_DEFAULT_GRAPH_ID)) + ".\n"
    args = (
        triple[0].n3(),
        triple[1].n3(),
        _quoteLiteral(triple[2]) if isinstance(triple[2], RdfLiteral) else triple[2].n3(),
        *((context.n3(),) if context != DATASET_DEFAULT_GRAPH_ID else ()),
    )
    return template % args


workaround_rdflib_serializes_default_graph_id = patch(
    "rdflib.plugins.serializers.nquads._nq_row",
    new=_new_nq_row,
)
workaround_rdflib_serializes_default_graph_id.start()

ALL_TO_JELLY_CASES = load_to_jelly_manifest_cases(TO_JELLY_MANIFEST)

PHYSICAL_POSITIVE_CASES = [
    pytest.param(case, id=case.id) for case in ALL_TO_JELLY_CASES
    if case.test_type == 'positive' and case.category == 'physical'
]

GENERIC_POSITIVE_CASES = [
    pytest.param(case, id=case.id) for case in ALL_TO_JELLY_CASES
    if case.test_type == 'positive'
]

PHYSICAL_NEGATIVE_CASES = [
    pytest.param(case, id=case.id) for case in ALL_TO_JELLY_CASES
    if case.test_type == 'negative' and case.category == 'physical'
]

GENERIC_NEGATIVE_CASES = [
    pytest.param(case, id=case.id) for case in ALL_TO_JELLY_CASES
    if case.test_type == 'negative'
]


@needs_jelly_cli
@pytest.mark.parametrize("case", PHYSICAL_POSITIVE_CASES)
def test_serializes_physical_positive(case: ToJellyTestCase) -> None:
    test_id = case.action_paths[0].parent.name if case.action_paths else "unknown"
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"

    write_graph_or_dataset(
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
@pytest.mark.parametrize("case", GENERIC_POSITIVE_CASES)
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
@pytest.mark.parametrize("case", PHYSICAL_NEGATIVE_CASES)
def test_serializing_fails_physical_negative(case: ToJellyTestCase) -> None:
    test_id = case.action_paths[0].parent.name if case.action_paths else "unknown"
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"

    with pytest.raises(Exception):
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

    with pytest.raises(Exception):
        write_generic_sink(
            *[str(path) for path in case.action_paths],
            options=str(case.options_path) if case.options_path else None,
            out_filename=actual_out,
        )