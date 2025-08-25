from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
from rdflib import RDF, Dataset, Graph, Literal
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.namespace import Namespace
from rdflib.plugins.serializers.nt import _quoteLiteral
from rdflib.term import Node

from pyjelly.integrations.rdflib.parse import parse_jelly_grouped
from tests.meta import (
    RDF_FROM_JELLY_TESTS_DIR,
    TEST_OUTPUTS_DIR,
)
from tests.utils.ordered_memory import OrderedMemory
from tests.utils.rdf_test_cases import (
    jelly_validate,
    needs_jelly_cli,
)

MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
JELLYT = Namespace("https://w3id.org/jelly/dev/tests/vocab#")
RDFT = Namespace("http://www.w3.org/ns/rdftest#")


def _load_manifest_tests(manifest_path: Path) -> list[dict[str, Any]]:
    graph = Graph()
    graph.parse(manifest_path, format="turtle")

    tests = []
    for s in graph.subjects(RDF.type, MF.Manifest):
        entries_list = graph.value(s, MF.entries)
        if entries_list is None:
            continue
        for test_entry in graph.items(entries_list):
            test_type_pos = (test_entry, RDF.type, JELLYT.TestPositive) in graph
            test_type_neg = (test_entry, RDF.type, JELLYT.TestNegative) in graph
            test_action = graph.value(test_entry, MF.action)
            test_name_node = graph.value(test_entry, MF.name)
            test_name = (
                str(test_name_node)
                if test_name_node is not None
                else f"Test from {manifest_path.name}"
            )

            result_list = graph.value(test_entry, MF.result)
            expected_results: list[Node] = []
            if result_list:
                expected_results.extend(graph.items(result_list))

            test_entry_str = str(test_entry)
            test_id_parts = test_entry_str.split("/")
            test_id = (
                f"{manifest_path.parent.name}_{test_id_parts[-2]}_{test_id_parts[-1]}"
            )

            tests.append(
                {
                    "id": test_id,
                    "name": str(test_name),
                    "action": cast(Node, test_action),
                    "expected_results": expected_results,
                    "is_positive": test_type_pos,
                    "is_negative": test_type_neg,
                    "manifest_path": manifest_path,
                }
            )
    return tests


def _get_full_path(manifest_path: Path, relative_path_str: str) -> Path:
    if isinstance(relative_path_str, str) and relative_path_str.startswith("http"):
        from urllib.parse import urlparse

        parsed = urlparse(relative_path_str)
        relative_path = Path(parsed.path.split("/")[-1])
    else:
        relative_path = Path(str(relative_path_str))

    for subdir in manifest_path.parent.rglob("*"):
        if subdir.is_dir() and (subdir / relative_path).exists():
            return subdir / relative_path

    return (manifest_path.parent / relative_path).resolve()


def _collect_all_manifest_tests() -> list[dict[str, Any]]:
    all_tests = []
    manifests = [
        RDF_FROM_JELLY_TESTS_DIR / "manifest.ttl",
    ]

    for manifest_path in manifests:
        if manifest_path.exists():
            all_tests.extend(_load_manifest_tests(manifest_path))
    return all_tests


_all_manifest_test_cases = _collect_all_manifest_tests()
_test_ids = [test_case["id"] for test_case in _all_manifest_test_cases]


def _new_nq_row(triple: tuple[Any, Any, Any], context: Any) -> str:
    has_graph = context is not None and context.identifier != DATASET_DEFAULT_GRAPH_ID
    template = "%s " * (3 + has_graph) + ".\n"
    literal_val = (
        _quoteLiteral(triple[2]) if isinstance(triple[2], Literal) else triple[2].n3()
    )
    graph_part = (context.identifier.n3(),) if has_graph else ()
    args = (
        triple[0].n3(),
        triple[1].n3(),
        literal_val,
        *graph_part,
    )
    return template % args


workaround_rdflib_serializes_default_graph_id = patch(
    "rdflib.plugins.serializers.nquads._nq_row",
    new=_new_nq_row,
)
workaround_rdflib_serializes_default_graph_id.start()


@needs_jelly_cli
@pytest.mark.parametrize("test_case", _all_manifest_test_cases, ids=_test_ids)
def test_parses_from_manifest(test_case: dict[str, Any]) -> None:
    if not test_case["is_positive"]:
        pytest.skip("Negative test case, handled in another test function")

    input_path = _get_full_path(test_case["manifest_path"], test_case["action"])
    test_id = test_case["id"]
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(parents=True, exist_ok=True)

    with input_path.open("rb") as input_file:
        frames = list(
            parse_jelly_grouped(
                input_file,
                graph_factory=lambda: Graph(store=OrderedMemory()),
                dataset_factory=lambda: Dataset(store=OrderedMemory()),
            )
        )

    for frame_no, graph in enumerate(frames):
        extension = "nquads" if isinstance(graph, Dataset) else "ntriples"
        output_filename = output_dir / f"out_{frame_no:03}.{extension[:2]}"
        graph.serialize(destination=output_filename, encoding="utf-8", format=extension)

        expected_results = test_case["expected_results"]
        expected_result_paths = [
            _get_full_path(test_case["manifest_path"], res) for res in expected_results
        ]
        if frame_no < len(expected_result_paths):
            expected_result_path = expected_result_paths[frame_no]
            jelly_validate(
                input_path,
                "--compare-ordered",
                "--compare-frame-indices",
                frame_no,
                "--compare-to-rdf-file",
                expected_result_path,
                hint=f"Test ID: {test_id}, output file: {output_filename}",
            )


@needs_jelly_cli
@pytest.mark.parametrize("test_case", _all_manifest_test_cases, ids=_test_ids)
def test_parsing_fails_from_manifest(test_case: dict[str, Any]) -> None:
    if not test_case["is_negative"]:
        pytest.skip("Positive test case, handled in another test function")

    input_path = _get_full_path(test_case["manifest_path"], test_case["action"])
    test_id = test_case["id"]
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset = Dataset(store=OrderedMemory())
    with pytest.raises((ValueError, SyntaxError), match=r".*"):
        dataset.parse(location=str(input_path), format="jelly")
