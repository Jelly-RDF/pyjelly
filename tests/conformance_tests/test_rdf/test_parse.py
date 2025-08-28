from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from rdflib import Dataset, Graph, Literal, Node, Namespace, URIRef
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.plugins.serializers.nt import _quoteLiteral
from rdflib.namespace import RDF

from pyjelly.integrations.generic.parse import (
    parse_jelly_grouped as generic_parse_jelly_grouped,
)
from pyjelly.integrations.rdflib.parse import parse_jelly_grouped
from tests.meta import (
    RDF_FROM_JELLY_TESTS_DIR,
    TEST_OUTPUTS_DIR,
)
from tests.utils.generic_sink_test_serializer import GenericSinkSerializer
from tests.utils.ordered_memory import OrderedMemory
from tests.utils.rdf_test_cases import (
    GeneralizedTestCasesDir,
    PhysicalTypeTestCasesDir,
    RDFStarGeneralizedTestCasesDir,
    RDFStarTestCasesDir,
    id_from_path,
    jelly_validate,
    needs_jelly_cli,
    walk_directories,
)

# Namespace for manifest parsing
JELLYT = Namespace("https://w3id.org/jelly/dev/tests/vocab#")
MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")


# Manifest parsing utilities
def parse_manifest_test_cases(manifest_path: Path, test_type: str | None = None):
    """Parse test cases from manifest file"""
    graph = Graph()
    with manifest_path.open("rb") as f:
        graph.parse(f, format="turtle", publicID=str(manifest_path))

    manifest_uri = URIRef(manifest_path.as_uri())
    test_cases = []

    entries = graph.objects(manifest_uri, MF.entries)
    for entry_list in entries:
        for test_uri in graph.items(entry_list):
            if not isinstance(test_uri, URIRef):
                continue

            # Check test type
            if test_type == "positive" and (test_uri, RDF.type, JELLYT.TestPositive) not in graph:
                continue
            if test_type == "negative" and (test_uri, RDF.type, JELLYT.TestNegative) not in graph:
                continue

            # Get action (input file)
            action = graph.value(test_uri, MF.action)
            if action and isinstance(action, URIRef):
                test_cases.append({
                    'uri': str(test_uri),
                    'action': action,
                    'name': str(graph.value(test_uri, MF.name) or '')
                })

    return test_cases


# Path to manifest (adjust as needed)
PROTOBUF_SUBMODULE_DIR = Path(__file__).parent.parent.parent.parent / "submodules" / "protobuf"
FROM_JELLY_MANIFEST = PROTOBUF_SUBMODULE_DIR / "test" / "rdf" / "from_jelly" / "manifest.ttl"


def _new_nq_row(triple: tuple[Node, Node, Node], context: Graph) -> str:
    template = "%s " * (3 + (context != DATASET_DEFAULT_GRAPH_ID)) + ".\n"
    args = (
        triple[0].n3(),
        triple[1].n3(),
        _quoteLiteral(triple[2]) if isinstance(triple[2], Literal) else triple[2].n3(),
        *((context.n3(),) if context != DATASET_DEFAULT_GRAPH_ID else ()),
    )
    return template % args


workaround_rdflib_serializes_default_graph_id = patch(
    "rdflib.plugins.serializers.nquads._nq_row",
    new=_new_nq_row,
)

workaround_rdflib_serializes_default_graph_id.start()


# NEW: Test using manifest (only this test is changed)
@needs_jelly_cli
def test_parses_from_manifest() -> None:
    """Test parsing using manifest file instead of directory walking"""
    if not FROM_JELLY_MANIFEST.exists():
        pytest.skip(f"Manifest file not found: {FROM_JELLY_MANIFEST}")

    test_cases = parse_manifest_test_cases(FROM_JELLY_MANIFEST, "positive")

    for test_case in test_cases:
        test_uri = test_case['uri']
        if "triples_rdf_1_1/pos_001" not in test_uri:
            continue  # Only run one specific test for now

        input_uri = test_case['action']
        input_path = Path(str(input_uri).replace("file://", "").lstrip("/"))

        if not input_path.exists():
            pytest.skip(f"Input file not found: {input_path}")

        test_id = test_uri.split("/")[-1]
        output_dir = TEST_OUTPUTS_DIR / test_id
        output_dir.mkdir(exist_ok=True)

        with input_path.open("rb") as input_file:
            for frame_no, graph in enumerate(
                    parse_jelly_grouped(
                        input_file,
                        graph_factory=lambda: Graph(store=OrderedMemory()),
                        dataset_factory=lambda: Dataset(store=OrderedMemory()),
                    )
            ):
                extension = f"n{'quads' if isinstance(graph, Dataset) else 'triples'}"
                output_filename = output_dir / f"out_{frame_no:03}.{extension[:2]}"
                graph.serialize(
                    destination=output_filename, encoding="utf-8", format=extension
                )
                jelly_validate(
                    input_path,
                    "--compare-ordered",
                    "--compare-frame-indices",
                    frame_no,
                    "--compare-to-rdf-file",
                    output_filename,
                    hint=f"Test ID: {test_id}, output file: {output_filename}",
                )


# OLD: Keep all original tests as they were
@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.QUADS,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.GRAPHS,
    glob="pos_*",
)
def test_parses(path: Path) -> None:
    input_filename = path / "in.jelly"
    test_id = id_from_path(path)
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True)
    with input_filename.open("rb") as input_file:
        for frame_no, graph in enumerate(
                parse_jelly_grouped(
                    input_file,
                    graph_factory=lambda: Graph(store=OrderedMemory()),
                    dataset_factory=lambda: Dataset(store=OrderedMemory()),
                )
        ):
            extension = f"n{'quads' if isinstance(graph, Dataset) else 'triples'}"
            output_filename = output_dir / f"out_{frame_no:03}.{extension[:2]}"
            graph.serialize(
                destination=output_filename, encoding="utf-8", format=extension
            )
            jelly_validate(
                input_filename,
                "--compare-ordered",
                "--compare-frame-indices",
                frame_no,
                "--compare-to-rdf-file",
                output_filename,
                hint=f"Test ID: {test_id}, output file: {output_filename}",
            )


@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.QUADS,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.GRAPHS,
    glob="pos_*",
)
def test_1_1_parses(path: Path) -> None:
    run_generic_test(path)


@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / GeneralizedTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / GeneralizedTestCasesDir.QUADS,
    RDF_FROM_JELLY_TESTS_DIR / GeneralizedTestCasesDir.GRAPHS,
    glob="pos_*",
)
def test_generalized_parses(path: Path) -> None:
    run_generic_test(path)


@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / RDFStarTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / RDFStarTestCasesDir.QUADS,
    RDF_FROM_JELLY_TESTS_DIR / RDFStarTestCasesDir.GRAPHS,
    glob="pos_*",
)
def test_rdf_star_parses(path: Path) -> None:
    run_generic_test(path)


@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / RDFStarGeneralizedTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / RDFStarGeneralizedTestCasesDir.QUADS,
    RDF_FROM_JELLY_TESTS_DIR / RDFStarGeneralizedTestCasesDir.GRAPHS,
    glob="pos_*",
)
def test_rdf_star_generalized_parses(path: Path) -> None:
    run_generic_test(path)


@needs_jelly_cli
def run_generic_test(path: Path) -> None:
    input_filename = path / "in.jelly"
    test_id = id_from_path(path)
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True)
    with input_filename.open("rb") as input_file:
        for frame_no, graph in enumerate(generic_parse_jelly_grouped(input_file)):
            extension = f"n{'triples' if 'triples' in test_id else 'quads'}"
            output_filename = output_dir / f"out_{frame_no:03}.{extension[:2]}"
            serializer = GenericSinkSerializer(graph)
            serializer.serialize(output_filename=output_filename, encoding="utf-8")
            jelly_validate(
                input_filename,
                "--compare-ordered",
                "--compare-frame-indices",
                frame_no,
                "--compare-to-rdf-file",
                output_filename,
                hint=f"Test ID: {test_id}, output file: {output_filename}",
            )


@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.GRAPHS,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.QUADS,
    glob="neg_*",
)
def test_parsing_fails(path: Path) -> None:
    input_filename = str(path / "in.jelly")
    test_id = id_from_path(path)
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True)
    dataset = Dataset(store=OrderedMemory())
    with pytest.raises(Exception):  # TODO: more specific  # noqa: PT011, B017, TD002
        dataset.parse(location=input_filename, format="jelly")


@needs_jelly_cli
def run_generic_fail_test(path: Path) -> None:
    input_filename = path / "in.jelly"
    test_id = id_from_path(path)
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True)

    with (
        pytest.raises(Exception),  # TODO: more specific  # noqa: PT011, B017, TD002
        input_filename.open("rb") as input_file,
    ):
        list(generic_parse_jelly_grouped(input_file))


@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / RDFStarTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / RDFStarTestCasesDir.GRAPHS,
    RDF_FROM_JELLY_TESTS_DIR / RDFStarTestCasesDir.QUADS,
    glob="neg_*",
)
def test_parsing_rdf_star_fails(path: Path) -> None:
    run_generic_fail_test(path)


@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.GRAPHS,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.QUADS,
    glob="neg_*",
)
def test_parsing_rdf_1_1_fails(path: Path) -> None:
    run_generic_fail_test(path)


@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / RDFStarGeneralizedTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / RDFStarGeneralizedTestCasesDir.GRAPHS,
    RDF_FROM_JELLY_TESTS_DIR / RDFStarGeneralizedTestCasesDir.QUADS,
    glob="neg_*",
)
def test_parsing_rdf_star_generalized_fails(path: Path) -> None:
    run_generic_fail_test(path)