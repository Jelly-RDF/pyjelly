from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from rdflib import Dataset, Graph, Literal, Node
from rdflib.graph import DATASET_DEFAULT_GRAPH_ID
from rdflib.plugins.serializers.nt import _quoteLiteral

from pyjelly.integrations.rdflib.parse import graphs_from_jelly
from tests.meta import (
    RDF_FROM_JELLY_TESTS_DIR,
    TEST_OUTPUTS_DIR,
)
from tests.utils.ordered_memory import OrderedMemory
from tests.utils.rdf_test_cases import (
    PhysicalTypeTestCasesDir,
    id_from_path,
    jelly_validate,
    needs_jelly_cli,
    walk_directories,
)


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
            graphs_from_jelly(input_file, store=OrderedMemory())
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
