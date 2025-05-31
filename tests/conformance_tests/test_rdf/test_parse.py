from __future__ import annotations

from functools import partialmethod
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from rdflib import Dataset, Graph

from tests.meta import (
    RDF_FROM_JELLY_TESTS_DIR,
    TEST_OUTPUTS_DIR,
)
from tests.utils.rdf_test_cases import (
    PhysicalTypeTestCasesDir,
    id_from_path,
    jelly_validate,
    needs_jelly_cli,
    walk_directories,
)


def gather(self: Any, graphs: list[Graph]) -> None:
    """Frame collection for the test."""
    graphs.append(self.graph)
    self.graph = Graph()  # reset for next frame


@needs_jelly_cli
@walk_directories(
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.TRIPLES,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.GRAPHS,
    RDF_FROM_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.QUADS,
    glob="pos_*",
)
def test_parses(path: Path) -> None:
    input_filename = str(path / "in.jelly")
    test_id = id_from_path(path)
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True)
    dataset = Dataset()
    frames_as_graphs: list[Graph] = []
    with patch(
        "pyjelly.integrations.rdflib.parse.RDFLibTriplesAdapter.frame",
        partialmethod(gather, graphs=frames_as_graphs),
    ):
        dataset.parse(location=input_filename, format="jelly")
    for frame_no, graph in enumerate(frames_as_graphs):
        frame_no_str = str(frame_no + 1).zfill(3)
        output_filename = output_dir / f"out_{frame_no_str}.nt"
        graph.serialize(destination=output_filename, encoding="utf-8", format="nt")
        jelly_validate(
            input_filename,
            "--compare-ordered",
            "--compare-to-rdf-file",
            output_filename,
            "--compare-frame-indices",
            frame_no,
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
    dataset = Dataset()
    with pytest.raises(Exception):  # TODO: more specific  # noqa: PT011, B017, TD002
        dataset.parse(location=input_filename, format="jelly")
