from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import partialmethod
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from rdflib import Dataset, Graph

from tests.conformance_tests.test_rdf.constants import (
    RDF_FROM_JELLY_TESTS_DIR,
    TEST_OUTPUTS_DIR,
)
from tests.utils.ordered_memory import OrderedMemory
from tests.utils.rdf_test_cases import (
    id_from_path,
    jelly_validate,
    needs_jelly_cli,
    negative_test_cases_for,
    positive_test_cases_for,
)


@dataclass
class ConsumersTestCase:
    input_filename: Path
    out_filenames: Sequence[Path] | None = None  # none for negative

    def __post_init__(self) -> None:
        assert self.input_filename.is_file()
        if self.out_filenames is not None:
            assert any(self.out_filenames)


def gather(self: Any, lst: list[Graph]) -> None:
    """Frame collection for the test."""
    lst.append(self.graph)
    self.graph = Graph()  # reset for next frame


@needs_jelly_cli
@positive_test_cases_for(RDF_FROM_JELLY_TESTS_DIR)
def test_positive(path: Path) -> None:
    case = ConsumersTestCase(
        input_filename=path / "in.jelly",
        out_filenames=tuple(path.glob("out_*")),
    )
    input_filename = str(case.input_filename)
    test_id = id_from_path(path)
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True)
    dataset = Graph(store=OrderedMemory())
    frames_as_graphs: list[Graph] = []
    with patch(
        "pyjelly.integrations.rdflib.parser.RDFLibTriplesAdapter.frame",
        partialmethod(gather, lst=frames_as_graphs),
    ):
        dataset.parse(location=input_filename, format="jelly")
    for frame_no, graph in enumerate(frames_as_graphs):
        frame_no_str = str(frame_no + 1).zfill(3)
        output_filename = output_dir / f"out_{frame_no_str}.nt"
        graph.serialize(destination=output_filename, encoding="utf-8", format="nt")
        jelly_validate(
            case.input_filename,
            "--compare-to-rdf-file",
            output_filename,
            "--compare-frame-indices",
            frame_no,
        )


@negative_test_cases_for(RDF_FROM_JELLY_TESTS_DIR)
def test_negative(path: Path) -> None:
    case = ConsumersTestCase(
        input_filename=path / "in.jelly",
        out_filenames=None,
    )
    input_filename = str(case.input_filename)
    test_id = id_from_path(path)
    output_dir = TEST_OUTPUTS_DIR / test_id
    output_dir.mkdir(exist_ok=True)
    dataset = Dataset()
    with pytest.raises(Exception):  # TODO: more specific  # noqa: PT011, B017, TD002
        dataset.parse(location=input_filename, format="jelly")
