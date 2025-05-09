from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.conformance_tests.test_rdf.constants import (
    RDF_TO_JELLY_TESTS_DIR,
    TEST_OUTPUTS_DIR,
)
from tests.serialize import write_graph, write_graph_or_dataset
from tests.utils.rdf_test_cases import (
    id_from_path,
    jelly_validate,
    needs_jelly_cli,
    negative_test_cases_for,
    positive_test_cases_for,
)


@needs_jelly_cli
@positive_test_cases_for(RDF_TO_JELLY_TESTS_DIR)
def test_positive(path: Path) -> None:
    options_filename = path / "stream_options.jelly"
    input_filenames = tuple(path.glob("in_*"))
    test_id = id_from_path(path)
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"

    for input_filename in input_filenames:
        write_graph(
            input_filename,
            options_from=options_filename,
            out_filename=actual_out,
            one_frame=True,
        )
        jelly_validate(
            actual_out,
            "--compare-ordered",
            "--compare-to-rdf-file",
            input_filename,
            "--options-file",
            options_filename,
        )


@needs_jelly_cli
@negative_test_cases_for(RDF_TO_JELLY_TESTS_DIR)
def test_negative(path: Path) -> None:
    options_filename = path / "stream_options.jelly"
    test_id = id_from_path(path)
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"
    with pytest.raises(Exception):  # TODO: more specific  # noqa: PT011,B017,TD002
        write_graph_or_dataset(
            *map(str, path.glob("in_*")),
            options_from=options_filename,
            out_filename=actual_out,
        )
