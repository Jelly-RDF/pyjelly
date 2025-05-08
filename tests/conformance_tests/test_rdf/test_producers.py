from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.conformance_tests.test_rdf.constants import (
    RDF_TO_JELLY_TESTS_DIR,
    TEST_OUTPUTS_DIR,
)
from tests.serialize import write_graph_or_dataset
from tests.utils.rdf_test_cases import (
    id_from_path,
    jelly_validate,
    needs_jelly_cli,
    negative_test_cases_for,
    positive_test_cases_for,
)


@dataclass
class ProducersTestCase:
    input_filenames: Sequence[Path]
    options_filename: Path
    output_filename: Path | None = None

    def __post_init__(self) -> None:
        assert any(self.input_filenames)
        assert self.options_filename.is_file()
        if self.output_filename is not None:  # none for negative tests
            assert self.output_filename.is_file()


@needs_jelly_cli
@positive_test_cases_for(RDF_TO_JELLY_TESTS_DIR)
def test_positive(path: Path) -> None:
    options_filename = path / "stream_options.jelly"
    expected_out = path / "out.jelly"
    case = ProducersTestCase(
        input_filenames=tuple(path.glob("in_*")),
        options_filename=options_filename,
        output_filename=expected_out,
    )
    test_id = id_from_path(path)
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"
    write_graph_or_dataset(
        *case.input_filenames,
        options_from=options_filename,
        out_filename=actual_out,
    )
    jelly_validate(
        expected_out,
        actual_out,
        "--compare-ordered",
        "--options-file",
        options_filename,
    )


@needs_jelly_cli
@negative_test_cases_for(RDF_TO_JELLY_TESTS_DIR)
def test_negative(path: Path) -> None:
    options_filename = path / "stream_options.jelly"
    case = ProducersTestCase(
        input_filenames=tuple(path.glob("in_*")),
        options_filename=options_filename,
        output_filename=None,
    )
    test_id = id_from_path(path)
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"
    with pytest.raises(Exception):  # TODO: more specific  # noqa: PT011,B017,TD002
        write_graph_or_dataset(
            *map(str, case.input_filenames),
            options_from=options_filename,
            out_filename=actual_out,
        )
