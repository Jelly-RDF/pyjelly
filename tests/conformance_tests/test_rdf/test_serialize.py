from __future__ import annotations

from pathlib import Path

import pytest

from tests.meta import (
    RDF_TO_JELLY_TESTS_DIR,
    TEST_OUTPUTS_DIR,
)
from tests.serialize import write_graph_or_dataset
from tests.utils.rdf_test_cases import (
    PhysicalTypeTestCasesDir,
    id_from_path,
    jelly_validate,
    needs_jelly_cli,
    walk_directories,
)


@needs_jelly_cli
@walk_directories(
    RDF_TO_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.TRIPLES,
    RDF_TO_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.QUADS,
    RDF_TO_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.GRAPHS,
    glob="pos_*",
)
def test_serializes(path: Path) -> None:
    options_filename = path / "stream_options.jelly"
    input_filenames = tuple(path.glob("in_*"))
    test_id = id_from_path(path)
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"

    write_graph_or_dataset(
        *input_filenames,
        options=options_filename,
        out_filename=actual_out,
    )
    for input_filename in input_filenames:
        jelly_validate(
            actual_out,
            "--compare-ordered",
            "--compare-to-rdf-file",
            input_filename,
            "--options-file",
            options_filename,
            hint=f"Test ID: {test_id}, tested file: {input_filename}",
        )


@needs_jelly_cli
@walk_directories(
    RDF_TO_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.TRIPLES,
    RDF_TO_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.QUADS,
    RDF_TO_JELLY_TESTS_DIR / PhysicalTypeTestCasesDir.GRAPHS,
    glob="neg_*",
)
def test_serializing_fails(path: Path) -> None:
    options_filename = path / "stream_options.jelly"
    test_id = id_from_path(path)
    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"
    with pytest.raises(Exception):  # TODO: more specific  # noqa: PT011,B017,TD002
        write_graph_or_dataset(
            *map(str, path.glob("in_*")),
            options=options_filename,
            out_filename=actual_out,
        )
