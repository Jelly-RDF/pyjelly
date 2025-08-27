from __future__ import annotations

import functools
from pathlib import Path
from typing import Any, Callable

import pytest
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS

from tests.meta import RDF_TO_JELLY_TESTS_DIR, TEST_OUTPUTS_DIR
from tests.serialize import write_generic_sink, write_graph_or_dataset
from tests.utils.rdf_test_cases import jelly_validate, needs_jelly_cli

MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
JELLYT = Namespace("https://w3id.org/jelly/dev/tests/vocab#")
RDFT = Namespace("http://www.w3.org/ns/rdftest#")


def parse_manifest(manifest_path: Path) -> list[dict[str, Any]]:
    """Parse a test manifest and return list of test cases."""
    g = Graph()
    g.parse(manifest_path, format="turtle")

    test_cases: list[dict[str, Any]] = []
    manifest = None
    for s in g.subjects(RDF.type, MF.Manifest):
        manifest = s
        break

    if manifest is None:
        msg = "No manifest found in the file"
        raise ValueError(msg)

    entries = list(g.objects(manifest, MF.entries))
    if entries:
        entries_list = list(g.items(entries[0]))
        for test_case in entries_list:
            test_types = list(g.objects(test_case, RDF.type))
            if JELLYT.TestRdfToJelly not in test_types:
                continue
            test_info: dict[str, Any] = {
                "uri": str(test_case),
                "type": test_types,
                "name": (
                    str(g.value(test_case, RDFS.label))
                    if g.value(test_case, RDFS.label)
                    else None
                ),
                "comment": (
                    str(g.value(test_case, RDFS.comment))
                    if g.value(test_case, RDFS.comment)
                    else None
                ),
                "action": [str(action) for action in g.objects(test_case, MF.action)],
                "result": [str(result) for result in g.objects(test_case, MF.result)],
                "requires": [str(req) for req in g.objects(test_case, MF.requires)],
                "manifest_path": manifest_path,
            }
            test_cases.append(test_info)

    return test_cases


def resolve_manifest_paths(
    action_paths: list[str], manifest_dir: Path
) -> tuple[list[Path], Path | None]:
    """Resolve relative paths from manifest actions to absolute paths."""
    input_files: list[Path] = []
    options_file: Path | None = None

    for action_path in action_paths:
        rel_path = action_path.removeprefix("file://")
        action_full_path = (manifest_dir / rel_path).resolve()

        if action_full_path.suffix in (
            ".nt",
            ".nq",
            ".ttl",
            ".trig",
            ".n3",
            ".rdf",
            ".xml",
        ):
            input_files.append(action_full_path)
        elif "stream_options.jelly" in action_full_path.name:
            options_file = action_full_path

    return input_files, options_file


def get_test_cases_from_manifests() -> list[dict[str, Any]]:
    """Get test cases from all relevant manifests to maintain full coverage."""
    test_cases: list[dict[str, Any]] = []
    manifest_paths = [
        RDF_TO_JELLY_TESTS_DIR / "PhysicalTypeTestCases" / "manifest.ttl",
        RDF_TO_JELLY_TESTS_DIR / "RDFStarTestCases" / "manifest.ttl",
        RDF_TO_JELLY_TESTS_DIR / "RDFStarGeneralizedTestCases" / "manifest.ttl",
        RDF_TO_JELLY_TESTS_DIR / "GeneralizedTestCases" / "manifest.ttl",
        RDF_TO_JELLY_TESTS_DIR / "to_jelly" / "manifest.ttl",
    ]

    for manifest_path in manifest_paths:
        if manifest_path.exists():
            manifest_cases = parse_manifest(manifest_path)
            for case in manifest_cases:
                case["manifest_dir"] = manifest_path.parent
                test_cases.append(case)

    return test_cases


def is_generic_test(test_case: dict[str, Any]) -> bool:
    """Check if this is a generic test based on requirements and manifest location."""
    requirements = test_case.get("requires", [])
    manifest_path: Path = test_case.get("manifest_path", Path())

    generic_requirements = [
        "https://w3id.org/jelly/dev/tests/vocab#requirementGeneralizedRdf",
        "https://w3id.org/jelly/dev/tests/vocab#requirementRdfStar",
    ]

    is_from_generic_manifest = any(
        part in str(manifest_path)
        for part in ["RDFStar", "Generalized", "RDFStarGeneralized"]
    )

    return any(req in requirements for req in generic_requirements) or (
        is_from_generic_manifest
    )


def run_serialization_test(test_case: dict[str, Any]) -> None:
    """Run a single serialization test case from manifest."""
    test_uri: str = test_case["uri"]
    test_types = test_case["type"]
    actions = test_case["action"]
    manifest_dir: Path = test_case["manifest_dir"]

    test_id = test_uri.split("/")[-1]
    is_positive = JELLYT.TestPositive in test_types
    is_negative = JELLYT.TestNegative in test_types

    if not (is_positive or is_negative):
        pytest.skip(f"Test {test_id} is neither positive nor negative")

    input_files, options_file = resolve_manifest_paths(actions, manifest_dir)

    if not input_files:
        pytest.skip(f"No input files found for test {test_id}")

    actual_out = TEST_OUTPUTS_DIR / f"{test_id}.jelly"
    use_generic = is_generic_test(test_case)

    if is_positive:
        if use_generic:
            write_generic_sink(
                *input_files, options=options_file, out_filename=actual_out
            )
        else:
            write_graph_or_dataset(
                *input_files, options=options_file, out_filename=actual_out
            )

        for frame_no, input_filename in enumerate(input_files):
            validation_args: list[Any] = [
                actual_out,
                "--compare-ordered",
                "--compare-frame-indices",
                frame_no,
                "--compare-to-rdf-file",
                input_filename,
            ]
            if options_file:
                validation_args.extend(["--options-file", options_file])

            jelly_validate(
                *validation_args,
                hint=(f"Test ID: {test_id}, frame {frame_no}, file: {input_filename}"),
            )

    elif is_negative:
        call: Callable[[], None]
        if use_generic:
            call = functools.partial(
                write_generic_sink,
                *input_files,
                options=options_file,
                out_filename=actual_out,
            )
        else:
            call = functools.partial(
                write_graph_or_dataset,
                *input_files,
                options=options_file,
                out_filename=actual_out,
            )
        with pytest.raises(BaseException, match="."):
            call()


test_cases = get_test_cases_from_manifests()


def make_test(tc: dict[str, Any]) -> Callable[[], None]:
    @needs_jelly_cli
    def _test() -> None:
        run_serialization_test(tc)

    return _test


for test_case in test_cases:
    test_id = test_case["uri"].split("/")[-1]
    test_name = f"test_to_jelly_{test_id}"
    test_func = make_test(test_case)
    test_func.__name__ = test_name
    globals()[test_name] = test_func
