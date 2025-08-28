from __future__ import annotations

import functools
from dataclasses import dataclass
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


@dataclass
class JellySerializationCase:
    uri: str
    types: list[Any]
    name: str | None
    comment: str | None
    action: list[str]
    result: list[str]
    requires: list[str]
    manifest_path: Path
    manifest_dir: Path | None = None


def parse_manifest(manifest_path: Path) -> list[JellySerializationCase]:
    g = Graph()
    g.parse(manifest_path, format="turtle")
    manifest = next(g.subjects(RDF.type, MF.Manifest), None)
    if manifest is None:
        msg = "No manifest found in the file"
        raise ValueError(msg)

    cases: list[JellySerializationCase] = []
    entries = list(g.objects(manifest, MF.entries))
    if entries:
        for test_case in g.items(entries[0]):
            test_types = list(g.objects(test_case, RDF.type))
            if JELLYT.TestRdfToJelly not in test_types:
                continue

            name_val = g.value(test_case, RDFS.label)
            comment_val = g.value(test_case, RDFS.comment)

            cases.append(
                JellySerializationCase(
                    uri=str(test_case),
                    types=test_types,
                    name=str(name_val) if name_val else None,
                    comment=str(comment_val) if comment_val else None,
                    action=[str(action) for action in g.objects(test_case, MF.action)],
                    result=[str(result) for result in g.objects(test_case, MF.result)],
                    requires=[str(req) for req in g.objects(test_case, MF.requires)],
                    manifest_path=manifest_path,
                )
            )
    return cases


def resolve_manifest_paths(
    action_paths: list[str],
    manifest_dir: Path,
) -> tuple[list[Path], Path | None]:
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


def get_test_cases_from_manifests() -> list[JellySerializationCase]:
    cases: list[JellySerializationCase] = []
    manifest_paths = [
        RDF_TO_JELLY_TESTS_DIR / "PhysicalTypeTestCases" / "manifest.ttl",
        RDF_TO_JELLY_TESTS_DIR / "RDFStarTestCases" / "manifest.ttl",
        RDF_TO_JELLY_TESTS_DIR / "RDFStarGeneralizedTestCases" / "manifest.ttl",
        RDF_TO_JELLY_TESTS_DIR / "GeneralizedTestCases" / "manifest.ttl",
        RDF_TO_JELLY_TESTS_DIR / "to_jelly" / "manifest.ttl",
    ]

    for manifest_path in manifest_paths:
        if manifest_path.exists():
            for case in parse_manifest(manifest_path):
                case.manifest_dir = manifest_path.parent
                cases.append(case)

    return cases


def is_generic_test(test_case: JellySerializationCase) -> bool:
    requirements = test_case.requires or []
    manifest_path: Path = test_case.manifest_path

    generic_requirements = [
        "https://w3id.org/jelly/dev/tests/vocab#requirementGeneralizedRdf",
        "https://w3id.org/jelly/dev/tests/vocab#requirementRdfStar",
    ]

    parts = ["RDFStar", "Generalized", "RDFStarGeneralized"]
    is_from_generic_manifest = any(part in str(manifest_path) for part in parts)

    return any(req in requirements for req in generic_requirements) or (
        is_from_generic_manifest
    )


def run_serialization_test(test_case: JellySerializationCase) -> None:
    test_uri: str = test_case.uri
    test_types = test_case.types
    actions = test_case.action
    manifest_dir = test_case.manifest_dir

    if manifest_dir is None:
        pytest.skip(f"No manifest dir for {test_uri}")

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
                *input_files,
                options=options_file,
                out_filename=actual_out,
            )
        else:
            write_graph_or_dataset(
                *input_files,
                options=options_file,
                out_filename=actual_out,
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


def make_test(tc: JellySerializationCase) -> Callable[[], None]:
    @needs_jelly_cli
    def _test() -> None:
        run_serialization_test(tc)

    return _test


for test_case in test_cases:
    test_id = test_case.uri.split("/")[-1]
    test_name = f"test_to_jelly_{test_id}"
    test_func = make_test(test_case)
    test_func.__name__ = test_name
    globals()[test_name] = test_func
