from __future__ import annotations

import datetime as dt
import os
import re
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest
from _pytest.nodes import Item
from _pytest.reports import TestReport
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF, XSD

EARL = Namespace("http://www.w3.org/ns/earl#")
DOAP = Namespace("http://usefulinc.com/ns/doap#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")

ASSERTOR = URIRef("#assertor")
IMPL = URIRef("#impl")
DEVELOPER = URIRef("#developer")
_outcome_map = {
    "passed": EARL.passed,
    "failed": EARL.failed,
    "inapplicable": EARL.inapplicable,
}


@dataclass(frozen=True)
class Result:
    """Single test result."""

    name: str
    outcome: str


def get_release_version_date() -> tuple[str, str]:
    """
    Get version and date of the current pyjelly release.

    Returns:
        tag (str): current release version
        date (str): current release date

    """
    git = shutil.which("git")
    if git is None:
        raise RuntimeError
    cp = subprocess.run(  # noqa: S603
        [git, "describe", "--tags", "--abbrev=0"],
        text=True,
        check=True,
        capture_output=True,
    )
    tag = cp.stdout.strip()
    if not re.match(r"^[A-Za-z0-9._-]+$", tag):
        raise ValueError
    date = subprocess.check_output(
        [git, "log", "-1", "--format=%ai", tag], text=True
    ).strip()  # noqa: S603
    return tag.strip("v"), date.split(" ")[0]


class ConformanceReportPlugin:
    """
    Plugin class used during pytest execution for conformance test reporting.

    Pytest Hooks:
        pytest_runtest_makereport: store results data from each test
        pytest_sessionfinish: build RDF graph with all test results
    """

    def __init__(
        self,
        path_to_rdflib_report: str,
        path_to_generic_report: str,
    ) -> None:
        """
        Initialize ConformanceReportPlugin.

        Args:
            path_to_rdflib_report (str): report path for RDFLib integration
            path_to_generic_report (str): report path for pyjelly with Generic API

        """
        self.path_to_rdflib_report = path_to_rdflib_report
        self.path_to_generic_report = path_to_generic_report
        self._results: defaultdict[str, list[Result]] = defaultdict(list)
        self._case_by_id: dict[str, object | None] = defaultdict(list)

    @staticmethod
    def _outcome_from_report(report: pytest.TestReport) -> str:
        """
        Map the report.outcome to string.

        Static method.
        """
        if report.outcome == "skipped":
            return "inapplicable"
        return report.outcome

    @staticmethod
    def _initialize_graph(implementation: str) -> Graph:
        """
        Initialize the graph, depending on implementation.

        Args:
            implementation (str): implementation name

        Returns:
            Graph: the initialized graph

        """
        pyjelly_v, pyjelly_d = get_release_version_date()
        g = Graph()
        g.bind("earl", EARL)
        g.bind("doap", DOAP)
        g.bind("foaf", FOAF)
        g.bind("dc", DCTERMS)
        g.bind("xsd", XSD)

        # add primaryTopic
        g.add((URIRef(""), FOAF.primaryTopic, URIRef("#impl")))
        g.add(
            (
                URIRef(""),
                DCTERMS.issued,
                Literal(
                    dt.datetime.now(dt.timezone.utc).isoformat(), datatype=XSD.dateTime
                ),
            )
        )
        g.add((URIRef(""), FOAF.maker, URIRef("#assertor")))

        # add developer
        g.add((DEVELOPER, RDF.type, FOAF.Group))
        g.add((DEVELOPER, DOAP.name, Literal("pyjelly contributors")))
        g.add((DEVELOPER, DOAP.homepage, URIRef("https://w3id.org/jelly/pyjelly")))

        # add assertor
        g.add((ASSERTOR, RDF.type, EARL.Software))
        g.add((ASSERTOR, RDF.type, EARL.Assertor))
        g.add((ASSERTOR, DOAP.name, Literal("Pyjelly test suite")))
        g.add(
            (
                ASSERTOR,
                DOAP.homepage,
                URIRef(
                    "https://github.com/Jelly-RDF/pyjelly/tree/main/tests/conformance_tests"
                ),
            )
        )

        # add implementation
        g.add((IMPL, RDF.type, DOAP.Project))
        g.add((IMPL, RDF.type, DOAP.TestSubject))
        g.add((IMPL, RDF.type, DOAP.Software))
        g.add((IMPL, DOAP.name, Literal(f"pyjelly ({implementation})")))
        g.add((IMPL, DOAP.homepage, URIRef("https://w3id.org/jelly/pyjelly")))

        if implementation.lower() == "generic":
            description = f"pyjelly ({implementation})"
        elif implementation.lower() == "rdflib":
            description = f"pyjelly integration with {implementation}"
        else:
            description = "implementation not found"

        g.add((IMPL, DOAP.description, Literal(description, lang="en")))
        g.add((IMPL, DOAP.programming_language, Literal("Python")))
        release = BNode()
        g.add((IMPL, DOAP.release, release))
        g.add((release, DOAP.name, Literal(f"pyjelly {pyjelly_v}")))
        g.add((release, DOAP.version, Literal(pyjelly_v)))
        g.add((release, DCTERMS.created, Literal(pyjelly_d, datatype=XSD.date)))
        return g

    @staticmethod
    def _add_test_record_to_graph(
        g: Graph, outcome: str, test_uri: str, assertor: URIRef, impl: URIRef
    ) -> Graph:
        """
        Add one test record to a graph.

        Args:
            g (Graph): Graph to add test record to
            outcome (str): outcome of test
            test_uri (str): URI of test record
            assertor (URIRef): URIRef of assertor
            impl (URIRef): URIRef of implementation

        Returns:
            Graph: Graph with added test record

        Static method.

        """
        test_name = "#" + "_".join(test_uri.split("/")[6:])
        g.add((URIRef(test_name), RDF.type, EARL.Assertion))
        g.add((URIRef(test_name), EARL.assertedBy, assertor))
        g.add((URIRef(test_name), EARL.subject, impl))
        g.add(
            (
                URIRef(test_name),
                EARL.test,
                URIRef(
                    f"https://github.com/Jelly-RDF/jelly-protobuf/tree/main/test/{'/'.join(test_uri.split('/')[6:])}"
                ),
            )
        )
        g.add((URIRef(test_name), EARL.mode, EARL.automatic))
        result = BNode()
        g.add((URIRef(test_name), EARL.result, result))
        g.add((result, RDF.type, EARL.TestResult))
        g.add(
            (
                result,
                DCTERMS.date,
                Literal(
                    dt.datetime.now(dt.timezone.utc).isoformat(), datatype=XSD.dateTime
                ),
            )
        )
        g.add((result, EARL.result, _outcome_map[outcome]))
        return g

    def pytest_runtest_call(self, item: Item) -> None:
        case = None
        if hasattr(item, "callspec"):
            case = item.callspec.params.get("case")
        self._case_by_id[item.nodeid] = case

    def pytest_runtest_logreport(self, report: TestReport) -> None:
        """
        Pytest hook, called after each test.

        Args:
            report (TestReport): single test report

        """
        if report.when != "call":
            return
        node_id = report.nodeid
        case = self._case_by_id.get(node_id)
        outcome = self._outcome_from_report(report)
        test_name = report.nodeid.rsplit("::", 1)[-1]
        uri = getattr(case, "uri", None)
        self._results[cast(str, uri)].append(
            Result(
                name=test_name,
                outcome=outcome,
            )
        )

    def pytest_sessionfinish(self) -> None:
        """
        Pytest hook, called once the session finishes.

        Builds RDF Graph using RDFLib and serializes to Turtle.
        """
        g_generic = self._initialize_graph(implementation="generic")
        g_rdflib = self._initialize_graph(implementation="RDFLib")

        for k, v in self._results.items():
            if len(v) == 1:
                r = v[0]
                if (
                    "test_generic" in r.name
                    or "test_serializes_generic" in r.name
                    or "test_serializing_fails_generic" in r.name
                ):
                    g_generic = self._add_test_record_to_graph(
                        g_generic, r.outcome, k, ASSERTOR, IMPL
                    )
                    g_rdflib = self._add_test_record_to_graph(
                        g_rdflib, "inapplicable", k, ASSERTOR, IMPL
                    )
                elif (
                    "test_rdflib" in r.name
                    or "test_serializes_rdf" in r.name
                    or "test_serializing_fails_rdf" in r.name
                ):
                    g_rdflib = self._add_test_record_to_graph(
                        g_rdflib, r.outcome, k, ASSERTOR, IMPL
                    )
                    g_generic = self._add_test_record_to_graph(
                        g_generic, "inapplicable", k, ASSERTOR, IMPL
                    )

            if len(v) > 1:
                for r in v:
                    if (
                        "test_generic" in r.name
                        or "test_serializes_generic" in r.name
                        or "test_serializing_fails_generic" in r.name
                    ):
                        g_generic = self._add_test_record_to_graph(
                            g_generic, r.outcome, k, ASSERTOR, IMPL
                        )
                    elif (
                        "test_rdflib" in r.name
                        or "test_serializes_rdf" in r.name
                        or "test_serializing_fails_rdf" in r.name
                    ):
                        g_rdflib = self._add_test_record_to_graph(
                            g_rdflib, r.outcome, k, ASSERTOR, IMPL
                        )

        g_rdflib.serialize(self.path_to_rdflib_report, format="turtle")
        g_generic.serialize(self.path_to_generic_report, format="turtle")


def main() -> int:
    os.environ["REPORTING_MODE"] = "1"
    tests_dir = str(Path(__file__).resolve().parent)
    conformance_report_plugin = ConformanceReportPlugin(
        path_to_rdflib_report="pyjelly RDFLib.ttl",
        path_to_generic_report="pyjelly Generic API.ttl",
    )

    return pytest.main(args=[tests_dir], plugins=[conformance_report_plugin])


if __name__ == "__main__":
    raise SystemExit(main())
