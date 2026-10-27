from __future__ import annotations

import datetime as dt
import os
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from collections.abc import Collection
from pathlib import Path

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

GENERIC = "generic"
RDFLIB = "rdflib"
IMPLEMENTATIONS = (GENERIC, RDFLIB)

_outcome_map = {
    "passed": EARL.passed,
    "failed": EARL.failed,
    "inapplicable": EARL.inapplicable,
}

_outcome_priority = ("failed", "passed", "inapplicable")


def implementation_of(node_id: str) -> str | None:
    """
    Indicate which integration is tested by the conformance test.

    Every conformance test says so in its name: `rdflib` for the RDFLib
    integration, `generic` for the Generic API.

    Args:
        node_id (str): pytest node ID of the test

    Returns:
        str | None: the integration, or nothing if the name tells us neither

    """
    name = node_id.rsplit("::", 1)[-1]
    if RDFLIB in name:
        return RDFLIB
    if GENERIC in name:
        return GENERIC
    return None


def combine_outcomes(outcomes: Collection[str]) -> str:
    """
    Sum up what one integration did with one case.

    Args:
        outcomes (Collection[str]): outcomes of the tests that integration ran
            for the case; empty if it ran none

    Returns:
        str: the outcome to report for the case

    """
    for outcome in _outcome_priority:
        if outcome in outcomes:
            return outcome
    return "inapplicable"


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
    date = subprocess.check_output(  # noqa: S603
        [git, "log", "-1", "--format=%ai", tag], text=True
    ).strip()
    return tag.strip("v"), date.split(" ")[0]


class ConformanceReportPlugin:
    """
    Plugin class used during pytest execution for conformance test reporting.

    Pytest Hooks:
        pytest_itemcollected: remember which case each test runs
        pytest_runtest_logreport: store results data from each test
        pytest_sessionfinish: build RDF graph with all test results
    """

    def __init__(
        self,
        path_to_rdflib_report: str,
        path_to_generic_report: str,
        path_to_rdflib_sparql_report: str,
        path_to_generic_sparql_report: str,
    ) -> None:
        """
        Initialize ConformanceReportPlugin.

        Args:
            path_to_rdflib_report (str): report path for RDFLib integration
            path_to_generic_report (str): report path for pyjelly with Generic API
            path_to_rdflib_sparql_report (str): report path of the Jelly-SPARQL tests
                of the RDFLib integration
            path_to_generic_sparql_report (str): report path of the Jelly-SPARQL
                tests of pyjelly with Generic API

        """
        self.path_to_rdflib_report = path_to_rdflib_report
        self.path_to_generic_report = path_to_generic_report
        self.path_to_rdflib_sparql_report = path_to_rdflib_sparql_report
        self.path_to_generic_sparql_report = path_to_generic_sparql_report
        # Case IRI -> integration -> outcomes of that integration's tests.
        self._results: defaultdict[str, defaultdict[str, list[str]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._case_by_id: dict[str, object | None] = {}
        self.unclassified: set[str] = set()

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

        # add assertor
        g.add((ASSERTOR, RDF.type, EARL.Software))
        g.add((ASSERTOR, RDF.type, EARL.Assertor))
        g.add((ASSERTOR, FOAF.name, Literal("pyjelly test suite")))
        g.add(
            (
                ASSERTOR,
                FOAF.homepage,
                URIRef(
                    "https://github.com/Jelly-RDF/pyjelly/tree/main/tests/conformance_tests"
                ),
            )
        )

        g.add((DEVELOPER, RDF.type, FOAF.Group))
        g.add((DEVELOPER, FOAF.name, Literal("pyjelly contributors")))
        g.add((DEVELOPER, FOAF.homepage, URIRef("https://w3id.org/jelly/pyjelly")))

        if implementation.lower() == "generic":
            description = "pyjelly (Generic API)"
            implementation = "pyjelly (Generic API)"
        elif implementation.lower() == "rdflib":
            description = "pyjelly integration with RDFLib"
            implementation = "pyjelly (RDFLib)"
        else:
            description = "implementation not found"
        # add implementation
        g.add((IMPL, RDF.type, DOAP.Project))
        g.add((IMPL, RDF.type, DOAP.TestSubject))
        g.add((IMPL, RDF.type, DOAP.Software))
        g.add((IMPL, DOAP.name, Literal(implementation)))
        g.add((IMPL, DOAP.developer, DEVELOPER))
        g.add((IMPL, DOAP.homepage, URIRef("https://w3id.org/jelly/pyjelly")))
        g.add((IMPL, DOAP.description, Literal(description, lang="en")))
        g.add((IMPL, DOAP["programming-language"], Literal("Python")))
        release = BNode()
        g.add((IMPL, DOAP.release, release))
        g.add((release, DOAP.name, Literal("pyjelly")))
        g.add((release, DOAP.revision, Literal(pyjelly_v)))
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
        g.add((result, EARL.outcome, _outcome_map[outcome]))
        return g

    def pytest_itemcollected(self, item: Item) -> None:
        # Every collected test, so that the ones a mark skips are in here too.
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
        if report.when == "teardown":
            return
        if report.when == "setup" and report.outcome == "passed":
            # The test itself is yet to run. A test that never gets that far,
            # because a mark skips it, is only ever reported here.
            return
        case = self._case_by_id.get(report.nodeid)
        uri = getattr(case, "uri", None)
        if uri is None:
            # Not a conformance case -- a doctest, say.
            return
        implementation = implementation_of(report.nodeid)
        if implementation is None:
            self.unclassified.add(report.nodeid)
            return
        self._results[uri][implementation].append(self._outcome_from_report(report))

    def pytest_sessionfinish(self) -> None:
        """
        Pytest hook, called once the session finishes.

        Builds RDF Graph using RDFLib and serializes to Turtle.
        """
        rdf_graphs = {
            implementation: self._initialize_graph(implementation)
            for implementation in IMPLEMENTATIONS
        }
        sparql_graphs = {
            implementation: self._initialize_graph(implementation)
            for implementation in IMPLEMENTATIONS
        }

        for uri, outcomes in self._results.items():
            # The tests of Jelly-SPARQL have reports of their own
            graphs = sparql_graphs if "/tests/sparql/" in uri else rdf_graphs
            for implementation, g in graphs.items():
                # An integration that ran no test for the case gets an
                # inapplicable record, so that every report covers every case.
                self._add_test_record_to_graph(
                    g,
                    combine_outcomes(outcomes.get(implementation, ())),
                    uri,
                    ASSERTOR,
                    IMPL,
                )

        rdf_graphs[RDFLIB].serialize(self.path_to_rdflib_report, format="turtle")
        rdf_graphs[GENERIC].serialize(self.path_to_generic_report, format="turtle")
        sparql_graphs[RDFLIB].serialize(
            self.path_to_rdflib_sparql_report, format="turtle"
        )
        sparql_graphs[GENERIC].serialize(
            self.path_to_generic_sparql_report, format="turtle"
        )

        if self.unclassified:
            sys.stderr.write(
                "Left out of the reports, because their names say nothing about "
                "which integration they run:\n"
                + "".join(f"  {node_id}\n" for node_id in sorted(self.unclassified))
            )


def main() -> int:
    os.environ["REPORTING_MODE"] = "1"
    tests_dir = str(Path(__file__).resolve().parent)
    conformance_report_plugin = ConformanceReportPlugin(
        path_to_rdflib_report="pyjelly RDFLib.ttl",
        path_to_generic_report="pyjelly Generic API.ttl",
        path_to_rdflib_sparql_report="pyjelly RDFLib SPARQL.ttl",
        path_to_generic_sparql_report="pyjelly Generic API SPARQL.ttl",
    )

    code = int(pytest.main(args=[tests_dir], plugins=[conformance_report_plugin]))
    if conformance_report_plugin.unclassified:
        return max(code, 1)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
