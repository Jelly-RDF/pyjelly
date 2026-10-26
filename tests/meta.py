from pathlib import Path

TESTS_DIR = Path(__file__).parent
CONFORMANCE_TESTS_DIR = TESTS_DIR / "conformance_tests"
E2E_TEST_CASES_DIR = TESTS_DIR / "e2e_test_cases"

TEST_OUTPUTS_DIR = TESTS_DIR / "out"

RDF_TESTS_DIR = CONFORMANCE_TESTS_DIR / "rdf"
# The test suites of Jelly-RDF 1.1 (row layout) and 1.2 (column layout)
RDF_TEST_SUITES = ("v1_1", "v1_2")

RDF_FROM_JELLY_MANIFESTS = [
    RDF_TESTS_DIR / suite / "from_jelly" / "manifest.ttl" for suite in RDF_TEST_SUITES
]
RDF_TO_JELLY_MANIFESTS = [
    RDF_TESTS_DIR / suite / "to_jelly" / "manifest.ttl" for suite in RDF_TEST_SUITES
]
