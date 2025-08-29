from rdflib import Graph, Namespace, URIRef

JELLYT = Namespace("https://w3id.org/jelly/dev/tests/vocab#")
MF = Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")


def categorize_by_requires(graph: Graph, test_uri: URIRef) -> str:
    reqs = set(graph.objects(test_uri, MF.requires))
    has_star = JELLYT.requirementRdfStar in reqs
    has_gen = JELLYT.requirementGeneralizedRdf in reqs

    if has_star and has_gen:
        return "rdf_star_generalized"
    if has_star:
        return "rdf_star"
    if has_gen:
        return "generalized"
    return "rdf11"
