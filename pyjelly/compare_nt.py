from rdflib import Graph

def compare_nt_files(file1, file2):
    """
    Compare two .nt files to check if they are logically the same.
    """
    g1, g2 = Graph(), Graph()

    # Load RDF triples from both files
    g1.parse(file1, format="nt")
    g2.parse(file2, format="nt")

    # Convert graphs to sets of triples
    triples1 = set(g1)
    triples2 = set(g2)

    # Compare the sets
    if triples1 == triples2:
        print("The two .nt files are logically identical.")
    else:
        print("The .nt files differ.")
        only_in_file1 = triples1 - triples2
        only_in_file2 = triples2 - triples1

        if only_in_file1:
            print("\nTriples only in File 1:")
            for t in only_in_file1:
                print(t)

        if only_in_file2:
            print("\nTriples only in File 2:")
            for t in only_in_file2:
                print(t)

compare_nt_files("./test_files/generated_triples.nt", "output/output.nt")