import rdf_pb2
from rdflib import URIRef, BNode, Literal, Graph, Namespace


def process_options(options):
    return options.options.max_prefix_table_size != 0


def parse_rdf_stream(filename):
    rdf_stream_frame = rdf_pb2.RdfStreamFrame()
    with open(filename, "rb") as f:
        rdf_stream_frame.ParseFromString(f.read())
    return rdf_stream_frame


def resolve_term(triple, term, prefix_lookup, name_lookup, datatype_lookup,
                 last_prefix_get, last_name_get, last_value):
    
    term_key = term[0]
    bnode_value = getattr(triple, f"{term_key}_bnode")
    if bnode_value:
        return BNode(value=bnode_value), last_prefix_get, last_name_get

    literal = getattr(triple, f"{term_key}_literal")
    if literal.lex:
        if literal.langtag:
            return Literal(literal.lex, lang=literal.langtag), last_prefix_get, last_name_get
        elif literal.datatype > 0:
            return Literal(literal.lex, datatype=datatype_lookup[literal.datatype]), last_prefix_get, last_name_get
        else:
            return Literal(literal.lex), last_prefix_get, last_name_get

    if triple.WhichOneof(term) == f"{term_key}_iri":
        iri_field = getattr(triple, f"{term_key}_iri")
        if iri_field.prefix_id == 0:
            prefix_id = last_prefix_get
        else:
            prefix_id = iri_field.prefix_id
            last_prefix_get = prefix_id
        if iri_field.name_id == 0:
            last_name_get += 1
            name_id = last_name_get
        else:
            name_id = iri_field.name_id
            last_name_get = name_id
        return URIRef(prefix_lookup[prefix_id] + name_lookup[name_id]), last_prefix_get, last_name_get

    return last_value, last_prefix_get, last_name_get


def main():
    filename = "./output/encoded_file.jelly"
    rdf_stream_frame = parse_rdf_stream(filename)

    graph = Graph()
    options_row = rdf_stream_frame.rows.pop(0)
    use_prefix_table = process_options(options_row)

    prefix_lookup = {}
    name_lookup = {}
    datatype_lookup = {}

    last_prefix_set = 0
    last_name_set = 0
    last_datatype_set = 0
    last_prefix_get = 1
    last_name_get = 1
    last_terms = {"s": None, "p": None, "o": None}

    for message in rdf_stream_frame.rows:
        if message.HasField("prefix"):
            if message.prefix.id == 0:
                last_prefix_set += 1
                current_id = last_prefix_set
            else:
                current_id = message.prefix.id
                last_prefix_set = current_id
            prefix_lookup[current_id] = message.prefix.value

        elif message.HasField("name"):
            if message.name.id == 0:
                last_name_set += 1
                current_id = last_name_set
            else:
                current_id = message.name.id
                last_name_set = current_id
            name_lookup[current_id] = message.name.value

        elif message.HasField("namespace"):
            if use_prefix_table:
                if message.prefix.id == 0:
                    current_id = last_prefix_set
                else:
                    current_id = message.namespace.value.prefix_id
                last_prefix_get = current_id #bc namespaces unfortunately also use lookup tables
                namespace_str = prefix_lookup[current_id]
            else:
                if message.name.id == 0:
                    current_id = last_name_set + 1
                else:
                    current_id = message.namespace.value.name_id
                namespace_str = name_lookup[current_id]
                last_name_get = current_id
            graph.namespace_manager.bind(message.namespace.name, Namespace(namespace_str))

        elif message.HasField("datatype"):
            if message.datatype.id == 0:
                last_datatype_set += 1
                current_id = last_datatype_set
            else:
                current_id = message.datatype.id
            last_datatype_set = current_id
            datatype_lookup[current_id] = message.datatype.value

        elif message.HasField("triple"):
            triple = message.triple

            subject, last_prefix_get, last_name_get = resolve_term(
                triple, "subject", prefix_lookup, name_lookup, datatype_lookup,
                last_prefix_get, last_name_get, last_terms["s"]
            )
            last_terms["s"] = subject

            predicate, last_prefix_get, last_name_get = resolve_term(
                triple, "predicate", prefix_lookup, name_lookup, datatype_lookup,
                last_prefix_get, last_name_get, last_terms["p"]
            )
            last_terms["p"] = predicate

            obj, last_prefix_get, last_name_get = resolve_term(
                triple, "object", prefix_lookup, name_lookup, datatype_lookup,
                last_prefix_get, last_name_get, last_terms["o"]
            )
            last_terms["o"] = obj

            graph.add((subject, predicate, obj))

    graph.serialize("./output/output.ttl", format="ttl")


if __name__ == "__main__":
    main()