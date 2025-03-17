import rdf_pb2
from rdflib import URIRef, BNode, Literal
from rdflib import Graph

def get_ids(g, item):
    prefix, namespace, localname = g.namespace_manager.compute_qname(item)
    
    # what if namespace is not in the prefix lookup?
    
    global name_counter
    if localname not in name_lookup.keys():
        name_lookup.update({localname: name_counter})
        row_name_entry = rdf_pb2.RdfStreamRow()
        row_name_entry.name.id = name_counter
        row_name_entry.name.value = localname
        rdf_stream_frame.rows.append(row_name_entry)
        name_counter += 1
        
    return prefix_lookup[namespace], name_lookup[localname]
    
def get_datatype_id(item):
    global datatype_counter
    if item not in datatype_lookup.keys():
        datatype_lookup.update({item:datatype_counter})
        row_datatype_entry = rdf_pb2.RdfStreamRow()
        row_datatype_entry.datatype.id = datatype_counter
        row_datatype_entry.datatype.value = item
        rdf_stream_frame.rows.append(row_datatype_entry)
        datatype_counter += 1
    
    return datatype_lookup[item]

def convert_subject(item, triple_row, g):
    print(item)
    if isinstance(item, URIRef):
        print("Is an IRI")
        prefix_id, name_id = get_ids(g, item)
        triple_row.triple.s_iri.prefix_id = prefix_id
        triple_row.triple.s_iri.name_id = name_id
        
    elif isinstance(item, BNode):
        print("Is a Blank Node")
        triple_row.triple.s_bnode = item
    elif isinstance(item, Literal):
        print("Is a Literal")
        triple_row.triple.s_literal.lex = str(item)
        if item.datatype:
            datatype_id = get_datatype_id(item.datatype)
            triple_row.triple.s_literal.datatype = datatype_id
        if item.language:
            triple_row.triple.s_literal.langtag = item.language
    elif isinstance(item, tuple) and len(item) == 3:
        print("Is a RDF-star Triple")
    else:
        exit(-1)
    return triple_row

def convert_predicate(item, triple_row, g):
    print(item)
    if isinstance(item, URIRef):
        print("Is an IRI")
        prefix_id, name_id = get_ids(g, item)
        triple_row.triple.p_iri.prefix_id = prefix_id
        triple_row.triple.p_iri.name_id = name_id
        
    elif isinstance(item, BNode):
        print("Is a Blank Node")
        triple_row.triple.p_bnode = item
    elif isinstance(item, Literal):
        print("Is a Literal")
        triple_row.triple.p_literal.lex = str(item)
        if item.datatype:
            datatype_id = get_datatype_id(item.datatype)
            triple_row.triple.p_literal.datatype = datatype_id
        if item.language:
            triple_row.triple.p_literal.langtag = item.language
    # elif isinstance(item, tuple) and len(item) == 3:
    #     print("Is a RDF-star Triple")
    #     create_RDF_star_row()
    else:
        exit(-1)
    return triple_row

def convert_object(item, triple_row, g):
    print(item)
    if isinstance(item, URIRef):
        print("Is an IRI")
        prefix_id, name_id = get_ids(g, item)
        triple_row.triple.o_iri.prefix_id = prefix_id
        triple_row.triple.o_iri.name_id = name_id
        
    elif isinstance(item, BNode):
        print("Is a Blank Node")
        triple_row.triple.o_bnode = item
    elif isinstance(item, Literal):
        print("Is a Literal")
        triple_row.triple.o_literal.lex = str(item) #.value does not work every time
        if item.datatype:
            datatype_id = get_datatype_id(item.datatype)
            triple_row.triple.o_literal.datatype = datatype_id
        if item.language:
            triple_row.triple.o_literal.langtag = item.language
    # elif isinstance(item, tuple) and len(item) == 3:
    #     print("Is a RDF-star Triple")
    #     create_RDF_star_row()
    else:
        exit(-1)
    return triple_row

def create_triple(subj, obj, pred, g):
    triple_row = rdf_pb2.RdfStreamRow()
    triple_row = convert_subject(subj, triple_row, g)
    triple_row = convert_predicate(pred, triple_row, g)
    triple_row = convert_object(obj, triple_row, g)
    return triple_row

name_lookup = {}     
prefix_lookup = {}   
datatype_lookup = {} 

g = Graph()
g.parse("./test_files/multiple_blank_nodes.ttl")
filename = "./output/encoded_file.bin"

rdf_stream_frame = rdf_pb2.RdfStreamFrame()
first_row = rdf_pb2.RdfStreamRow()
first_row.options.physical_type = rdf_pb2.PhysicalStreamType.PHYSICAL_STREAM_TYPE_TRIPLES
first_row.options.version = 1
rdf_stream_frame.rows.append(first_row)

prefix_counter = 1
name_counter = 1
datatype_counter = 1

for prefix, namespace in g.namespaces():
    print(f"Prefix: {prefix}, Namespace: {namespace}")
    prefix_lookup.update({namespace: prefix_counter})
    row_prefix_entry = rdf_pb2.RdfStreamRow()
    row_prefix_entry.prefix.id = prefix_counter
    row_prefix_entry.prefix.value = namespace
    rdf_stream_frame.rows.append(row_prefix_entry)
    
    row_name_entry = rdf_pb2.RdfStreamRow()
    row_name_entry.name.id = 0
    row_name_entry.name.value = ""
    rdf_stream_frame.rows.append(row_name_entry)
    
    row_namespace_declaration = rdf_pb2.RdfStreamRow()
    row_namespace_declaration.namespace.name = prefix
    row_namespace_declaration.namespace.value.prefix_id = prefix_counter
    row_namespace_declaration.namespace.value.name_id = 0
    rdf_stream_frame.rows.append(row_namespace_declaration)
    
    prefix_counter += 1
    
for subj, pred, obj in g:
    print(f"Subject: {subj}")
    print(f"Predicate: {pred}")
    print(f"Object: {obj}")
    print("-" * 30)
    triple_row = create_triple(subj, obj, pred, g)

    rdf_stream_frame.rows.append(triple_row)
    
with open(filename, "wb") as f:
  f.write(rdf_stream_frame.SerializeToString())
    
    
    