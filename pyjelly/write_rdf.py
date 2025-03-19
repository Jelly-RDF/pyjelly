import logging
from rdflib import URIRef, BNode, Literal, Graph
import rdf_pb2

logging.basicConfig(level=logging.INFO)


class RDFEncoder:
    def __init__(self, graph: Graph):
        self.graph = graph
        self.use_prefix_lookup = True
        if self.use_prefix_lookup:
            self.prefix_lookup = {}
        else:
            self.prefix_lookup = None
            
        self.name_lookup = {}
        self.datatype_lookup = {}
        self.prefix_counter = 1
        self.name_counter = 1
        self.datatype_counter = 1
        
        self.prefix_last_set_id = -1000
        self.prefix_last_get_id = -1000
        
        self.name_last_set_id = -1000
        self.name_last_get_id = -1000
 
        self.datatype_last_set_id = -1000
        self.datatype_last_get_id = -1000 
            
        self.rdf_stream_frame = rdf_pb2.RdfStreamFrame()
        self.rdf_stream_frame.metadata["dataset"] = b"test_nt_data"
        self.last_terms = {"s": None, "o": None, "p": None}
        self._initialize_frame()

    def _initialize_frame(self):
        first_row = rdf_pb2.RdfStreamRow()
        first_row.options.stream_name = "Test"
        first_row.options.physical_type = rdf_pb2.PhysicalStreamType.PHYSICAL_STREAM_TYPE_TRIPLES
        first_row.options.generalized_statements = False
        first_row.options.rdf_star = False
        first_row.options.max_name_table_size = 50
        if self.use_prefix_lookup:
            first_row.options.max_prefix_table_size = 50
        else:
            first_row.options.max_prefix_table_size = 0
        first_row.options.max_datatype_table_size = 10
        first_row.options.version = 2
        self.rdf_stream_frame.rows.append(first_row)
        
    def update_name_lookup(self, name):
        id = self.name_lookup.get(name)
        if id == None:
            self.name_lookup[name] = self.name_counter
            id = self.name_counter
            if id == self.name_last_set_id + 1:
                set_id =  0
            else:
                set_id = id
            self.name_last_set_id = id
            
            row_name_entry = rdf_pb2.RdfStreamRow()
            row_name_entry.name.id = set_id
            row_name_entry.name.value = name
            self.rdf_stream_frame.rows.append(row_name_entry)
            self.name_counter += 1
            
        if id == (self.name_last_get_id+1):
            get_id = 0
        else:
            get_id = id
        self.name_last_get_id = id
        return get_id
    
    def split_iri(self, iri):
        """
        Splits an IRI into a prefix and local name by finding the last occurrence
        of '#' or '/'. (as in jvm version)
        """
        idx_hash = iri.find('#', 8)
        
        if idx_hash == -1:  
            idx_slash = iri.rfind('/')  
            if idx_slash != -1: 
                return iri[:idx_slash + 1], iri[idx_slash + 1:]
            else: 
                return '', iri
        else: 
            return iri[:idx_hash + 1], iri[idx_hash + 1:]

    def update_prefix_lookup(self, prefix):
        id = self.prefix_lookup.get(prefix)
        if id == None:
            self.prefix_lookup[prefix] = self.prefix_counter
            id = self.prefix_counter
            if id == self.prefix_last_set_id + 1:
                set_id =  0
            else:
                set_id = id
            self.prefix_last_set_id = id
            
            row_prefix_entry = rdf_pb2.RdfStreamRow()
            row_prefix_entry.prefix.id = set_id
            row_prefix_entry.prefix.value = prefix
            self.rdf_stream_frame.rows.append(row_prefix_entry)
            
            self.prefix_counter += 1
            
        if id == self.prefix_last_get_id:
            get_id = 0
        else:
            get_id = id
        self.prefix_last_get_id = id
        return get_id
    
    def get_ids(self, item: URIRef):
        
        if self.use_prefix_lookup:
            prefix, name =  self.split_iri(item)
            prefix_id = self.update_prefix_lookup(prefix)
        else:
            prefix_id = 0
            name = item
        name_id = self.update_name_lookup(name)
        return prefix_id, name_id

    def get_datatype_id(self, item):
        id = self.datatype_lookup.get(item)
        if id == None:
            self.datatype_lookup[item] = self.datatype_counter
            id = self.datatype_counter
            if id == self.datatype_last_set_id + 1:
                set_id =  0
            else:
                set_id = id
            self.datatype_last_set_id = id
            
            row_datatype_entry = rdf_pb2.RdfStreamRow()
            row_datatype_entry.datatype.id = set_id
            row_datatype_entry.datatype.value = item
            self.rdf_stream_frame.rows.append(row_datatype_entry)
            self.datatype_counter += 1

        get_id = id
        self.datatype_last_get_id = id
        return get_id

    def convert_term(self, item, triple_row, field_prefix: str):

        if isinstance(item, URIRef):
            logging.info("Processing IRI: %s", item)
            prefix_id, name_id = self.get_ids(item)
            getattr(triple_row.triple, f"{field_prefix}_iri").prefix_id = prefix_id
            getattr(triple_row.triple, f"{field_prefix}_iri").name_id = name_id

        elif isinstance(item, BNode):
            logging.info("Processing Blank Node: %s", item)
            setattr(triple_row.triple, f"{field_prefix}_bnode", item)

        elif isinstance(item, Literal):
            logging.info("Processing Literal: %s", item)
            literal_field = getattr(triple_row.triple, f"{field_prefix}_literal")
            literal_field.lex = str(item)
            if item.datatype:
                datatype_id = self.get_datatype_id(str(item.datatype))
                literal_field.datatype = datatype_id
            if item.language:
                literal_field.langtag = item.language

        else:
            raise TypeError(f"Unsupported RDF term type: {type(item)}")
        return triple_row
    
    

    def create_triple(self, subj, pred, obj):
        triple_row = rdf_pb2.RdfStreamRow()
        if self.last_terms["s"] != subj:
            self.last_terms["s"] = subj
            triple_row = self.convert_term(subj, triple_row, "s")
        
        if self.last_terms["p"] != pred:
            self.last_terms["p"] = pred
            triple_row = self.convert_term(pred, triple_row, "p")
        
        if self.last_terms["o"] != obj:
            self.last_terms["o"] = obj
            triple_row = self.convert_term(obj, triple_row, "o")
        return triple_row

    def process_namespaces(self):
        for prefix, namespace in self.graph.namespaces():
            logging.info("Processing namespace: prefix=%s, namespace=%s", prefix, namespace)

            if self.use_prefix_lookup:
                prefix_id = self.update_prefix_lookup(str(namespace))
                name_id = self.update_name_lookup("")
            else:
                prefix_id = 0
                name_id = self.update_name_lookup(namespace)
            
            row_namespace_decl = rdf_pb2.RdfStreamRow()
            row_namespace_decl.namespace.name = prefix
            row_namespace_decl.namespace.value.prefix_id = prefix_id
            row_namespace_decl.namespace.value.name_id = name_id
            self.rdf_stream_frame.rows.append(row_namespace_decl)

    def process_triples(self):
        for subj, pred, obj in self.graph:
            logging.info("Triple - Subject: %s, Predicate: %s, Object: %s", subj, pred, obj)
            triple_row = self.create_triple(subj, pred, obj)
            self.rdf_stream_frame.rows.append(triple_row)

    def serialize_to_file(self, filename: str):
        with open(filename, "wb") as f:
            f.write(self.rdf_stream_frame.SerializeToString())


def main():
    graph = Graph()
    graph.parse("./test_files/new_sample.ttl")
    output_file = "./output/encoded_file.jelly"

    encoder = RDFEncoder(graph)
    encoder.process_namespaces()
    encoder.process_triples()
    encoder.serialize_to_file(output_file)
    logging.info("Serialization complete. Output written to %s", output_file)


if __name__ == "__main__":
    main()