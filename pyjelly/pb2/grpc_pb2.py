# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: grpc.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'grpc.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import rdf_pb2 as rdf__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\ngrpc.proto\x12!eu.ostrzyciel.jelly.core.proto.v1\x1a\trdf.proto\"s\n\x12RdfStreamSubscribe\x12\r\n\x05topic\x18\x01 \x01(\t\x12N\n\x11requested_options\x18\x02 \x01(\x0b\x32\x33.eu.ostrzyciel.jelly.core.proto.v1.RdfStreamOptions\"\x13\n\x11RdfStreamReceived2\x87\x02\n\x10RdfStreamService\x12z\n\x0cSubscribeRdf\x12\x35.eu.ostrzyciel.jelly.core.proto.v1.RdfStreamSubscribe\x1a\x31.eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame0\x01\x12w\n\nPublishRdf\x12\x31.eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame\x1a\x34.eu.ostrzyciel.jelly.core.proto.v1.RdfStreamReceived(\x01\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'grpc_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_RDFSTREAMSUBSCRIBE']._serialized_start=60
  _globals['_RDFSTREAMSUBSCRIBE']._serialized_end=175
  _globals['_RDFSTREAMRECEIVED']._serialized_start=177
  _globals['_RDFSTREAMRECEIVED']._serialized_end=196
  _globals['_RDFSTREAMSERVICE']._serialized_start=199
  _globals['_RDFSTREAMSERVICE']._serialized_end=462
# @@protoc_insertion_point(module_scope)
