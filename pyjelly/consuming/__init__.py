from pyjelly import jelly
from pyjelly.options import ConsumerStreamOptions


def options_from_frame(
    frame: jelly.RdfStreamFrame, *, delimited: bool
) -> ConsumerStreamOptions:
    row = frame.rows[0]
    options = row.options
    return ConsumerStreamOptions(
        stream_name=options.stream_name,
        version=options.version,
        name_lookup_size=options.max_name_table_size,
        prefix_lookup_size=options.max_prefix_table_size,
        datatype_lookup_size=options.max_datatype_table_size,
        delimited=delimited,
        physical_type=options.physical_type,
        logical_type=options.logical_type,
    )
