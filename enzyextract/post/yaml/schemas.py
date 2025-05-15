import polars as pl
_data_schema = {
    'descriptor': pl.Utf8,
    'substrate': pl.Utf8,
    'kcat': pl.Utf8,
    'km': pl.Utf8,
    'kcat_km': pl.Utf8,
    'fragments': pl.List(pl.Utf8),
}
_enzyme_ctx_schema = {
    'fullname': pl.Utf8,
    'synonyms': pl.List(pl.Utf8),
    'organisms': pl.List(pl.Utf8),
    'mutants': pl.List(pl.Utf8),
}
_substrate_ctx_schema = {
    'fullname': pl.Utf8,
    'synonyms': pl.List(pl.Utf8),
}
_general_ctx_schema = {
    'temperatures': pl.List(pl.Utf8),
    'pHs': pl.List(pl.Utf8),
    # 'solutions': pl.List(pl.Utf8),
    'other': pl.List(pl.Utf8),
}
_complete_ctx_schema = {
    'enzymes': pl.List(pl.Struct(_enzyme_ctx_schema)),
    'substrates': pl.List(pl.Struct(_substrate_ctx_schema)),
    **_general_ctx_schema,
}
_errors_schema = {
    # 'pmid': pl.Utf8,
    'msg': pl.Utf8,
    'stacktrace': pl.Utf8,
}

rulebreakers_schema = {
    'record_id': pl.Utf8,
    'self_id': pl.Int64,
    'key': pl.Utf8,
    'value_str': pl.Utf8,
    'value_num': pl.Float64,
    'value_list_str': pl.List(pl.Utf8),
    'value_list_num': pl.List(pl.Float64),
    'value_list_ref': pl.List(pl.Int64),
    'value_ref': pl.Int64
}


__all__ = [
    '_data_schema',
    '_enzyme_ctx_schema',
    '_substrate_ctx_schema',
    '_general_ctx_schema',
    '_complete_ctx_schema',
    '_errors_schema',
    'rulebreakers_schema',
]