import polars as pl

pdb_df_schema = {
    'pdb': pl.Utf8,
    'descriptor': pl.Utf8,
    'name': pl.Utf8,
    'sys_name': pl.Utf8,
    'organism': pl.Utf8,
    'info': pl.Utf8,
    'seq': pl.Utf8,
    'seq_can': pl.Utf8,
    # 'pmids': pl.Utf8, # these are pipe-separated strings (due to legacy pandas code)
    # 'dois': pl.Utf8, # these are pipe-separated strings (due to legacy pandas code)
    'pmids': pl.List(pl.Utf8),
    'dois': pl.List(pl.Utf8),
}

uniprot_df_schema = {
    'query_uniprot': pl.Utf8,
    'uniprot': pl.Utf8,
    'uniprot_aliases': pl.List(pl.Utf8),
    'enzyme_name': pl.Utf8,
    'organism': pl.Utf8,
    'organism_common': pl.Utf8,
    'sequence': pl.Utf8,
    'ec_numbers': pl.List(pl.Utf8),
    'dois': pl.List(pl.Utf8),
    'pmids': pl.List(pl.Utf8),
    'uniparc': pl.Utf8,
    'why_deleted': pl.Utf8,
    'full_response': pl.Utf8,
}

__all__ = [
    'pdb_df_schema',
    'uniprot_df_schema',
]