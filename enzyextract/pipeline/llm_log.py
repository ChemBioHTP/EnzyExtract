import base64
import hashlib
import os
import polars as pl


llm_log_schema = {
    'namespace': pl.Utf8,
    'version': pl.Utf8,
    'shard': pl.UInt32,
    'status': pl.Utf8, # aborted | local | submitted | downloaded

    'model_name': pl.Utf8,
    'llm_provider': pl.Utf8,
    'structured': pl.Boolean,
    # 'prompt': pl.Utf8,
    'prompt_hash': pl.Utf8,

    'file_uuid': pl.Utf8, # filename given by openai
    'batch_uuid': pl.Utf8, # batch id given by openai

    '_enzy_prefix': pl.Utf8, # prefix: probably the path to the .enzy folder.
    'batch_fpath': pl.Utf8,
    'corresp_fpath': pl.Utf8, # where the correspondence between custom_id and relavent info is stored
    'completion_fpath': pl.Utf8, # where the completion is stored
}


def read_log(log_location: str) -> pl.DataFrame:
    blank_log = pl.DataFrame(schema=llm_log_schema)

    if os.path.exists(log_location):
        # reorder
        if log_location.endswith('.parquet'):
            log = pl.read_parquet(log_location)
            log = pl.concat([blank_log, log], how='diagonal_relaxed') # reorder
        elif log_location.endswith('.tsv'):
            log = pl.read_csv(log_location, separator='\t', schema_overrides=llm_log_schema)
            log = pl.concat([blank_log, log], how='diagonal_relaxed')
    else:
        log = blank_log
    log = log.with_columns(
        pl.col('_enzy_prefix').fill_null(''), # fill nulls with empty string
    )
    # apply the enzy_prefix to the batch_fpath and corresp_fpath
    log = log.with_columns([
        (pl.col('_enzy_prefix') + pl.col('batch_fpath')).alias('batch_fpath'),
        (pl.col('_enzy_prefix') + pl.col('corresp_fpath')).alias('corresp_fpath'),
        (pl.col('_enzy_prefix') + pl.col('completion_fpath')).alias('completion_fpath'),
    ]).drop('_enzy_prefix')
    return log


def separate_prefix(log: pl.DataFrame) -> pl.DataFrame:
    """Attempts to extract the prefix from the log"""
    # recreate the _enzy_prefix column
    def get_enzy_prefix(struct: dict):
        return os.path.commonprefix([x for x in [
            struct['batch_fpath'],
            struct['corresp_fpath'],
            struct['completion_fpath'],
        ] if x is not None])
    log = log.with_columns([
        pl.struct([
            'batch_fpath',
            'corresp_fpath',
            'completion_fpath',
        ]).map_elements(get_enzy_prefix, return_dtype=pl.Utf8).alias('_enzy_prefix'),
    ]).with_columns([
        pl.when(
            pl.col('_enzy_prefix').is_not_null()
            & pl.col('_enzy_prefix').str.ends_with('.enzy/')
        ).then(
            pl.col('_enzy_prefix')
        ).otherwise(
            pl.lit('')
        ).alias('_enzy_prefix'),
    ]).with_columns([
        pl.col('batch_fpath').str.strip_prefix(pl.col('_enzy_prefix')),
        pl.col('corresp_fpath').str.strip_prefix(pl.col('_enzy_prefix')),
        pl.col('completion_fpath').str.strip_prefix(pl.col('_enzy_prefix')),
    ])
    return log


def write_log(log: pl.DataFrame, log_location: str):

    # log = separate_prefix(log)

    if log_location.endswith('.parquet'):
        log.write_parquet(log_location)
        # migrate to tsv
        # write_dest = log_location.removesuffix('.parquet') + '.tsv'
        # if os.path.exists(write_dest):
        #     print("File already exists, skipping.")
        #     return
        # log.write_csv(log_location.removesuffix('.parquet') + '.tsv', separator='\t')
    elif log_location.endswith('.tsv'):
        log.write_csv(log_location, separator='\t')
    else:
        log.write_parquet(log_location + '.parquet')


def update_log(
    *,
    log_location: str,
    namespace: str,
    version: str,
    shard: int,
    status: str,

    model_name: str,
    llm_provider: str,
    prompt: str,
    structured: bool,

    file_uuid: str,
    batch_uuid: str,
    batch_fpath: str,
    corresp_fpath: str,
    try_to_overwrite: bool = False,
):
    # try to extract the enzy_prefix
    # enzy_prefix = os.path.commonprefix([
    #     batch_fpath,
    #     corresp_fpath,
    # ])
    # if enzy_prefix and enzy_prefix.endswith('.enzy/'):
    #     batch_fpath = batch_fpath.removeprefix(enzy_prefix)
    #     corresp_fpath = corresp_fpath.removeprefix(enzy_prefix)
    # else:
    #     enzy_prefix = ''


    df = pl.DataFrame({
        'namespace': [namespace],
        'version': [version],
        'shard': [shard],
        'status': [status],

        'model_name': [model_name],
        'llm_provider': [llm_provider],
        'prompt_hash': [base64.b64encode(hashlib.sha256(prompt.encode()).digest()).decode()],
        'structured': [structured],

        'file_uuid': [file_uuid],
        'batch_uuid': [batch_uuid],
        # '_enzy_prefix': [enzy_prefix],
        'batch_fpath': [batch_fpath],
        'corresp_fpath': [corresp_fpath],
        'completion_fpath': [None],
    }, schema_overrides=llm_log_schema)
    log = read_log(log_location)
    if try_to_overwrite:
        log = log.update(df, on=['namespace', 'version', 'shard'])
    else:
        log = pl.concat([log, df], how='diagonal_relaxed')
    write_log(log, log_location)


def convert_log(
    df
):
    """Update a log file into the newer version. TODO"""
    return df