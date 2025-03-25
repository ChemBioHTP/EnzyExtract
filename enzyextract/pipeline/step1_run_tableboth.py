# working_enzy_table_md, but tableless

import asyncio
import json
import re
import time
from typing import Optional
import polars as pl
import pandas as pd
import pymupdf
import glob
import os
from tqdm import tqdm

from enzyextract.utils import prompt_collections
from enzyextract.submit.batch_utils import to_openai_batch_request, write_to_jsonl
from enzyextract.utils.fresh_version import next_available_version
from enzyextract.pre.reocr.micro_fix import duplex_mM_corrected_text
from enzyextract.submit.litellm_management import process_env, submit_batch_file
from enzyextract.utils.namespace_management import glean_model_name, validate_namespace
from enzyextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_directory
from enzyextract.utils.working import pmid_to_tables_from
from enzyextract.utils.yaml_process import get_pmid_to_yaml_dict
from enzyextract.utils.openai_schema import to_openai_batch_request_with_schema
from enzyextract.pre.reocr.micro_fix import true_widest_mM_re, ends_with_ascii_control_re



llm_log_schema_overrides = {
    'namespace': pl.Utf8,
    'version': pl.Utf8,
    'shard': pl.UInt32,
    'batch_fpath': pl.Utf8,
    'model_name': pl.Utf8,
    'llm_provider': pl.Utf8,
    'prompt': pl.Utf8,
    'structured': pl.Boolean,
    'file_uuid': pl.Utf8,
    'batch_uuid': pl.Utf8,
    'status': pl.Utf8,
    'completion_fpath': pl.Utf8,
}
def read_log(log_location: str) -> pl.DataFrame:
    if os.path.exists(log_location):
        if log_location.endswith('.parquet'):
            log = pl.read_parquet(log_location)
        elif log_location.endswith('.tsv'):
            log = pl.read_csv(log_location, separator='\t', schema_overrides=llm_log_schema_overrides)
    else:
        log = pl.DataFrame({
            'namespace': [],
            'version': [],
            'shard': [],
            'batch_fpath': [],
            'model_name': [],
            'llm_provider': [],
            'prompt': [],
            'structured': [],
            'file_uuid': [], # filename given by openai
            'batch_uuid': [], # batch id given by openai
            'status': [], # aborted | local | submitted | downloaded
            'completion_fpath': [], # where the completion is stored
        }, schema_overrides=llm_log_schema_overrides)
    return log

def update_log(
    log_location: str, 
    namespace: str,
    version: str,
    shard: int,
    batch_fpath: str,
    model_name: str,
    llm_provider: str,
    prompt: str,
    structured: bool,
    file_uuid: str,
    batch_uuid: str,
    status: str,
    try_to_overwrite: bool = False,
):
    df = pl.DataFrame({
        'namespace': [namespace],
        'version': [version],
        'shard': [shard],
        'batch_fpath': [batch_fpath],
        'model_name': [model_name],
        'llm_provider': [llm_provider],
        'prompt': [prompt],
        'structured': [structured],
        'file_uuid': [file_uuid],
        'batch_uuid': [batch_uuid],
        'status': [status],
        'completion_fpath': [None],
    }, schema_overrides=llm_log_schema_overrides)
    log = read_log(log_location)
    if try_to_overwrite:
        log = log.update(df, on=['namespace', 'version', 'shard'])
    else:
        log = pl.concat([log, df], how='diagonal_relaxed')
    log.write_parquet(log_location)

def build_manifest(pdf_root):
    """
    Build a manifest view of the pdfs in the pdf_root directory.
    """
    pdfs = glob.glob(f"{pdf_root}/**/*.pdf", recursive=True)
    pmids = [os.path.basename(x).rsplit('.', 1)[0] for x in pdfs]
    # create a dataframe with the pmids and their corresponding pdfs
    manifest = pl.DataFrame({
        'fileroot': [os.path.dirname(x) for x in pdfs],
        'filename': [os.path.basename(x) for x in pdfs],
        'pmid': pmids,
    })
    return manifest

def main(
    namespace: str, # ids
    pdf_root: str, # read from
    micro_path: str,
    tables_from: Optional[str],
    dest_folder: str, # write to
    log_location: str,
    model_name: str, # model settings
    prompt: str, 
    structured = False,
    llm_provider: str = 'openai',
    version=None,
    _check_nonzero_reocr=True,
    _check_nonzero_tables=True,
):
    
    process_env('.env')

    try_to_overwrite = False

    if version is None:
        version = time.strftime("%Y%m%d%H%M%S")
    if not isinstance(version, str):
        version = str(version)
    previous_log = read_log(log_location)

    # do validation, making sure namespace is unique
    validate_namespace(namespace)
    previous_namespace = previous_log.filter(
        pl.col('namespace') == namespace
    ) # .select('namespace')
    if previous_namespace.height > 0:
        print(f"Namespace {namespace} has already been tried, are you sure? Type 'previous' to reuse the old batch.")
        yn = input("y/n: ")
        if yn == 'previous' or yn == 'p':
            version = previous_namespace.item(0, 'version')
            try_to_overwrite = True
            # we need to remove the old namespace
            # previous_log = previous_log.with_columns(
            #     pl.col('namespace').replace(namespace, f"DELETED: {namespace}")
            # )
            # previous_log.write_parquet(log_location)
        else:
            return

    os.makedirs(dest_folder, exist_ok=True)
    batch = []

    print("Namespace: ", namespace)

    will_write_to = f'{dest_folder}/{namespace}_{version}.jsonl'

    if os.path.exists(will_write_to):
        print(f"File {will_write_to} already exists, skipping.")

        need_to_submit = [will_write_to]
        i = 0
    else:
        pmid_to_tables = {}
        if tables_from is not None:
            pmid_to_tables = pmid_to_tables_from(tables_from)
            if _check_nonzero_tables:
                assert pmid_to_tables, "No tables found"


        acceptable_pmids = pmids_from_directory(pdf_root)

        # NOTE: Put custom processing/filtering of pmids here
        target_pmids = acceptable_pmids

        # Option 1: do not use a manifest
        # manifest_view = None
        manifest_view = build_manifest(pdf_root)

        # Option 2: use a manifest
        if manifest_view is None:
            manifest = pl.read_parquet('data/manifest.parquet')
            # only readable
            manifest = manifest.with_columns([
                pl.col('filename').str.replace('\.pdf$', '').alias('pmid')
            ])
            manifest_view = manifest.filter(
                pl.col('readable')
                & ~pl.col('bad_ocr')
                & pl.col('pmid').is_in(target_pmids)
            ).unique('filename').select(['fileroot', 'filename', 'pmid'])

            print(f"Using pmids {len(acceptable_pmids)} -> {manifest_view.height}")

        # make sure this is an intersection between what we will read and pmid_to_tables
        _intersect = 0
        for pmid in manifest_view['pmid']:
            if pmid in pmid_to_tables:
                _intersect += 1
        print(f"Intersection of {_intersect} pmids with tables")
        if _check_nonzero_tables:
            assert _intersect > 0, "No intersection of tables found"
        elif _intersect == 0:
            print("Warning: No tables found, but this is ok.")


        # apply micro fix
        if micro_path.endswith('.parquet'):
            micro_df = pl.read_parquet(micro_path) # .to_pandas()
        else:
            micro_df = pd.read_csv(micro_path)
            micro_df = micro_df.astype({'pdfname': 'str'})

        # only want 
        true_micro_df = micro_df.filter(
            (pl.col('real_char') == "mu") 
            & (pl.col('confidence') > 0.98)
        )
        true_m_df = micro_df.filter(pl.col('real_char') == "m")

        micro_df = pl.concat([true_micro_df, true_m_df]) # , ignore_index=True)


        # sanity check: ensure that some of the pmids are in the micro_df
        _num_in_micro = len(set(micro_df['pdfname']).intersection(set(manifest_view['pmid'])))
        if _num_in_micro == 0:
            # try removing '.pdf' from the pdfname
            micro_df = micro_df.with_columns([
                (pl.col('pdfname').str.replace("\.pdf$", "")).alias('pdfname')
            ])
            _num_in_micro = len(set(micro_df['pdfname']).intersection(set(target_pmids)))
        print(f"Intersection of {_num_in_micro} pmids with micro corrections")
        if _check_nonzero_reocr:
            assert _num_in_micro > 0, "No intersection of micro corrections found"
        elif _num_in_micro == 0:
            print("Warning: No micro corrections found, but this is ok.")

        _pmid_with_tables = 0
        for fileroot, filename, pmid in tqdm(manifest_view.iter_rows(), total=manifest_view.height):
            assert pmid in target_pmids
            
            try:
                doc = pymupdf.open(fileroot + '/' + filename)
            except Exception as e:
                print("Error opening", fileroot)
                print(e)
                continue
            
            if len(doc) > 100:
                # 100 pages is excessive
                continue

            # now obtain texts
            docs = []
            
            
            if pmid_to_tables and pmid in pmid_to_tables:
                for filename in pmid_to_tables.get(pmid, []):
                    with open(f'{tables_from}/{filename}', 'r', encoding='utf-8') as f:
                        docs.append(f.read())
                _pmid_with_tables += 1
            
            # best micro re
            # widest_mM_re = re.compile(r'\bmm(?=$|[\Wo2])', re.IGNORECASE)
            # \u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0011\u0012\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f
            ascii_control_re = re.compile(r'(?<!\w)[\x00-\x08\x11\x12\x14-\x1F]M\b') # \x7F-\x9F
            pages = duplex_mM_corrected_text(doc, pmid, micro_df, _re=true_widest_mM_re)
            # post-processing
            for i, page in enumerate(pages):
                # if 'µMo' in page:
                    # print("Warning: funny looking capitalization issue in", pmid)
                    # pass
                txt = page.replace('µMo', 'µmo') # fix funny looking capitalization issue in post
                txt = ascii_control_re.sub('µM', txt)
                pages[i] = txt
            docs.extend(pages)
            

            # obtain original annotation from part A
            # use the table_md_root

            if structured:
                req = to_openai_batch_request_with_schema(f'{namespace}_{version}_{pmid}', prompt, docs,
                                                            model_name=model_name)
            else:
                req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt, docs, 
                                        model_name=model_name)
            batch.append(req)

        if _pmid_with_tables:
            print(f"Found {_pmid_with_tables} pmids with tables")
        else:
            print("WARNING: no pmids with tables")
        print("Using model", model_name)

            
        # write in chunks

        chunk_size = 1000
        have_multiple = len(batch) > chunk_size # need to enforce chunk size, since OpenAI has data size limit
        need_to_submit = []
        for i in range(0, len(batch), chunk_size):
            chunk = batch[i:i+chunk_size]


            if have_multiple:
                will_write_to = f'{dest_folder}/{namespace}_{version}.{i}.jsonl'
            write_to_jsonl(chunk, will_write_to)
            need_to_submit.append(will_write_to)

    print("Time to submit!")
    for will_write_to in need_to_submit:
        try:
            # if i == 0:
            #     # write the first one as an inspection, for debugging purposes

            #     # get content 
            #     prompts = [(x['body']['messages'][0]['content']) for x in chunk]
            #     contents = ['\n'.join(y['content'] for y in x['body']['messages'][1:]) for x in chunk] # list of strings
            #     df = pl.DataFrame({'custom_id': [x['custom_id'] for x in chunk], 
            #             'pmid': [x['custom_id'].split('_', 2)[2] for x in chunk], 
            #             'content': contents,
            #                 # 'pages': pagess,
            #             'prompt': prompts})
            #     df.write_parquet('_debug/latest_tableboth.parquet')
        
            # batchname = submit_batch_file(will_write_to, pending_file='batches/pending.jsonl') # will ask for confirmation
            file_uuid, batchname = asyncio.run(submit_batch_file(will_write_to, custom_llm_provider=llm_provider))
            status = 'submitted'
        except Exception as e:
            print("Error submitting batch", will_write_to)
            print(e)
            file_uuid = None
            batchname = None
            status = 'local'

    
        # update log
        update_log(
            log_location=log_location,
            namespace=namespace,
            version=version,
            shard=i,
            batch_fpath=will_write_to,
            model_name=model_name,
            llm_provider=llm_provider,
            prompt=prompt,
            structured=structured,
            file_uuid=file_uuid,
            batch_uuid=batchname,
            status=status,
            try_to_overwrite=try_to_overwrite
        )

if __name__ == '__main__':
    raise NotImplementedError("This script is only an example.")
    process_env('.env')

    llm_provider = 'openai'
    model_name, suggested_prompt, structured = glean_model_name('baba-t2neboth')
    

    main(
        namespace='',
        pdf_root='pdfs/general',
        micro_path='.enzy/pre/mM/mM.parquet',
        tables_from='.enzy/pre/tables/markdown',
        dest_folder='.enzy/batches',
        log_location='.enzy/llm_log.tsv',
        model_name=model_name,
        llm_provider=llm_provider,
        prompt=suggested_prompt,
        structured=structured,
    )