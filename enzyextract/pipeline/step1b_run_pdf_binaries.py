# working_enzy_table_md, but tableless

import asyncio
import base64
import hashlib
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
from anthropic.types.messages.batch_create_params import Request

from enzyextract.pipeline.llm_log import read_log, update_log
from enzyextract.pipeline.step1_run_tableboth import build_manifest
from enzyextract.submit.anthropic_management import submit_anthropic_batch_file, to_anthropic_batch_request
from enzyextract.submit.base import SubmitPreference, do_presubmit
from enzyextract.utils import prompt_collections
from enzyextract.submit.batch_utils import to_openai_batch_request, write_to_jsonl
from enzyextract.utils.fresh_version import next_available_version
from enzyextract.pre.reocr.micro_fix import duplex_mM_corrected_text
from enzyextract.submit.litellm_management import process_env
from enzyextract.utils.namespace_management import glean_model_name, validate_namespace
from enzyextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_directory
from enzyextract.utils.working import pmid_to_tables_from
from enzyextract.utils.yaml_process import get_pmid_to_yaml_dict
from enzyextract.submit.openai_schema import to_openai_batch_request_with_schema
from enzyextract.pre.reocr.micro_fix import true_widest_mM_re, ends_with_ascii_control_re




def step1b_main(
    *, 
    namespace: str, # ids
    pdf_root: str, # read from
    model_name: str, # model settings
    prompt: str, 

    log_location: str,
    dest_folder: str, # write to
    corresp_folder: str, 
    
    structured = False,
    llm_provider: str = 'openai',
    version=None,
    save_as_jsonl=False,
):
    
    process_env('.env')

    try_to_overwrite = False

    if version is None:
        version = time.strftime("%m%d%H%M%S")
    if not isinstance(version, str):
        version = str(version)
    previous_log = read_log(log_location)

    # do validation, making sure namespace is unique
    validate_namespace(namespace)
    previous_namespace = previous_log.filter(
        pl.col('namespace') == namespace
    ) # .select('namespace')
    if previous_namespace.height > 0:
        print(f"Namespace {namespace} has already been tried, are you sure? Type 'previous' to reuse the old batch. Type 'overwrite' to overwrite the old batch.")
        yn = input("p?: ")
        if yn == 'previous' or yn == 'p':
            version = previous_namespace.item(0, 'version')
            try_to_overwrite = True
        elif yn == 'overwrite' or yn == 'o':
            print("Overwriting previous batch.")
            
        else:
            return

    os.makedirs(dest_folder, exist_ok=True)
    batch: list[Request] = []
    correspondences = []

    print("Namespace: ", namespace)

    will_write_to = f'{dest_folder}/{namespace}_{version}.jsonl'

    if os.path.exists(will_write_to):
        print(f"File {will_write_to} already exists, skipping.")

        need_to_submit = [will_write_to]
        i = 0
    else:
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

        for fileroot, filename, pmid in tqdm(manifest_view.iter_rows(), total=manifest_view.height):
            assert pmid in target_pmids
            
            fpath = fileroot + '/' + filename
            try:
                doc = pymupdf.open(fpath)
            except Exception as e:
                print("Error opening", fileroot)
                print(e)
                continue
            
            if len(doc) > 100:
                # 100 pages is excessive
                continue

            # obtain original annotation from part A
            # use the table_md_root

            custom_id = f'{namespace}_{version}_{pmid}'
            if structured:
                raise NotImplementedError("Structured mode is not implemented yet.")
            else:
                req = to_anthropic_batch_request(
                    custom_id, 
                    prompt, 
                    pdf_fpath=fpath, 
                    model_name=model_name)
            batch.append(req)
            correspondences.append({"custom_id": custom_id, "pmid": pmid})

        print("Using model", model_name)
        # write in chunks
        # need to enforce chunk size, because there is data size limit
        chunk_size = 100
        have_multiple = len(batch) > chunk_size 
        need_to_submit = []
        for i in range(0, len(batch), chunk_size):
            chunk = batch[i:i+chunk_size]


            if have_multiple:
                will_write_to = f'{dest_folder}/{namespace}_{version}.{i}.jsonl'

            if save_as_jsonl:
                write_to_jsonl(chunk, will_write_to)
                print(f"Wrote to {will_write_to}")
            need_to_submit.append(chunk)
            

    print("Time to submit!")
    for chunk in need_to_submit:
        
        # special case with 1 shard
        if len(need_to_submit) == 1:
            i = None
        
        inp = do_presubmit(
            count=len(chunk),
            submit_suffix="Submit to Anthropic?"
        )
        
        if inp == SubmitPreference.REMOVE:
            print("Removing.")
            # remove the file
            os.remove(will_write_to)
            continue
        elif inp == SubmitPreference.UNTRACK:
            print("Saved untracked copy at", will_write_to)
            continue
            
        elif inp == SubmitPreference.YES:
            try:
                batchname = submit_anthropic_batch_file(chunk)
                if i is None:
                    corresp_fpath = f'{corresp_folder}/{namespace}_{version}.parquet'
                else:
                    corresp_fpath = f'{corresp_folder}/{namespace}_{version}.{i}.parquet'
                corr_df = pl.DataFrame(correspondences)
                corr_df.write_parquet(corresp_fpath)
                status = 'submitted'
            except Exception as e:
                print("Error submitting batch", will_write_to)
                print(e)
                batchname = None
                corresp_fpath = None
                status = 'local'
        elif inp == SubmitPreference.LOCAL:
            print("Tracked local copy at", will_write_to)
            batchname = None
            corresp_fpath = None
            status = 'local'
        else:
            print("Unknown consent", inp, "exiting.")
            return

        # update log
        update_log(
            log_location=log_location,
            namespace=namespace,
            version=version,
            shard=i,
            status=status,

            model_name=model_name,
            llm_provider=llm_provider,
            prompt=prompt,
            structured=structured,

            file_uuid=None,
            batch_uuid=batchname,
            batch_fpath=will_write_to,
            corresp_fpath=corresp_fpath,
            replace_existing_record=try_to_overwrite
        )
