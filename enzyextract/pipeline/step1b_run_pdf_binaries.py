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
from enzyextract.pipeline.step1_run_tableboth import build_manifest, step1_main
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


def step1b_create_batch(
    *, 
    pdf_root: str, # read pdfs from
    tables_from: Optional[str], # read tables from (IGNORE)
    micro_path: str, # read micro corrections from (IGNORE)
    manifest_view: Optional[pl.DataFrame], # use specific pmids

    namespace: str, # ids
    version: str, # ids
    model_name: str, # model settings
    prompt: str, # prompt settings
    structured: bool = False, # whether to use structured prompt or not
    
    _check_nonzero_tables=False, # validate that tables exist (IGNORE)
    _check_nonzero_reocr=False, # validate that micro corrections exist (IGNORE)
): 
    target_pmids = acceptable_pmids = pmids_from_directory(pdf_root)

    # Option 1: build manifest from PDFs
    if manifest_view is None:
        manifest_view = build_manifest(pdf_root)
    
    # Option 2: use given manifest
    # manifest = pl.read_parquet('data/manifest.parquet')
    # # only readable
    # manifest = manifest.with_columns([
    #     pl.col('filename').str.replace('\.pdf$', '').alias('pmid')
    # ])
    # manifest_view = manifest.filter(
    #     pl.col('readable')
    #     & ~pl.col('bad_ocr')
    #     & pl.col('pmid').is_in(target_pmids)
    # ).unique('filename').select(['fileroot', 'filename', 'pmid'])

    batch = []
    correspondences = []
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
    return batch, correspondences

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
    llm_provider: str = 'anthropic',
    version=None,
    save_as_jsonl=False,
):
    return step1_main(
        namespace=namespace,
        pdf_root=pdf_root,
        micro_path=None,
        tables_from=None,

        model_name=model_name,
        prompt=prompt,
        log_location=log_location,
        dest_folder=dest_folder,
        corresp_folder=corresp_folder,
        structured=structured,
        llm_provider=llm_provider,
        version=version,

        _check_nonzero_tables=False,  # validate that tables exist (IGNORE)
        _check_nonzero_reocr=False,  # validate that micro corrections exist (IGNORE)
        chunk_size=100,
        create_batch=step1b_create_batch,
    )