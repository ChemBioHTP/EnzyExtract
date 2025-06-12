# working_enzy_table_md, but tableless

import io
from typing import Optional
import pandas as pd
import polars as pl
import pymupdf
from tqdm import tqdm

from enzyextract.pipeline.step1_run_tableboth import build_manifest, step1_main
from enzyextract.pre.xml.xml_format import get_pure_tables, process_xml
from enzyextract.submit.anthropic_management import to_anthropic_batch_request
from enzyextract.submit.batch_utils import to_openai_batch_request
from enzyextract.submit.openai_schema import to_openai_batch_request_with_schema
from enzyextract.utils.pmid_management import pmids_from_directory
from enzyextract.pre.xml.xml_cals import parse_cals_table


def step1c_create_xml_batch(
    *, 
    pdf_root: str, # read XMLs from (REPURPOSE)
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
    xml_root = pdf_root  # repurpose pdf_root to read XMLs
    
    target_pmids = acceptable_pmids = pmids_from_directory(xml_root)

    # Option 1: build manifest from PDFs
    if manifest_view is None:
        manifest_view = build_manifest(xml_root, file_ext='xml')

    batch = []
    correspondences = []
    for fileroot, filename, pmid in tqdm(manifest_view.iter_rows(), total=manifest_view.height):
        assert pmid in target_pmids
        
        filepath = fileroot + '/' + filename
        try:
            docs = process_xml(filepath, original_tables=None)
            if not docs:
                continue
        except Exception as e:
            print("Error opening", filepath)
            print(e)
            continue
        
        tables = get_pure_tables(filepath)
        table_docs = []
        for table in tables:
            html, text = parse_cals_table(table)
            content = ''
            if text:
                content = text + '\n\n'
                

            if html is not None:
                try:
                    df = pd.read_html(io.StringIO(html))
                    if df:
                        df = df[0]
                        content += df.fillna('').to_markdown() + '\n\n'
                except Exception as e:
                    print(e)
            table_docs.append(content)
        docs = table_docs + docs
        
        
        # now make a batch
        custom_id = f'{namespace}_{version}_{pmid}'
        if structured:
            req = to_openai_batch_request_with_schema(custom_id, prompt, docs,
                                                        model_name=model_name)
        else:
            req = to_openai_batch_request(custom_id, prompt, docs, 
                                    model_name=model_name)
        batch.append(req)
        correspondences.append({"custom_id": custom_id, "pmid": pmid})
    return batch, correspondences

def step1c_xml_main(
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
        chunk_size=1000,
        create_batch=step1c_create_xml_batch,
    )