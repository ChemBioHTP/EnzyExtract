# working_enzy_table_md, but tableless

import asyncio
import re
from typing import Optional
import polars as pl
import pymupdf
import glob
import os
from tqdm import tqdm

from enzyextract.pipeline.llm_log import read_log, update_log
from enzyextract.pre.table.reocr_for_gmft import load_correction_df
from enzyextract.submit.base import ReusePreference, SubmitPreference, VersioningPreference, check_file_destinations_and_ask, do_presubmit, get_user_versioning_preference
from enzyextract.submit.batch_utils import to_openai_batch_request, write_to_jsonl
from enzyextract.pre.reocr.micro_fix import duplex_mM_corrected_text
from enzyextract.submit.litellm_management import process_env, submit_litellm_batch_file
from enzyextract.utils.namespace_management import validate_namespace
from enzyextract.utils.pmid_management import pmids_from_directory
from enzyextract.utils.working import pmid_to_tables_from
from enzyextract.submit.openai_schema import to_openai_batch_request_with_schema
from enzyextract.pre.reocr.micro_fix import true_widest_mM_re



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

def find_previous_remnants(namespace, dest_folder, corresp_folder, *, version=None):
    """
    Find suspicious file paths (possible remnants of a previous run) in the dest_folder and corresp_folder.
    """
    if version and not isinstance(version, str):
        version = str(version)

    if version is None:
        query = f'{namespace}'
    else:
        query = f'{namespace}_{version}'
    suspicious_fpaths = []
    for fpath in glob.glob(f'{dest_folder}/{query}*.jsonl'):
        suspicious_fpaths.append(fpath)

    for fpath in glob.glob(f'{corresp_folder}/{query}*.parquet'):
        suspicious_fpaths.append(fpath)

    return suspicious_fpaths

def step1_create_batch(
    *, 
    pdf_root: str, # read pdfs from
    tables_from: Optional[str], # read tables from
    micro_path: str, # read micro corrections from
    manifest_view: Optional[pl.DataFrame], # use specific pmids

    namespace: str, # ids
    version: str, # ids
    model_name: str, # model settings
    prompt: str, # prompt settings
    structured: bool = False, # whether to use structured prompt or not
    
    _check_nonzero_tables=True, # validate that tables exist
    _check_nonzero_reocr=True, # validate that micro corrections exist
):
    batch = []
    correspondences = []

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
    micro_df = load_correction_df(micro_path, manifest_view['filename'].to_list())

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

        custom_id = f'{namespace}_{version}_{pmid}'
        if structured:
            req = to_openai_batch_request_with_schema(custom_id, prompt, docs,
                                                        model_name=model_name)
        else:
            req = to_openai_batch_request(custom_id, prompt, docs, 
                                    model_name=model_name)
        batch.append(req)
        correspondences.append({"custom_id": custom_id, "pmid": pmid})

    if _pmid_with_tables:
        print(f"Found {_pmid_with_tables} pmids with tables")
    else:
        print("WARNING: no pmids with tables")
    print("Using model", model_name)

    return batch, correspondences

def try_write_corr_df(corr_df, corresp_fpath, reuse_pref, _wrote_corr_yet):
    if _wrote_corr_yet:
        return False
    if corr_df is None:
        return False
    if reuse_pref == ReusePreference.OVERWRITE or not os.path.exists(corresp_fpath):
        corr_df.write_parquet(corresp_fpath)
    return True

def assign_new_serial_version(previous_log: pl.DataFrame):
    """
    assign a new serial version (ie. v1, v2, ...)
    """
    relevant = previous_log.filter(
        # pl.col('namespace') == namespace,
        pl.col('version').str.starts_with('v')
    ).select('version')

    # select the max plus 1, or 1 if none exist
    if relevant.height == 0:
        return 'v1'
    else:
        max_version = relevant.select(
            pl.col('version').str.strip_prefix('v').cast(pl.Int64).max()
        ).item(0, 'version')
        return f'v{max_version + 1}'


def get_previous_version(previous_log: pl.DataFrame, namespace: str):
    """
    get the highest previous version of a namespace
    """
    relevant = previous_log.filter(
        pl.col('namespace') == namespace,
        pl.col('version').str.starts_with('v')
    ).select('version')

    # select the max plus 1, or 1 if none exist
    if relevant.height == 0:
        return None
    else:
        max_version = relevant.select(
            pl.col('version').str.strip_prefix('v').cast(pl.Int64).max()
        ).item(0, 'version')
        return f'v{max_version}'



def step1_main(
    *, 
    namespace: str, # ids
    pdf_root: str, # read from
    micro_path: str,
    tables_from: Optional[str],
    model_name: str, # model settings
    prompt: str, 

    log_location: str,
    dest_folder: str, # write to
    corresp_folder: str, # write any data matching custom_id to 
    # *, 
    structured = False,
    llm_provider: str = 'openai',
    version=None,
    _check_nonzero_reocr=True,
    _check_nonzero_tables=True,
):
    
    process_env('.env')

    _should_exist = False # expect that the namespace has not been used before

    # if version is None:
    #     version = time.strftime("%m%d%H%M%S")
    # if not isinstance(version, str):
    #     version = str(version)
    previous_log = read_log(log_location)

    # do validation, making sure namespace is unique
    validate_namespace(namespace)
    previous_namespace = previous_log.filter(
        pl.col('namespace') == namespace
    ) # .select('namespace')

    new_version = assign_new_serial_version(previous_log)

    # only care about tracked files
    # unversioned_remnants = find_previous_remnants(namespace, dest_folder, corresp_folder)
    if previous_namespace.height > 0:
        previous_version = get_previous_version(previous_log, namespace)
        versioning_pref = get_user_versioning_preference(namespace, previous_version)
        if versioning_pref == VersioningPreference.PREVIOUS:
            # use the last provided version
            version = previous_version
            _should_exist = True
        elif versioning_pref == VersioningPreference.NEW:
            print(f"Assigning new version {new_version}.")
            version = new_version
        elif isinstance(versioning_pref, str) and versioning_pref.startswith('v'):
            # Secret: allow user to enter a custom version number
            version = versioning_pref
            print(f"Using custom version {version}.")
        else:
            print("Aborting.")
            return
    else:
        # nothing found, so assign a new version
        version = new_version

    # check to make sure that we don't overwrite anything
    remnants = find_previous_remnants(namespace, dest_folder, corresp_folder, version=version)
    if remnants:
        # remnants means that a previous run has been attempted
        if not _should_exist:
            raise FileExistsError(
                f"Found remnants of {namespace}_{version} in {dest_folder} or {corresp_folder}, " + 
                f"but we expected {version} to be newly assigned (unique)."
            )
        reuse_pref = check_file_destinations_and_ask(remnants)
    else:
        reuse_pref = ReusePreference.OVERWRITE
    
    if reuse_pref == ReusePreference.ABORT:
        print("Aborting submission.")
        return

    os.makedirs(dest_folder, exist_ok=True)
    os.makedirs(corresp_folder, exist_ok=True)

    print("Namespace: ", namespace)

    will_write_to = f'{dest_folder}/{namespace}_{version}.jsonl'

    if reuse_pref in [ReusePreference.OVERWRITE, ReusePreference.REUSE_AS_NEEDED]:
        # Overwrite (recalculate the files)
        batch, correspondences = step1_create_batch(
            pdf_root=pdf_root,
            tables_from=tables_from,
            micro_path=micro_path,
            manifest_view=None, # None means use all PDFs in pdf_root
            
            namespace=namespace,
            version=version,
            model_name=model_name,
            prompt=prompt,
            structured=structured,

            _check_nonzero_tables=_check_nonzero_tables,
            _check_nonzero_reocr=_check_nonzero_reocr,
        )
      
        # write in chunks

        chunk_size = 1000
        have_multiple = len(batch) > chunk_size # need to enforce chunk size, since OpenAI has data size limit
        need_to_submit = []
        for i in range(0, len(batch), chunk_size):
            chunk = batch[i:i+chunk_size]


            if have_multiple:
                will_write_to = f'{dest_folder}/{namespace}_{version}.{i}.jsonl'
            if reuse_pref == ReusePreference.OVERWRITE or not os.path.exists(will_write_to):
                write_to_jsonl(chunk, will_write_to)

            need_to_submit.append(will_write_to)
        
        corresp_fpath = f'{corresp_folder}/{namespace}_{version}.parquet'
        corr_df = pl.DataFrame(correspondences)
    else:
        # Reuse existing, completely 
        
        need_to_submit = []
        for fname in os.listdir(dest_folder):
            if fname.startswith(f"{namespace}_{version}") and fname.endswith('.jsonl'):
                need_to_submit.append(f'{dest_folder}/{fname}')
        print("Found", len(need_to_submit), "files to submit:", need_to_submit)

        corresp_fpath = f'{corresp_folder}/{namespace}_{version}.parquet'
        corr_df = None # nothing here, so do not try to overwrite
    
    
    

    print("Time to submit!")
    _wrote_corr = False
    for i, will_write_to in enumerate(need_to_submit):

        # special case with 1 shard
        if len(need_to_submit) == 1:
            i = None
        
        # read to make sure
        inp = do_presubmit(
            filepath=will_write_to,
            submit_suffix=f"Submit to {llm_provider}?",
        )
        
        if inp == SubmitPreference.REMOVE:
            print("Removing.")
            os.remove(will_write_to)
            # do NOT try_write_corr_df
            continue
        elif inp == SubmitPreference.UNTRACK:
            print("Saved untracked copy at", will_write_to)
            _wrote_corr |= try_write_corr_df(corr_df, corresp_fpath, reuse_pref, _wrote_corr)
            continue
            
        elif inp == SubmitPreference.YES:
            _wrote_corr |= try_write_corr_df(corr_df, corresp_fpath, reuse_pref, _wrote_corr)
            try:
            
                # batchname = submit_batch_file(will_write_to, pending_file='batches/pending.jsonl') # will ask for confirmation
                file_uuid, batchname = asyncio.run(submit_litellm_batch_file(will_write_to, custom_llm_provider=llm_provider))
                status = 'submitted'
            except Exception as e:
                print("Error submitting batch", will_write_to)
                print(e)
                file_uuid = None
                batchname = None
                status = 'local'
        elif inp == SubmitPreference.LOCAL:
            print("Tracked local copy at", will_write_to)
            _wrote_corr |= try_write_corr_df(corr_df, corresp_fpath, reuse_pref, _wrote_corr)
            file_uuid = None
            batchname = None
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

            file_uuid=file_uuid,
            batch_uuid=batchname,
            batch_fpath=will_write_to,
            corresp_fpath=corresp_fpath,
            # try to update (and replace) existing record if it had already existed
            replace_existing_record=_should_exist
        )
