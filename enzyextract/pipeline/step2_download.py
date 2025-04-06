import os
from typing import Optional, Union
from enzyextract.pipeline.llm_log import write_log
import litellm
import polars as pl
from google.cloud import storage

from enzyextract.submit.anthropic_management import retrieve_anthropic_batch, retrieve_anthropic_results
from enzyextract.submit.base import LLMCommonBatch
from enzyextract.submit.litellm_management import process_env
from enzyextract.pipeline.llm_log import llm_log_schema, read_log
import requests


def download_gcs_file(gcs_url, destination_file_name):
    """
    Funny enough, litellm does not support GCS.
    Download a file from GCS to local storage."""
    # gcs_uri = "gs://MyProject/litellm-vertex-files/publishers/...
    assert gcs_url.startswith("gs://")
    gcs_url = gcs_url[5:]
    parts = gcs_url.split('/', 1)
    assert len(parts) == 2, "Invalid GCS URL: should be gs://bucket_name/file_path"
    bucket_name = parts[0]
    source_blob_path = parts[1]
    

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_path)

    # blob.download_as_bytes()
    blob.download_to_filename(destination_file_name)
    
    print(f"Downloaded {source_blob_path} to {destination_file_name}")


def retrieve_my_batch(
    batch_id: str,
    llm_provider: str = 'openai',
) -> Union[litellm.LiteLLMBatch, LLMCommonBatch]:
    if llm_provider == 'anthropic':
        return retrieve_anthropic_batch(batch_id)
    return litellm.retrieve_batch(
        batch_id=batch_id,
        custom_llm_provider=llm_provider,
    )

def retrieve_my_file(
    batch_id: Optional[str], 
    file_id: str,
    llm_provider: str = 'openai',
):
    if llm_provider == 'anthropic':
        return retrieve_anthropic_results(batch_id, file_id, to_json_response=True)

    else:
        return litellm.file_content(
            file_id=file_id,
            custom_llm_provider=llm_provider,
        )



def download(
    log_location: str,
    dest_folder: str,
    err_folder: str,
):
    
    os.makedirs(dest_folder, exist_ok=True)
    os.makedirs(err_folder, exist_ok=True)

    # Check to see which files are missing
    # df = pl.read_parquet(log_location) # .with_row_index('index')
    df = read_log(log_location)

    to_download = df.filter(
        pl.col('status') == 'submitted'
    )

    updates = []
    for namespace, version, shard, batch_id, llm_provider in to_download.select([
        # 'index',
        'namespace',
        'version',
        'shard', 
        'batch_uuid',
        'llm_provider'
    ]).iter_rows():
        if shard is not None:
            write_dest = f"{dest_folder}/{namespace}_{version}.{shard}.jsonl"
        else:
            write_dest = f"{dest_folder}/{namespace}_{version}.jsonl"
        # check if the file already exists
        if os.path.exists(write_dest):
            print(f"File {write_dest} already exists, skipping download.")
            updates.append({
                'namespace': namespace,
                'version': version,
                'shard': shard,
                'completion_fpath': write_dest,
                'status': 'downloaded',
            })
            continue
        retrieved_batch = retrieve_my_batch(
            batch_id=batch_id,
            llm_provider=llm_provider,
        )
        if retrieved_batch.status == 'completed':
            # download the file
            output_file_id = retrieved_batch.output_file_id
            if output_file_id.startswith("gs://"):
                # rip, litellm does not support GCS
                output_file_id = output_file_id + '/predictions.jsonl'
                download_gcs_file(output_file_id, write_dest)
            else:
                
                # openai
                file = retrieve_my_file(
                    batch_id=batch_id,
                    file_id=output_file_id,
                    llm_provider=llm_provider,
                )
                
                with open(write_dest, 'wb') as f:
                    f.write(file.content)
                    print(f"Downloaded {namespace}_{version}.jsonl")
            
            err_file_id = retrieved_batch.error_file_id
            if err_file_id:
                err_file = retrieve_my_file(
                    batch_id=batch_id,
                    file_id=err_file_id,
                    custom_llm_provider=llm_provider,
                )
                err_dest = f"{err_folder}/{namespace}_{version}.jsonl"
                with open(err_dest, 'wb') as f:
                    f.write(err_file.content)
                print(f"Downloaded error file {namespace}_{version}_error.jsonl")
            updates.append({
                'namespace': namespace,
                'version': version,
                'shard': shard,
                'completion_fpath': write_dest,
                'status': 'downloaded',
            })
            
    
    # update the log file
    updates_df = pl.DataFrame(updates, schema_overrides=llm_log_schema)
    if updates_df.height == 0:
        print("No new files to download.")
        return
    
    _updates_sharded = updates_df.filter(pl.col('shard').is_not_null())
    _updates_unsharded = updates_df.filter(pl.col('shard').is_null())
    df = df.update(_updates_sharded, on=['namespace', 'version', 'shard'], how='left')
    df = df.update(_updates_unsharded, on=['namespace', 'version'], how='left') # cannot update on null
    # df = df.update(updates_df, on=['namespace', 'version', 'shard'], how='left')
    write_log(df, log_location)



if __name__ == "__main__":
    raise NotImplementedError("This script is only an example.")
    process_env('.env')
    download(
        log_location=".enzy/llm_log.tsv",
        dest_folder=".enzy/completions",
        err_folder=".enzy/errors",
    )
