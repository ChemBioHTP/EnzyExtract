import os
import litellm
import polars as pl

from enzyextract.submit.litellm_management import process_env

def download(
    log_location: str,
    dest_folder: str,
    err_folder: str,
):
    
    os.makedirs(dest_folder, exist_ok=True)
    os.makedirs(err_folder, exist_ok=True)

    # Check to see which files are missing
    df = pl.read_parquet(log_location) # .with_row_index('index')

    to_download = df.filter(
        pl.col('status') == 'submitted'
    )

    updates = []
    for namespace, version, batch_id, llm_provider in to_download.select([
        # 'index',
        'namespace',
        'version',
        'batch_uuid',
        'llm_provider'
    ]).iter_rows():
        write_dest = f"{dest_folder}/{namespace}_{version}.jsonl"
        # check if the file already exists
        if os.path.exists(write_dest):
            print(f"File {write_dest} already exists, skipping download.")
            continue
        retrieved_batch = litellm.retrieve_batch(
            batch_id=batch_id,
            custom_llm_provider=llm_provider,
        )
        if retrieved_batch.status == 'completed':
            # download the file
            file = litellm.file_content(
                file_id=retrieved_batch.output_file_id,
                llm_provider=llm_provider,
            )
            
            with open(write_dest, 'wb') as f:
                f.write(file.content)
                print(f"Downloaded {namespace}_{version}.jsonl")
            
            err_file_id = retrieved_batch.error_file_id
            if err_file_id:
                err_file = litellm.file_content(
                    file_id=err_file_id,
                    llm_provider=llm_provider,
                )
                err_dest = f"{err_folder}/{namespace}_{version}.jsonl"
                with open(err_dest, 'wb') as f:
                    f.write(err_file.content)
                print(f"Downloaded error file {namespace}_{version}_error.jsonl")
            updates.append({
                'namespace': namespace,
                'version': version,
                'completion_fpath': write_dest,
                'status': 'downloaded',
            })
            
    
    # update the log file
    updates_df = pl.DataFrame(updates)
    df = df.update(updates_df, on=['namespace', 'version'], how='left')
    df.write_parquet(log_location)



if __name__ == "__main__":
    raise NotImplementedError("This script is only an example.")
    process_env('.env')
    download(
        log_location=".enzy/llm_log.parquet",
        dest_folder=".enzy/completions",
        err_folder=".enzy/errors",
    )
