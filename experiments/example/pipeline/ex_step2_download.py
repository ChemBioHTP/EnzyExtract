from enzyextract.pipeline.step2_download import process_env, download

if __name__ == "__main__":
    # raise NotImplementedError("This script is only an example.")
    process_env('.env')
    download(
        log_location=".enzy/llm_log.tsv",
        dest_folder=".enzy/completions",
        err_folder=".enzy/errors",
    )
