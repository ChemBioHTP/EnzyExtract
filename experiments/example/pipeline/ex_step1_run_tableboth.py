
from enzyextract.pipeline.step1_run_tableboth import process_env, glean_model_name, step1_main

if __name__ == '__main__':
    raise NotImplementedError("This script is only an example.")
    process_env('.env')

    llm_provider = 'openai'
    model_name, suggested_prompt, structured = glean_model_name('baba-t2neboth')
    
    namespace = 'my-namespace-here' # no colons: needs to be a valid file name
    pdf_root = 'D:/papers'
    enzy_root = 'D:/MyExtractionRun/.enzy'
    step1_main(
        namespace=namespace,
        pdf_root=pdf_root,
        micro_path=f'{enzy_root}/pre/mM/mM.parquet',
        tables_from=f'{enzy_root}/pre/tables/markdown',

        dest_folder=f'{enzy_root}/batches',
        corresp_folder=f'{enzy_root}/corresp',
        log_location=f'{enzy_root}/llm_log.tsv',
        model_name=model_name,
        llm_provider=llm_provider,
        prompt=suggested_prompt,
        structured=structured,
    )