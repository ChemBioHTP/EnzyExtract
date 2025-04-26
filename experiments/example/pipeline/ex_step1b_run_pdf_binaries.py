

from enzyextract.pipeline.step1b_run_pdf_binaries import process_env, glean_model_name
from enzyextract.pipeline.step1b_run_pdf_binaries import main as step1b_main


if __name__ == '__main__':
    # raise NotImplementedError("This script is only an example.")
    process_env('.env')

    llm_provider = 'openai'
    model_name, suggested_prompt, structured = glean_model_name('baba-t2neboth')
    
    namespace = 'my-namespace-here' # no colons: needs to be a valid file name
    pdf_root = 'D:/papers'
    enzy_root = 'D:/MyExtractionRun/.enzy'
    step1b_main(
        namespace=namespace,
        pdf_root=pdf_root,
        dest_folder=f'{enzy_root}/batches',
        log_location=f'{enzy_root}/llm_log.tsv',
        model_name=model_name,
        llm_provider=llm_provider,
        prompt=suggested_prompt,
        structured=structured,
    )