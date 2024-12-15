
import json
import os
from enzyextract.utils.construct_batch import preview_batches_in_folder
from enzyextract.utils.openai_management import process_env, preview_batches_uploaded, check_undownloaded

# from generate_bmatched import run_stats
def script0():
    # preview_batches_in_folder('C:/conjunct/table_eval/batches/enzy', 'C:/conjunct/table_eval/completions/enzy', undownloaded_only=True)
    
    # view pending batches
    process_env('.env')
    # preview_batches_uploaded()
    
    # check undownloaded
    redownloaded = check_undownloaded(autodownload=True, path_to_pending='batches/pending.jsonl') # , path_to_pending=None)
    
    if len(redownloaded) > 0:
        print("Detected Batches:")
        for batch, name in redownloaded:
            if name != batch.id:
                print(f"{name} was {batch.id}.")
            else:
                print(name)
        # print("Would you like to evaluate these? (y/n)")
        # if input() != 'y':
        #     exit(0)
        
        # # evaluate
        # for batch, name in redownloaded:
        #     namespace, version = name.rsplit('_', 1)
        #     run_stats(namespace=namespace, version=version, compl_folder='completions/enzy')
    

def script1():
    pass
    # just submit a few
    from enzyextract.utils.openai_management import process_env, submit_batch_file
    # stragglers = ['batches/enzy/wos-open-apogee-t2neboth_1.7000.jsonl', 'batches/enzy/wos-open-apogee-t2neboth_1.8000.jsonl']
    # stragglers = ['batches/enzy/brenda-hindawi-apogee-t2neboth_1.jsonl']

    splits = [0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]
    splits = [split + 30000 for split in splits]
    for split in splits:
        filedest = f'batches/enzy/bucket/openelse-bucket-md-t2neboth_1.{split}.jsonl'
    # for filedest in stragglers: # ['batches/enzy/scratch-open-apogee-t2neboth_2.0.jsonl', 'batches/enzy/scratch-open-apogee-t2neboth_2.1000.jsonl', 'batches/enzy/scratch-open-apogee-t2neboth_2.2000.jsonl']:	
        batchname = submit_batch_file(filedest, pending_file='batches/pending.jsonl') # will ask for confirmation
        print(batchname)

def script2(path_to_dir, pending_file='batches/pending.jsonl'):
    """rename hashed batch file names"""
    with open(pending_file, 'r') as f:
        pending = [json.loads(line) for line in f]
    pending = {obj['output']: obj['input'] for obj in pending}
    
    for filename in os.listdir(path_to_dir):
        error_mode = False
        if filename.endswith('_error.jsonl'):
            batch_id = filename[:-len('_error.jsonl')]
            error_mode = True
        elif filename.endswith('.jsonl'):
            batch_id = filename[:-len('.jsonl')]
        else:
            batch_id = filename
        if batch_id in pending:
            basename = os.path.basename(pending[batch_id])
            if basename.endswith('.jsonl'):
                basename = basename[:-len('.jsonl')]
            if error_mode:
                os.rename(f'{path_to_dir}/{filename}', f'{path_to_dir}/{basename}.err.jsonl')
            else:
                os.rename(f'{path_to_dir}/{filename}', f'{path_to_dir}/{basename}.jsonl')

def script_download_all_errors():
    # download all errors, because oops I forgot
    from enzyextract.utils.openai_management import iter_all_error_files
    from tqdm import tqdm
    for want_name, content in tqdm(iter_all_error_files()):
        with open(f'completions/errors/length/{want_name}.err.jsonl', 'wb') as f:
            f.write(content)

if __name__ == '__main__':
    # script2(path_to_dir='completions/errors')
    # script1()
    # script0()

    script_download_all_errors()
