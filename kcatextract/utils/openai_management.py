import openai
import os
import json

from tqdm import tqdm

_openai_client = None
def process_env(filepath):
    global _openai_client
    with open(filepath, 'r') as f:
        for line in f:
            key, value = line.strip().split('=', 1)
            if key == 'OPENAI_API_KEY':
                _openai_client = openai.OpenAI(api_key=value)
                break
        else:
            raise ValueError("No OpenAI key found!")
                
def get_openai_client():
    global _openai_client
    if _openai_client is None:
        _openai_client = openai.OpenAI()
        if _openai_client.api_key is None:
            raise ValueError("No OpenAI key found!")
    return _openai_client


def submit_batch_file(filepath):
    openai_client = get_openai_client()
    # read to make sure
    assert filepath.endswith('.jsonl')
    with open(filepath, 'r') as f:
        count = sum(1 for _ in f)
    print(f"Batch of {count} items at {filepath} ready for submission. Submit to OpenAI?")
    if input("Proceed? (y/n): ").lower() == 'y':
        with open(filepath, 'rb') as f:
            batch_input_file = openai_client.files.create(
                file=f,
                purpose="batch"
            )
        batch_input_file_id = batch_input_file.id

        batch_confirm = openai_client.batches.create(
            input_file_id=batch_input_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={
                "filepath": filepath
            }
        )
        
        # get the id
        batch_id = batch_confirm.id
        print("Submitted as batch", batch_id)
        
        return batch_id
        
        # client.batches.list(limit=10)
        
        # check status: 
        # batch_status = client.batches.retrieve(batch_id)
    else:
        print("Aborted.")

def get_last_n_batches(n: int = 10):
    openai_client = get_openai_client()
    last_n = openai_client.batches.list(limit=n)
    
    # for some reason, this method tends to give all batches
    for i, batch in enumerate(last_n):
        yield batch
        if i >= 9:
            break

def preview_batches_uploaded():
    
    for batch in get_last_n_batches():
        print(f"{batch.id}, {batch.status}, {batch.metadata}")
        
def check_undownloaded(batch_id_to_orig_name: dict[str, str] | None = None, path_to_pending:str = 'batches/pending.jsonl', 
                       all_download_folders=['completions/enzy', 'completions/explode', 'C:/conjunct/table_eval/completions/enzy'], 
                       download_folder='completions/enzy',
                       printme=True, autodownload=True, y_for_autodownload=False):
    """
    
    :return: list of tuples (batch, name)
    If autodownload is True, then only the freshly downloaded batches are returned.
    """
    batches = []
    if batch_id_to_orig_name is None:
        batch_id_to_orig_name = {}
        if path_to_pending is not None and os.path.exists(path_to_pending):
            # read from pending.jsonl
            with open(path_to_pending, 'r') as f:
                for line in f:
                    obj = json.loads(line)
                    batch_id_to_orig_name[obj['output']] = obj['input']
        else:
            # take from the last 10 batches
            batches = list(get_last_n_batches())
            # try to retain the name from the metadata
            for batch in batches:
                name = batch.metadata.get('filepath', batch.metadata.get('description'))
                if name is None:
                    name = batch.id
                else:
                    # get the basename, remove the extension
                    name = os.path.basename(name)
                    name = os.path.splitext(name)[0]
                batch_id_to_orig_name[batch.id] = name
    
    def preferred_name(batch):
        return batch_id_to_orig_name.get(batch.id, batch.id + '_output')
    
    if not batches:
        batches = [get_openai_client().batches.retrieve(batch_id) for batch_id in batch_id_to_orig_name.keys()]
    
    
    
    downloaded_files = set()
    for fdr in all_download_folders:
        downloaded_files.update(os.listdir(fdr))
    
    
    print()
    print()
    pending = [batch for batch in batches if batch.status == 'in_progress']
    if printme:
        for batch in pending:
            print(f"[PENDING] {batch.id}, {batch.metadata}")
    
    
    undownloaded = [batch for batch in batches if batch.status == 'completed' and 
                    (preferred_name(batch) + '.jsonl' not in downloaded_files and
                     batch.id + '_output.jsonl' not in downloaded_files # legacy
                     )]
    
    if printme:
        for batch in undownloaded:
            print(f"[{batch.status}] {batch.id}, {batch.metadata}")
    
    if autodownload:        
        freshly_downloaded = []
        if len(undownloaded) == 0:
            print("All batches downloaded.")
        else:
            if not y_for_autodownload:
                y_for_autodownload = input("Download all undownloaded batches? (y/n): ").lower() == 'y'
            if y_for_autodownload:
                
                print(f"Downloading {len(undownloaded)} batches...")
                openai_client = get_openai_client()
                for batch in tqdm(undownloaded):

                    assert batch.status == 'completed'
                    
                    jsonl = openai_client.files.content(batch.output_file_id)
                    
                    correct_name = preferred_name(batch)
                    if os.path.exists(f'{download_folder}/{correct_name}.jsonl'):
                        print(f"File {correct_name}.jsonl already exists. Skipping.")
                        continue
                    with open(f'{download_folder}/{correct_name}.jsonl', 'wb') as f:
                        f.write(jsonl.content)
                    freshly_downloaded.append((batch, correct_name))
        return freshly_downloaded
    return [(batch, preferred_name(batch)) for batch in undownloaded]
    # undownloaded
    
    
            
            