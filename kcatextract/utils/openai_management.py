import glob
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


def submit_batch_file(filepath, pending_file=None):
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
        
        if pending_file is not None:
            with open(pending_file, 'a') as f:
                f.write(json.dumps({'input': filepath, 'output': batch_id}) + '\n')
        
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
        
def check_undownloaded(_all_batch2name: dict[str, str] | None = None, path_to_pending:str = 'batches/pending.jsonl', 
                       all_download_folders=['completions/enzy', 'completions/explode', 'completions/gptclose', 'completions/_cache', 'C:/conjunct/table_eval/completions/enzy',
                                             'completions/enzy/apogee'], 
                       download_folder='completions/enzy',
                       errors_folder='completions/errors',
                       printme=True, autodownload=True, y_for_autodownload=False):
    """
    
    :return: list of tuples (batch, name)
    If autodownload is True, then only the freshly downloaded batches are returned.
    """
    
    
    # downloaded files
    downloaded_files = set()
    for fdr in all_download_folders:
        downloaded_files.update(os.listdir(fdr))
        
    # determine the batch names in question
    batches = []
    if _all_batch2name is None:
        _all_batch2name = {}
        if path_to_pending is not None and os.path.exists(path_to_pending):
            # read from pending.jsonl
            with open(path_to_pending, 'r') as f:
                for line in f:
                    obj = json.loads(line)
                    
                    val = obj['input']
                    name = os.path.basename(val)
                    name = os.path.splitext(name)[0] # this preserves chunk names; ie. name = my_chunked_file.1000
                    _all_batch2name[obj['output']] = name
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
                    name = os.path.splitext(name)[0] # this preserves chunk names; ie. name = my_chunked_file.1000
                _all_batch2name[batch.id] = name
    
    def preferred_name(batch_id):
        return _all_batch2name.get(batch_id, batch_id + '_output')
    
    # we are only interested in batches not yet downloaded
    batch2name = {}
    for batch_id, name in _all_batch2name.items():
        if preferred_name(batch_id) + '.jsonl' not in downloaded_files and \
                     batch_id + '_output.jsonl' not in downloaded_files:
            batch2name[batch_id] = name
    
    if not batches:
        batches = [get_openai_client().batches.retrieve(batch_id) for batch_id in batch2name.keys()]
    
    
    print()
    print()
    pending = [batch for batch in batches if batch.status == 'in_progress']
    if printme:
        for batch in pending:
            print(f"[PENDING] {batch.id}, {batch.metadata}")
    
    
    undownloaded = [batch for batch in batches if batch.status == 'completed' and 
                    (preferred_name(batch.id) + '.jsonl' not in downloaded_files and
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
                    
                    correct_name = preferred_name(batch.id)
                    if os.path.exists(f'{download_folder}/{correct_name}.jsonl'):
                        print(f"File {correct_name}.jsonl already exists. Skipping.")
                        continue
                    with open(f'{download_folder}/{correct_name}.jsonl', 'wb') as f:
                        f.write(jsonl.content)
                    freshly_downloaded.append((batch, correct_name))
                    
                    # deposit the errors
                    if batch.errors:
                        with open(f'{errors_folder}/{correct_name}.err.jsonl', 'w') as f:
                            # f.write(json.dumps(batch.errors.data))
                            for error in batch.errors.data:
                                f.write(json.dumps(error) + '\n')
        return freshly_downloaded
    return [(batch, preferred_name(batch.id)) for batch in undownloaded]
    # undownloaded
    
    
            
def merge_chunked_completions(namespace, version, compl_folder='completions/enzy', dest_folder='completions/enzy'):
    """
    Merge all the chunked files into one.
    """
    # match namespace_version.\d+.jsonl but not namespace_version.jsonl
    
    chunked = glob.glob(f'{compl_folder}/{namespace}_{version}.*.jsonl')
    if len(chunked) == 0:
        print("No chunked files found.")
        return
    chunked.sort()
    print(f"Found {len(chunked)} chunked files.")
    
    if os.path.exists(f'{dest_folder}/{namespace}_{version}.jsonl'):
        print("Destination file already exists. Aborting.")
        return
    with open(f'{dest_folder}/{namespace}_{version}.jsonl', 'w', encoding='utf-8') as f:
        for chunk in chunked:
            with open(chunk, 'r', encoding='utf-8') as c:
                f.write(c.read())
    print("Merged to", f'{dest_folder}/{namespace}_{version}.jsonl')

def merge_all_chunked_completions(compl_folder, dest_folder):
    """Look for all chunked files and merge them."""
    
    chunked = glob.glob(f'{compl_folder}/*_*.*.jsonl')
    # also make sure that the conglomerated files do not exist
    valid = set()
    for chunk in chunked:
        basename = os.path.basename(chunk)
        
        if basename.count('.') < 2:
            # not a chunked file
            continue
        namespace_version, ending = basename.split('.', 1)
        chunkno, _ = ending.split('.', 1)
        
        if not chunkno.isdigit():
            print("Invalid chunked file", chunk)
            continue
        if not os.path.exists(f'{dest_folder}/{namespace_version}.jsonl'):
            valid.add(namespace_version)
    
    print("Detected the following namespaces to merge:")
    for namespace_version in sorted(valid):
        print(namespace_version)
    
    
    # asser validity of namespaces
    for namespace_version in valid:
        namespace, version = namespace_version.rsplit('_', 1)
        assert version.isdigit()
    
    if input("Proceed? (y/n): ").lower() == 'y':
        for namespace_version in valid:
            namespace, version = namespace_version.rsplit('_', 1)
            # assert version.isdigit()
            merge_chunked_completions(namespace, version, compl_folder=compl_folder, dest_folder=dest_folder)
    

    