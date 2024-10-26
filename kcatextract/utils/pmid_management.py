import os

from kcatextract.utils.fresh_version import latest_version, next_available_version


def pmids_from_file(filename) -> set[str]:
    with open(filename) as f:
        return set(f.read().splitlines())

def pmids_from_directory(filepath, recursive=False, filetype='.pdf') -> set[str]:
    """Searches directory for PDF files and returns their PMIDs"""
    result = set()
    for root, dirs, files in os.walk(filepath):
        for filename in files:
            if filename.endswith(filetype): # '.pdf'):
                pmid = filename.rsplit('.', 1)[0]
                result.add(pmid)
        if not recursive:
            break
    return result
    

def pmids_from_batch(path_to_jsonl) -> set[str]:
    # supports both input and output batches
    
    id_prefix  = '{"id": "batch_req_cOTEwOobeyeib0QWIhpo7PWQ"'
    cid_prefix = ', "custom_id": "'
    
    result = set()
    with open(path_to_jsonl, 'r') as f:
        # pmid_from_usual_custom_id: int(uuid.split('_', 2)[2])
        lines = f.readlines()
    if not lines:
        return set()
    if lines[0].startswith('{"custom_id": "'):
        # mode = 'input'
        start = len('{"custom_id": "')
    elif lines[0].startswith('{"id": "batch_req_'):
        # mode = 'output'
        start = len('{"id": "batch_req_cOTEwOobeyeib0QWIhpo7PWQ", "custom_id": "')
    else:
        raise ValueError("Not a valid batch file: " + path_to_jsonl)
    
    for line in lines:
        custom_id = line[start:line.index('"', start)]
        assert custom_id.count('_') >= 2, f"Actually, custom_id is {custom_id}"
        namespace, version, pmid = custom_id.split('_', 2)
        assert pmid.isdigit(), f"Actually, pmid is {pmid}"
        result.add(pmid)
    return result

def cache_pmids_to_disk(pmids, namespace, version=None, parent_dir='C:/conjunct/vandy/yang/corpora/manifest/auto'):

    if version is None:
        version = next_available_version(parent_dir, namespace, '.txt')
    with open(f"{parent_dir}/{namespace}_{version}.txt", 'w') as f:
        for pmid in pmids:
            f.write(str(pmid) + '\n')

def pmids_from_cache(namespace, version=None, parent_dir='C:/conjunct/vandy/yang/corpora/manifest/auto') -> set[str]:

    if version is None:
        if os.path.exists(f"{parent_dir}/{namespace}.txt"):
            return pmids_from_file(f"{parent_dir}/{namespace}.txt")
        version = latest_version(parent_dir, namespace, '.txt')
    if version is None:
        raise ValueError(f"No cache found for {namespace}")
    return pmids_from_file(f"{parent_dir}/{namespace}_{version}.txt")

def cache_directory_to_disk_manifest(parent_dir, write_dir='C:/conjunct/vandy/yang/corpora/manifest/auto', recursive=False):
    """Takes all the pdfs in a directory, and writes it into a manifest
    For instance, this would write these: 
    C:/.../manifest/auto/brenda_wiley_1.txt
    C:/.../manifest/auto/brenda_open_1.txt
    C:/.../manifest/auto/scratch_wiley_1.txt
    """
    
    overall_name = os.path.basename(parent_dir)
    
    for root, dirs, files in os.walk(parent_dir):
        # first, get pmids in the same-level as we walk
        result = []
        for filename in files:
            if filename.endswith('.pdf'):
                pmid = filename.rsplit('.', 1)[0]
                result.append(pmid)
        if result:
            # save to 
            rel_dir = os.path.relpath(root, parent_dir)
            namespace = rel_dir.replace('\\', '_').replace('/', '_')
            # if rel_dir is empty, we are at the top level
            if not namespace or namespace == '.': # top level
                cache_pmids_to_disk(result, namespace=overall_name, parent_dir=f"{write_dir}")
            else:
                os.makedirs(f"{write_dir}/{overall_name}", exist_ok=True)
                cache_pmids_to_disk(result, namespace=namespace, parent_dir=f"{write_dir}/{overall_name}")
        if not recursive:
            break

def lift_pmids(pmids, walk_dir, write_dir, extension='.pdf'):
    """
    From PMIDs stored in a recursive directory structure walk_dir, 
    elevate them to a flat structure in write_dir.

    Simply copy over the files that match the PMIDs.
    No need to provide the original location of the PMIDs.
    """
    if not pmids:
        print("No PMIDs to lift")
        return

    if isinstance(pmids[0], (int, float)):
        print("Warning: PMIDs should be strings, not integers or floats")
        pmids = [str(int(pmid)) for pmid in pmids]

    import shutil
    os.makedirs(write_dir, exist_ok=True)

    for root, dirs, files in os.walk(walk_dir):
        result = []
        for filename in files:
            if filename.endswith(extension):
                pmid = filename.rsplit('.', 1)[0]
                if pmid in pmids:
                    # copy over the file
                    shutil.copyfile(f"{root}/{filename}", f"{write_dir}/{filename}")
                    


def _run_tests():
    pmids = pmids_from_directory("D:/brenda/wiley")
    assert len(pmids) == 1772, len(pmids)
    assert sorted(pmids)[0] == '10074715', sorted(pmids)[0]
    
    pmids = pmids_from_file("backform/finetunes/tableless-oneshot.train.txt")
    assert len(pmids) == 92, len(pmids)
    assert sorted(pmids)[0] == '10074080', sorted(pmids)[0]
    
    pmids = pmids_from_batch("batches/enzy/tableless-oneshot_1.jsonl")
    assert len(pmids) == 1108, len(pmids)
    assert sorted(pmids)[0] == '1001678', sorted(pmids)[0]
    
    pmids = pmids_from_batch("completions/enzy/batch_nGafHlk7f4pMqlPNT3T09YIs_output.jsonl")
    assert len(pmids) == 1016, len(pmids)
    assert sorted(pmids)[0] == '1001678', sorted(pmids)[0]
    
    print("All tests pass")


    
if __name__ == "__main__":
    # cache_directory_to_disk_manifest("C:/conjunct/tmp/brenda_rekcat_pdfs", recursive=False)
    pmids = pmids_from_cache("brenda_rekcat_pdfs")
    print("Hits:", len(pmids))