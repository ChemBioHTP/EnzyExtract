from enzyextract.utils.construct_batch import locate_correct_batch, pmid_from_usual_cid
import os

def locate_by_pmid(src_folder, pmid, namespace=None, version=None, iscompletion=True,
                   
                   filename_whitelist=None):
    candidates = sorted(os.listdir(src_folder), 
                           key=lambda x: os.path.getctime(os.path.join(src_folder, x)),
                           reverse=True)
    yielded = False
    for filename in candidates:
        if not filename.endswith('.jsonl'):
            continue
        if filename_whitelist is not None and not filename_whitelist(filename):
            continue
        with open(f'{src_folder}/{filename}', 'r') as f:
            for line in f:
                if iscompletion:
                    query = '", "custom_id": "'
                else:
                    query = '"custom_id": "'
                    
                if query not in line:
                    continue
                prefix = line.index(query) + len(query)
                final = line.index('"', prefix)
                
                cid = line[prefix:final]
                # get pmid
                # found_pmid = pmid_from_usual_cid(cid)
                found_namespace, found_version, found_pmid = cid.split('_', 2)
                if namespace is not None:
                    if found_namespace != namespace:
                        continue
                if version is not None:
                    if found_version != version:
                        continue
                if found_pmid == pmid:
                    yield filename, found_namespace, found_version
                    yielded = True
    if not yielded:
        raise FileNotFoundError(f"Could not find {pmid} in {src_folder}")