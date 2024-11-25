# Purpose: look at how a document was ingested by GPT, to see why some documents fare better than others

from kcatextract.utils.locate_batch import locate_by_pmid
from kcatextract.utils.construct_batch import get_batch_input
from kcatextract.utils.construct_batch import pmid_from_usual_cid

# recover namespace, if unknown

pmid = '10801893'
ingest_folder = 'batches/enzy'

known_filename = None
filename_whitelist = None

filename_whitelist = lambda x: x.startswith('beluga')
# known_namespace = 'brenda-asm-apogee-t2neboth'
# known_version = '1'
# known_filename = f"brenda-jbc-apogee-t2neboth_1.0.jsonl"

# pmid = '10029307'
# ingest_folder = f"C:/conjunct/table_eval/batches/enzy"
# known_filename = f"rekcat-giveboth-4o_2.jsonl"


if known_filename is None:
    for filename, namespace, version in locate_by_pmid(ingest_folder, pmid, iscompletion=False, filename_whitelist=filename_whitelist):

        print(filename, namespace, version)
        known_namespace = namespace
        known_version = version
        known_filename = filename
        # open it up

# now, we may open up the file

def debug_pmid_into_file(known_filepath, pmid):
    content = get_batch_input(known_filepath)

    for cid, docs in content:
        thepmid = pmid_from_usual_cid(cid)
        if thepmid != pmid:
            continue

        # for doc in docs:
        #     print(doc)
        with open(f'dev/data/debugviewer.md', 'w', encoding='utf-8') as f:
            pass
        with open(f'dev/data/debugviewer.md', 'a+', encoding='utf-8') as f:
            f.write(f"# {pmid}\n")
            for i, doc in enumerate(docs):
                f.write(f"## {i}\n")
                f.write(doc)

debug_pmid_into_file(f"{ingest_folder}/{known_filename}", pmid)