import pandas as pd

from kcatextract.metrics.get_perfects import get_perfects_only
from kcatextract.utils.construct_batch import get_batch_input, pmid_from_usual_cid

a = 'openelse-brenda-md-4o'
b = 'openelse-brenda-xml-4o'


a_df = pd.read_csv(f'data/mbrenda/_cache_{a}_1.csv', dtype={'pmid': str})
b_df = pd.read_csv(f'data/mbrenda/_cache_{b}_1.csv', dtype={'pmid': str})

common_pmids = set(a_df['pmid']) & set(b_df['pmid'])
a_df = a_df[a_df['pmid'].isin(common_pmids)]
b_df = b_df[b_df['pmid'].isin(common_pmids)]

pmids_a_correct = get_perfects_only(a_df)['pmid'].unique()
pmids_b_correct = get_perfects_only(b_df)['pmid'].unique()

pmids_a_superior = set(pmids_a_correct) - set(pmids_b_correct)
pmids_b_superior = set(pmids_b_correct) - set(pmids_a_correct)

print("A superior:", len(pmids_a_superior), pmids_a_superior)
print("B superior:", len(pmids_b_superior), pmids_b_superior)

i = 6
print(f"Getting B superior #{i}")

pmid = sorted(pmids_b_superior)[i]
print(f"AKA pmid {pmid}")


def debug_pmid_into_file(known_filepath, write_dest, pmid):
    # assert known_filepath == f"batches/enzy/openelse-brenda-md-4o_1.jsonl"
    content = get_batch_input(known_filepath)

    for cid, docs in content:
        thepmid = pmid_from_usual_cid(cid)
        if thepmid != pmid:
            continue

        # for doc in docs:
        #     print(doc)
        
        with open(write_dest, 'w+', encoding='utf-8') as f:
            f.write(f"# {pmid}\n")
            for i, doc in enumerate(docs):
                f.write(f"## {i}\n")
                f.write(doc)
    

debug_pmid_into_file(f"batches/enzy/openelse-brenda-md-4o_1.jsonl", 'dev/data/debugviewer.md', pmid)
debug_pmid_into_file(f"batches/enzy/openelse-brenda-xml-4o_1.jsonl", 'dev/data/debugviewer.xml', pmid)

a_df = a_df[a_df['pmid'] == pmid]
b_df = b_df[b_df['pmid'] == pmid]

view = ['km_feedback', 'kcat_feedback', 'kcat', 'kcat_2', 'km', 'km_2', 'enzyme', 'substrate']
print(a_df[view])
print(b_df[view])