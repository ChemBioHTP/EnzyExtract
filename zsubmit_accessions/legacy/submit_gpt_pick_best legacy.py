# first, get a list of all enzyme names we need disambiguated

import polars as pl
from tqdm import tqdm
from enzyextract.prompts.ask_best_uniprots import pick_uniprot, PickAccessionSchema
from enzyextract.submit.batch_utils import chunked_write_to_jsonl
from enzyextract.submit.openai_management import submit_batch_file
from enzyextract.submit.openai_schema import to_openai_batch_request_with_schema

df = pl.read_parquet('data/valid/_valid_with_duplicates.parquet')

infos = df.select([
    'pmid',
    'enzyme',
    'enzyme_full',
    'organism'
]).unique()

infos = infos.filter(
    pl.col('enzyme').is_not_null()
    | pl.col('enzyme_full').is_not_null()
).with_row_index('index')


uniprot_all = pl.read_parquet('data/enzymes/accessions/final/uniprot.parquet')
uniprot_all = uniprot_all.unique('uniprot')
doc2accessions = pl.read_parquet('data/enzymes/sequence_scans/latest_sequence_scans.parquet')
doc2accessions.sort('pmid')
doc2accessions.set_sorted('pmid') # presumably make pmid access O(logn)

def to_target(enzyme, enzyme_full, organism, df_of_accessions):
    
    if enzyme and '\n' in enzyme:
        enzyme = enzyme.replace('\n', '')
        print("Newline in", enzyme)
    if enzyme_full and '\n' in enzyme_full:
        enzyme_full = enzyme_full.replace('\n', '')
        print("Newline in", enzyme_full)
    if organism and '\n' in organism:
        organism = organism.replace('\n', '')
        print("Newline in", organism)

    builder = f"Target Enzyme: {enzyme}\n"
    if enzyme_full:
        builder += f"Target Fullname: {enzyme_full}\n"
    if organism:
        builder += f"Target Organism: {organism}\n"
        
    for uniprot, enzyme_name, organism, organism_common in \
        df_of_accessions.select(['uniprot', 'enzyme_name', 'organism', 'organism_common']).iter_rows():

        builder += f"{uniprot}: {enzyme_name}"

        organism_str = organism
        if organism_common:
            organism_str += f" ({organism_common})"

        if organism_str:
            builder += f" from {organism_str}"
        builder += '\n'
    return builder

doc2accessions = doc2accessions.group_by('pmid').agg(
    pl.col('pdb').drop_nulls().flatten().unique(),
    pl.col('uniprot').drop_nulls().flatten().unique(),
    pl.col('refseq').drop_nulls().flatten().unique(),
    pl.col('genbank').drop_nulls().flatten().unique(),
).select([
    'pmid', 'pdb', 'uniprot', 'refseq', 'genbank'
]) # should be only 1 pmid

# join to accessions
infos = infos.join(doc2accessions, left_on='pmid', right_on='pmid')

# infos = infos.head(100)

namespace = 'pick-uniprot-prod1'
batch = []
for i, pmid, enzyme, enzyme_full, organism, \
    pdbs, uniprots, refseqs, genbanks in tqdm(infos.iter_rows(), total=infos.height):
    custom_id = f'{namespace}_{i}_{pmid}'

    

    # TODO add the actual accessions
    if not uniprots:
        continue
    uniprot_df = pl.DataFrame({'uniprot': uniprots})

    uniprot_df = uniprot_df.join(uniprot_all, left_on='uniprot', right_on='uniprot', how='inner')
    uniprot_df = uniprot_df.filter(
        pl.col('sequence').is_not_null()
    )
    if uniprot_df.is_empty():
        continue

    # refseq_df = desired_accessions.select('refseq').explode('refseq').drop_nulls()


    doc = to_target(enzyme, enzyme_full, organism, uniprot_df)        

    docs = [doc]
    req = to_openai_batch_request_with_schema(
        uuid=custom_id,
        system_prompt=pick_uniprot,
        docs=docs,
        model_name='gpt-4o',
        schema=PickAccessionSchema
    )
    batch.append(req)


dest_filepath = f'batches/pick/{namespace}.jsonl' # + namespace + '.jsonl'

print("Have", len(batch), "entries")
chunk_dests = chunked_write_to_jsonl(batch, dest_filepath, 10000)
for chunk_dest in chunk_dests:
    try:
        batchname = submit_batch_file(chunk_dest, pending_file='batches/pending.jsonl') # will ask for confirmation
    except Exception as e:
        print("Error submitting batch", chunk_dest, e)
    
# print(infos)