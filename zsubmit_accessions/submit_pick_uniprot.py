# first, get a list of all enzyme names we need disambiguated

import polars as pl
from tqdm import tqdm
from enzyextract.prompts.ask_best_uniprots import pick_accessions, PickAccessionSchema, PickAccessionSchemaAdditional
from enzyextract.submit.batch_utils import chunked_write_to_jsonl
from enzyextract.submit.openai_management import submit_batch_file
from enzyextract.submit.openai_schema import to_openai_batch_request_with_schema



def _clean_uniprot_str(x):
    if x:
        x = x.replace('\n', '')
        # if x.isupper():
            # return x.lower()
    return x
def to_target(enzyme, enzyme_full, organism, df_of_accessions):
    """
    Produces a document for GPT.
    """
    # pdb, descriptor, name, sys_name, organism_right, info
    
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
    builder += '\n\n'
        
    for uniprot, enzyme_name, organism_right, organism_common in \
        df_of_accessions.select(['uniprot', 'enzyme_name', 'organism_right', 'organism_common']).iter_rows():

        # if all uppercase, then lowercase it

        enzyme_name = _clean_uniprot_str(enzyme_name)
        organism_right = _clean_uniprot_str(organism_right)
        organism_common = _clean_uniprot_str(organism_common)
        
        # sys_name is most often EC number, not useful
        builder += f"{uniprot}: {enzyme_name}"
        if organism_right or organism_common:
            builder += f" from"
            if organism_right:
                builder += f" {organism_right}"
            if organism_common:
                builder += f" ({organism_common})"
        builder += '\n'
    return builder

batch = []
# for i, pmid, enzyme, enzyme_full, organism, \
    # pdb, descriptor, name, sys_name, organism_right, info in tqdm(
namespace = 'pick-uniprot-prod2'
read_from = f'data/ingest/pick/{namespace}.parquet'
info_view = pl.read_parquet(read_from)

# perfect_pdb = info_view.filter((pl.col('max_enzyme_similarity') >= 90) & (pl.col('similarity_organism') >= 95))
# imperfect_pdb = info_view.filter((pl.col('max_enzyme_similarity') < 90) | (pl.col('similarity_organism') < 95))
# imperfect_pdb = imperfect_pdb.join(perfect_pdb, on='index', how='anti') # perfect pdb no longer needs to be matched.

info_view = info_view.with_columns([
    # give unknown organisms a neutral value
    # only relevant when PDB does not give organism
    (pl.col('max_enzyme_similarity').fill_null(0) + pl.col('max_organism_similarity').fill_null(50)).alias('total_similarity')
])
# Mean enzyme similarity:  61.750864798010674
# GOAL: to put non-organism ahead of non-matches, but behind any close match
# can be validated by looking at the imperfect_pdb histogram
print("Mean organism similarity: ", info_view.filter(
    pl.col('max_organism_similarity') < 90
)['max_organism_similarity'].mean())
if '-dev' in namespace:
    print("RUNNING DEV MODE!")
    # sample 100 indices
    _indices = info_view['index'].sample(100).to_list()
    info_view = info_view.filter(pl.col('index').is_in(_indices))
for i, df in tqdm(
        info_view.partition_by('index', as_dict=True).items(), total=info_view['index'].n_unique()
    ):
    df = df.sort('total_similarity', descending=True)
    if (df.height > 12):
        if (df['organism'].drop_nulls().len()) and (df['organism_right'].drop_nulls().len() != df.height):
            pass
    df = df.head(12)

    pmid = df['pmid'][0]
    enzyme = df['enzyme'][0]
    enzyme_full = df['enzyme_full'][0]
    organism = df['organism'][0]
    i = i[0] # untuple
    custom_id = f'{namespace}_{i}_{pmid}'

    # pdb, descriptor, name, sys_name, organism_right, info
    # TODO add the actual accessions

    # refseq_df = desired_accessions.select('refseq').explode('refseq').drop_nulls()


    doc = to_target(enzyme, enzyme_full, organism, df)        

    docs = [doc]
    req = to_openai_batch_request_with_schema(
        uuid=custom_id,
        system_prompt=pick_accessions,
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