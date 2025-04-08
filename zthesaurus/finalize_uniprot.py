import polars as pl
from tqdm import tqdm
from enzyextract.thesaurus.enzyme_io import read_all_dfs
import json
import os

from enzyextract.utils.pl_utils import wrap_pbar
import rapidfuzz
from tqdm import tqdm

from enzyextract.thesaurus.fuzz_utils import compute_fuzz_with_progress
from enzyextract.thesaurus.organism_patterns import pl_fix_organism


def extract_uniprotkb_fields(entry_str: str, query_uniprot: str=None):
    """
    expects these keys in results:
    ['query_uniprot', 'uniprot', 'uniprot_aliases', 'enzyme_name', 'organism', 'organism_common', 'sequence', 'ec_numbers', 'pmids', 'dois', 'uniparc', 'why_deleted', 'full_response']
    """

    entry = json.loads(entry_str)
    enzyme = entry.get('proteinDescription', {}).get('recommendedName', {}).get('fullName', {}).get('value', None)
    if enzyme is None:
        enzyme = (entry.get('proteinDescription', {}).get('submissionNames') or [{}])[0].get('fullName', {}).get('value', None)
    if enzyme is None:
        enzyme = (entry.get('proteinDescription', {}).get('alternativeNames') or [{}])[0].get('fullName', {}).get('value', None)
    
    why_deleted = None
    uniprot_aliases = entry.get('secondaryAccessions', [])
    if entry.get('entryType') == 'Inactive':
        inactiveReasonType = entry.get('inactiveReason', {}).get('inactiveReasonType', None)
        if inactiveReasonType == 'DELETED':
            why_deleted = entry.get('inactiveReason', {}).get('deletedReason', 'deleted')
        elif inactiveReasonType == 'DEMERGED':
            why_deleted = 'demerged'
            for demerged in entry.get('inactiveReason', {}).get('mergeDemergeTo', []):
                uniprot_aliases.append(demerged)
        elif inactiveReasonType == 'MERGED':
            why_deleted = 'merged'
            for merged in entry.get('inactiveReason', {}).get('mergeDemergeTo', []):
                uniprot_aliases.append(merged)
        else:
            why_deleted = 'unknown'
    

    ec_numbers = []
    for submission in entry.get('proteinDescription', {}).get('submissionNames', []):
        ec = submission.get('ecNumbers', [])
        ec = [x.get('value') for x in ec]
        ec_numbers += ec

    dois = []
    pmids = []
    for lit in entry.get('references', []):
        if 'citation' in lit:
            if 'citationCrossReferences' in lit['citation']:
                for xref in lit['citation']['citationCrossReferences']:
                    if xref['database'] == 'DOI':
                        dois.append(xref['id'])
                    elif xref['database'] == 'PubMed':
                        pmids.append(xref['id'])



    return {
        'query_uniprot': query_uniprot,
        'uniprot': entry.get('primaryAccession', None),
        'uniprot_aliases': uniprot_aliases,
        'enzyme_name': enzyme,
        'organism': entry.get('organism', {}).get('scientificName', None),
        'organism_common': entry.get('organism', {}).get('commonName', None),
        'sequence': entry.get('sequence', {}).get('value', None),
        'ec_numbers': ec_numbers,
        'uniparc': entry.get('extraAttributes', {}).get('uniParcId', None),
        'why_deleted': why_deleted,
        'dois': dois,
        'pmids': pmids,

        'recommended_name': (
            entry.get('proteinDescription', {}).get('recommendedName', {})
            .get('fullName', {}).get('value', None)
        ),
        'submission_names': ([
            x.get('fullName', {}).get('value', None) 
            for x in entry.get('proteinDescription', {}).get('submissionNames', [])
        ]),
        'alternative_names': ([
            x.get('fullName', {}).get('value', None) 
            for x in entry.get('proteinDescription', {}).get('alternativeNames', [])
        ]),
    }

    # enzyme = entry.get('proteinDescription', {}).get('recommendedName', {}).get('fullName', {}).get('value', None)
    # if enzyme is None:
    #     enzyme = (entry.get('proteinDescription', {}).get('submissionNames') or [{}])[0].get('fullName', {}).get('value', None)
    # if enzyme is None:
    #     enzyme = (entry.get('proteinDescription', {}).get('alternativeNames') or [{}])[0].get('fullName', {}).get('value', None)
    # results['enzyme_name'].append(enzyme)

def reconvert_from_json(df):
    """
    Given a DataFrame with a column 'full_response' that contains JSON strings, 
    convert it back to a DataFrame with fields re-extracted.
    """
    # df = read_all_dfs('data/enzymes/accessions/uniprot')

    with tqdm(total=df.height) as pbar:
        df = df.with_columns([
            pl.col('full_response').map_elements(
                wrap_pbar(pbar, extract_uniprotkb_fields),
                return_dtype=pl.Struct
            ).alias('parsed')
        ])
    df = df.unnest('parsed').drop('full_response')
    # print(df)
    return df

def script_searched_gen2():
    """
    Compresses searched/gen2
    Compresses data from C:/conjunct/enzy_runner/data/enzymes/accessions/uniprot_searched/gen2
    """
    dfs = []
    for filename in os.listdir('data/enzymes/accessions/uniprot_searched/gen2'):
        df_raw = (
            pl.scan_parquet(f'data/enzymes/accessions/uniprot_searched/gen2/{filename}')
            .select('full_response')
            .collect()
        )
        df = reconvert_from_json(df_raw)
        dfs.append(df)
    df = pl.concat(dfs, how='diagonal')
    return df

def script_searched_gen2():
    """
    Compresses searched/gen2
    Compresses data from C:/conjunct/enzy_runner/data/enzymes/accessions/uniprot_searched/gen2
    """
    dfs = []
    for filename in os.listdir('data/enzymes/accessions/uniprot_searched/gen2'):
        df_raw = (
            pl.scan_parquet(f'data/enzymes/accessions/uniprot_searched/gen2/{filename}')
            .select('full_response', 'query_organism', 'query_enzyme')
            .collect()
        )
        df = reconvert_from_json(df_raw)
        dfs.append(df)
    df = pl.concat(dfs, how='diagonal')
    return df

def _fix_dumb_search_blunder(df):
    """
    oops, typo wrong column
    """

    df_normal = df.filter(
        pl.col('query_organism').is_not_null()
    )
    df_exception = df.filter(
        pl.col('query_organism').is_null()
    )
    df_exception = df_exception.with_columns(
        pl.col('organism').alias('query_organism'),
        pl.col('enzyme_preferred').alias('query_enzyme'),
        pl.lit(None).alias('organism'),
        pl.lit(None).alias('enzyme_preferred'),
    )
    df = pl.concat([df_normal, df_exception], how='diagonal').drop('enzyme_preferred')

    tried_queries = df.select(['query_organism', 'query_enzyme']).unique().height
    print(tried_queries, "tried queries")
    df.write_parquet('data/enzymes/accessions/uniprot_searched/gen2.parquet')


def script_cited_gen1():
    """
    Compresses cited/gen1
    """
    dfs = []
    root = 'data/enzymes/accessions/uniprot/gen1'
    for filename in os.listdir(root):
        df_raw = (
            pl.scan_parquet(f'{root}/{filename}')
            .select('full_response')
            .collect()
        )
        df = reconvert_from_json(df_raw)
        dfs.append(df)
    df = pl.concat(dfs, how='diagonal_relaxed')
    return df



def generate_searched_chapter():

    # df = read_all_dfs('data/enzymes/accessions/uniprot_searched/gen1')
    # df.write_parquet('data/enzymes/accessions/uniprot_searched/gen1.parquet')
    df1 = pl.read_parquet('data/enzymes/accessions/uniprot_searched/gen1.parquet').rename({
        'protein_name': 'recommended_name',
        'accession': 'uniprot'
    })
    df2 = pl.read_parquet('data/enzymes/accessions/uniprot_searched/gen2.parquet')
    df = pl.concat([df1, df2], how='diagonal')

    df = df.with_columns(
        pl_fix_organism(pl.col('query_organism')).alias('query_organism_fixed'),
    )

    # calculate similarity between query_enzyme and protein_name
    # similarities = df.select(['query_enzyme', 'enzyme_name']).unique()
    # def similarity(x):
    #     query_enzyme = x['query_enzyme']
    #     protein_name = x['enzyme_name']
    #     out = rapidfuzz.fuzz.partial_ratio(query_enzyme, protein_name) # 0 to 100
    #     return out
    
    comparisons = [
        # ('enzyme_preferred', 'enzyme_name', False, 'similarity_enzyme_name'),
        ('query_organism_fixed', 'organism', False, 'similarity_organism'),
        ('query_organism', 'organism_common', False, 'similarity_organism_common'),
        ('query_enzyme', 'recommended_name', False, 'similarity_recommended_name'),
        ('query_enzyme', 'submission_names', False, 'similarity_submission_names'),
        ('query_enzyme', 'alternative_names', False, 'similarity_alternative_names'),
    ]

    df = compute_fuzz_with_progress(df, comparisons).with_columns(
        pl.max_horizontal(
            pl.col(f"similarity_organism"),
            pl.col(f"similarity_organism_common"),
        ).alias('max_organism_similarity'),
        pl.max_horizontal(
            pl.col(f"similarity_recommended_name"),
            pl.col(f"similarity_submission_names"),
            pl.col(f"similarity_alternative_names"),
        ).alias('max_enzyme_similarity'),
    )

    # df = df.with_columns([
    #     (pl.col('max_organism_similarity').fill_null(50) + pl.col('max_enzyme_similarity').fill_null(0)).alias('total_similarity')
    # ]).sort('total_similarity', descending=True).unique('query_organism', 'query_enzyme', keep='first')

    acceptable = df.filter(
        (pl.col('max_organism_similarity') >= 80) & (pl.col('max_enzyme_similarity') >= 60)
    )
    acceptable = acceptable.with_columns([
        (pl.col('max_organism_similarity').fill_null(50) + pl.col('max_enzyme_similarity').fill_null(0)).alias('total_similarity')
    ]).sort('total_similarity', descending=True).unique(['query_organism', 'query_enzyme'], keep='first')
    pass
    return acceptable


    # with tqdm(total=similarities.height) as pbar:
    #     similarities = similarities.with_columns([
    #         pl.struct(
    #             pl.col('query_enzyme'),
    #             pl.col('enzyme_name')
    #         ).map_elements(
    #             wrap_pbar(pbar, similarity),
    #             return_dtype=pl.Float64
    #         ).alias('similarity')
    #     ])
    # df = df.join(similarities, on=['query_enzyme', 'enzyme_name'], how='left')

    # # now for each query_enzyme, get the best accession, organism, protein_name, sequence
    # df = df.sort('similarity', descending=True)
    # optimal_similarity = df.group_by(['query_enzyme', 'query_organism'], maintain_order=True).agg(
    #     pl.col('enzyme_name').first(),
    #     pl.col('organism').first(),
    #     pl.col('uniprot').first(),
    #     pl.col('sequence').first(),
    #     pl.col('similarity').first()
    # )

    # acceptable = optimal_similarity.filter(pl.col('similarity') > 60) # 60 is arbitrary 
    # acceptable = acceptable.rename({
    #     # 'query_enzyme': 'enzyme',
    #     # 'query_organism': 'organism',
    #     # 'organism': 'alt_organism',
    #     # 'protein_name': 'alt_enzyme',
    #     # 'accession': 'uniprot',
    #     # 'sequence': 'sequence',
    #     # 'similarity': 'similarity'
    # })
    
    # acceptable = acceptable.with_columns([
    #     pl.lit('searched').alias('enzyme_source')
    # ])

    # # print(optimal_similarity)
    # return acceptable
    
if __name__ == "__main__":
    # df = script_searched_gen2()
    # df.write_parquet('data/enzymes/accessions/uniprot_searched/gen2.parquet')

    # df = script_cited_gen1()
    # df.write_parquet('data/enzymes/accessions/uniprot/all_cited.parquet')

    searched = generate_searched_chapter()
    searched.write_parquet('data/thesaurus/enzymes/uniprots_searched.parquet')