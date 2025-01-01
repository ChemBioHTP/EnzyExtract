import json
import os
import pandas as pd
import polars as pl
import re

from enzyextract.utils.pmid_management import cache_pmids_to_disk

brenda = None

def load_brenda():
    global brenda

    with open("C:/conjunct/vandy/yang/corpora/brenda/brenda_2023_1.json") as f:
        brenda = json.load(f)


def get_needles(needles):
    if needles == None:
        needles = ['turnover_number', 'km_value']
    good_refs = set()
    for ec in brenda['data']:
        for key in needles: # 'kcat_km',  'km_value', 
            for item in brenda['data'][ec].get(key, []):
                for ref in item['references']:
                    good_refs.add((ec, ref))
    good_references = []
    for ec, ref in good_refs:
        good_references.append((ec, ref, brenda['data'][ec]['references'][ref]))
    return good_references




def require_ref(row):
    # if the row has a comment, make sure that the comment cites the ref
    required = row['ref']
    # read citations in <1,2,3> format
    comments = row['comments']
    if comments == '':
        return True
    # look for the required reference
    myre = r"<[\d,]*>"
    found = re.findall(myre, comments)
    for f in found:
        inside_part = f[1:-1]
        splits = inside_part.split(',')
        if required in splits:
            return True
    if len(found) == 0:
        return True
    return False

def get_comment_citations(row):
    # because some comments do not cite self, we can't quite explode the comments until
    # we know that at least one comment cites the reference. 
    # thus, push all cited references into a list
    # if the row has a comment, make sure that the comment cites the ref
    # required = row['ref']
    # read citations in <1,2,3> format
    comments = row['comments']
    if comments == '':
        return []
    # look for the required reference
    myre = r"<[\d,]*>"
    found = re.findall(myre, comments)
    
    citations = []
    for f in found:
        inside_part = f[1:-1]
        citations.extend(inside_part.split(','))
    citations = set(citations)
    citations = [x.strip() for x in citations]
    return citations

def collect_from_brenda(good_pmids):
    """
    Reads from the brenda dict, and collects the data directly into a DataFrame.
    """
    brenda_refs = []

    for ec in brenda['data']: 
        for ref in brenda['data'][ec].get('references', {}):
            if 'pmid' in brenda['data'][ec]['references'][ref]:
                if brenda['data'][ec]['references'][ref]['pmid'] in good_pmids:
                    brenda_refs.append((ec, ref, brenda['data'][ec]['references'][ref]))
    found_stuff = []
    for ec, ref, obj in brenda_refs:
        for key in ['km_value', 'turnover_number', 'kcat_km']:
            for item in brenda['data'][ec].get(key, []):
                # citations = get_comment_citations({'comments': item.get('comment', '')})
                # this makes no difference
                if ref in item['references']: #  or ref in citations:
                    found_stuff.append((ec, ref, obj, key, item))
    # turn it into df
    prep_arr = []
    for found in found_stuff:
        ec, ref, obj, key, item = found
        num_range = str(item.get('min_value', '')) + ' -- ' + str(item.get('max_value', ''))
        if num_range == ' -- ':
            num_range = None
        prep_arr.append({'pmid': obj['pmid'],
                        'ec': ec, 
                        'ref': ref,
                        'key': key,  # "turnover_number", "kcat_km", "km_value"
                        'enzyme': brenda['data'][ec]['name'],
                        #  'aliases': brenda['data'][ec].get('synonyms', []),
                        'comments': item.get('comment', ""),
                        'substrate': item['value'], 
                        'num': item.get('num_value', None),
                        'num_range': num_range,
                        })
    df_all = pd.DataFrame(prep_arr)
    
    
    return df_all


    


def condense_rows(df_all):
    """
    Logic to separate the comments and then condense the rows. 
    (go from having kcat and km in separate rows to matching them and having them in the same row)
    """
    # separate the comments
    
    # get citations per comments
    df_all['citations'] = df_all.apply(get_comment_citations, axis=1)
    
    # outlier: when ref is not in citations, then that pmid does not reference itself
    df_no_comment = df_all[df_all['comments'] == '']
    
    df_self_strangers = df_all[df_all.apply(lambda x: x['comments'] != '' and x['ref'] not in x['citations'], axis=1)]
    print("These pmids do not reference themselves: ", str(set(df_self_strangers['pmid'])))
    
    # only explode comments that do cite itself, otherwise we'll lose data
    # df_all = df_all.assign(comments=df_all['comments'].str.split('; ')).explode('comments')
    df_reliable = df_all[df_all.apply(lambda x: x['ref'] in x['citations'], axis=1)] # if comments == '', it's impossible for it to have citations, so we're safe
    df_reliable = df_reliable.assign(comments=df_reliable['comments'].str.split('; ')).explode('comments')

    # make sure that the datapoint actually cites the reference which we're taking
    df_rematch = df_reliable[df_reliable.apply(require_ref, axis=1)]
    
    # then merge in the self strangers
    df_rematch = pd.concat([df_no_comment, df_rematch, df_self_strangers])
    
    # get excluded pmids; add them back to be safe
    excluded_pmids = set(df_all['pmid']) - set(df_rematch['pmid'])
    print("Furthermore, we have lost these PMIDs: ", str(excluded_pmids))
    df_excluded = df_all[df_all['pmid'].isin(excluded_pmids)]
    df_rematch = pd.concat([df_rematch, df_excluded])
    
    # condense num_range into num
    df_rematch['num'].fillna(df_rematch['num_range'], inplace=True)
    
    # Pivot the DataFrame to condense entries
    
    df_pivot = df_rematch.pivot_table(index=['pmid', 'ec', 'ref', 'enzyme', 'substrate', 'comments'],
                            columns='key',
                            values='num',
                            aggfunc='first').reset_index()
    # these pmids usually correspond to null values
    excluded_pmids = set(df_rematch['pmid']) - set(df_pivot['pmid'])

    # these pmids correspond to null values
    print("These pmids probably have only null values: ", str(excluded_pmids))
    print(str(set(df_rematch[df_rematch['pmid'].isin(excluded_pmids)]['num'])))
    return df_pivot, df_self_strangers

def split_mutants_are_combined():
    """
    example of issue: 
    #31# pH 7, 25ºC, wild type and D280 mutant, phosphoenolpyruvate-producing reaction <46>
    wild type and D280 mutant are combined in the same row
    """
    # this is a hard problem, 
    pass

def script_detect_combined_mutants():
    df = pd.read_csv('C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv')

    # get those with comments which matches this regex:
    sus_mutant_re = re.compile(r"(wild[ -]?type|WT|[A-Z]\d{2,3}[A-Z]|mutant),? and (wild[ -]?type|WT|[A-Z]\d{2,3}[A-Z]|mutant)")
    # sus_df = df[df['comments'].str.contains(sus_mutant_re)] # ValueError: Cannot mask with non-boolean array containing NA / NaN values
    sus_df = df[df['comments'].apply(lambda x: isinstance(x, str) and sus_mutant_re.search(x) != None)]
    print(sus_df['comments']) # only 356 rows

    # maybe exclude these pmids from evaluation?
    # because if the value for wild-type and mutant being is described in 1 line,
    # the cardinalities cannot be the same as ours, and it messes up hungarian
    # cache_pmids_to_disk(sus_df['pmid'], 'brenda_sus_combined_mutants')

def script_construct_brenda_df():
    load_brenda()

    good_km_kcat_references = get_needles(['turnover_number', 'km_value'])
    km_kcat_pmids = set([x['pmid'] for ec, refno, x in good_km_kcat_references if 'pmid' in x])
    df_km_kcat = collect_from_brenda(km_kcat_pmids)

    df_km_kcat_pivot, df_self_strangers = condense_rows(df_km_kcat)

    # write
    # df_km_kcat_pivot.to_csv('brenda_km_kcat_key_v3.csv', index=False)
    # df_self_strangers.to_csv('brenda_km_kcat_self_strangers.csv', index=False)

def script_generate_brenda_parquets():
    """
    Generate the BRENDA parquet files:
    - brenda_km_kcat_key_v2.parquet
    - brenda_enzyme_synonyms.parquet
    - brenda_proteins.parquet
    - brenda_references.parquet

    """
    # generate enzyme synonyms

    write_dest = 'data/brenda/brenda_enzyme_synonyms.parquet'
    # if os.path.exists(write_dest):
    #     return pl.read_parquet(write_dest)
    
    load_brenda()
    builder = {
        'ec': [],
        'name': [],
        'systematic_name': [],
        'synonyms': [],
    }
    organisms = {
        'ec': [],
        'protein_id': [],
        'organism_name': [],
        'organism_comments': [],
    }

    proteins = {
        'ec': [],
        'protein_id': [],
        # 'comment': [],
        # 'accessions': [],
        # 'source': [],
        'protein_data': [],

    }

    references = {
        'ec': [],
        'reference_id': [],
        'title': [],
        'authors': [],
        'pmids': [],
    }
    for ec in brenda['data']:

        data = brenda['data'][ec]
        builder['ec'].append(ec)
        builder['name'].append(data.get('name'))
        builder['systematic_name'].append(data.get('systematic_name'))

        for synonym in data.get('synonyms', []):
            for k, v in synonym.items():
                assert k in ['proteins', 'organisms', 'references', 'value', 'comment'], k

        builder['synonyms'].append(data.get('synonyms'))

        protein_ids_set = set()
        for organism_id, organism_data in data.get('organisms', {}).items():
            organisms['ec'].append(ec)
            organisms['protein_id'].append(organism_id)
            organisms['organism_name'].append(organism_data.get('value'))
            organisms['organism_comments'].append(organism_data.get('comment'))
            protein_ids_set.add(organism_id)

        for protein_id, protein_data in data.get('proteins', {}).items():
            proteins['ec'].append(ec)
            proteins['protein_id'].append(protein_id)
            proteins['protein_data'].append(protein_data)

            assert protein_id in protein_ids_set, f"Protein ID {protein_id} not found in organisms for EC {ec}"
            # for protein_datum in protein_data:
            #     for k, v in protein_datum.items():
            #         assert k in ['comment', 'accessions', 'source'], k
            #     proteins['ec'].append(ec)
            #     proteins['protein_id'].append(protein_id)
            #     proteins['comment'].append(protein_datum.get('comment'))
            #     proteins['accessions'].append(protein_datum.get('accessions'))
            #     proteins['source'].append(protein_datum.get('source'))
        
        for reference_id, reference_datum in data.get('references', {}).items():
            # for k, v in reference_datum.items():
            #     assert k in ['title', 'authors'], k
            references['ec'].append(ec)
            references['reference_id'].append(reference_id)
            references['title'].append(reference_datum.get('title'))
            references['authors'].append(reference_datum.get('authors'))
            references['pmids'].append(reference_datum.get('pmid'))


    synonyms_schema = pl.List(pl.Struct({
        "proteins": pl.List(pl.Utf8),      # List of strings or None
        "organisms": pl.List(pl.Utf8),    # List of strings or None
        "references": pl.List(pl.Utf8),   # List of strings or None
        "value": pl.Utf8,                  # Always a string
        "comment": pl.Utf8,                # Always a string
    }))
    df = pl.DataFrame(builder, schema_overrides={'synonyms': synonyms_schema})
    df.write_parquet(write_dest)

    organisms_df = pl.DataFrame(organisms)
    # organisms_df.write_parquet('data/brenda/brenda_organisms.parquet')

    protein_schema = pl.List(pl.Struct({
        "comment": pl.Utf8,
        "accessions": pl.List(pl.Utf8),
        "source": pl.Utf8,
    }))
    proteins_df = pl.DataFrame(proteins, schema_overrides={'protein_data': protein_schema})

    proteins_df = organisms_df.join(proteins_df, on=['ec', 'protein_id'], how='left')
    proteins_df.write_parquet('data/brenda/brenda_proteins.parquet')

    # join with organisms
    # proteins_df = proteins_df.join(organisms_df, on=['ec', 'protein_id'], how='left')
    

    references_df = pl.DataFrame(references)
    references_df.write_parquet('data/brenda/brenda_references.parquet')
    return df

def _to_parquet():
    brenda_df = pl.read_csv('data/brenda/brenda_km_kcat_key_v2.csv', schema_overrides={
        'pmid': pl.Utf8,
        'pH': pl.Utf8,
        'temperature': pl.Utf8,
        'turnover_number': pl.Utf8,
        'km_value': pl.Utf8,
        'kcat_km': pl.Utf8,
    })
    brenda_df.write_parquet('data/brenda/brenda_km_kcat_key_v2.parquet')

def script_identify_bad_mutants(brenda_df):
    """
    Identify bad mutants: those where where the pmid indicates a multiple mutant like
    D34A/R35Y, but the annotator does not use the forward slash "/" notation and 
    instead uses a comma like "D34A, R35Y", which is ambiguous because the comma
    notation could imply that the kcat value belongs to both the D34A and R35Y mutants
    separately.

    
    Prerequisite:
    - brenda_df: the old dataframe (data/brenda/brenda_km_kcat_key_v2.parquet) or the
    new dataframe (data/brenda/brenda_kcat_v3.parquet) will both do

    Returns this:
    - list of suspect pmids
    """
    mutants_by_pmid = brenda_df.group_by("pmid").agg(pl.col("mutant").drop_nulls())
    mutants_by_pmid = mutants_by_pmid.with_columns([
    #     pl.col("mutant").list.join("\t").alias("mutant")
        pl.col("mutant").replace([], None).alias("mutant")
    ])
    mutants_by_pmid = mutants_by_pmid.with_columns([
        pl.col("mutant").list.eval(pl.col("").str.contains(";")).list.any().alias("contains_semicolon"),
        pl.col("mutant").list.eval(pl.col("").str.contains("/")).list.any().alias("contains_slash"),
        pl.col("mutant").list.eval(
                pl.col("").str.extract_all(r"(\d{1,4})")
            ).alias("contains_multiple_mutants")
    ])
    mutants_by_pmid = mutants_by_pmid.with_columns([
        pl.col("contains_multiple_mutants").list.eval(
            pl.col("").list.n_unique() > 1
        ).list.any().alias("contains_multiple_mutants")
    ])
    suspect_mutant_pmids = mutants_by_pmid.filter(
        pl.col("contains_semicolon") &
        ~pl.col("contains_slash") &
        pl.col("contains_multiple_mutants")
    )

    # now explode the '; '. We can exclude the ones that do not contain duplicates, because
    # then "R45D, D56E" might actually correspond to two mutants
    explode_mutants = suspect_mutant_pmids.explode("mutant")
    explode_mutants = explode_mutants.unique(["pmid", "mutant"])

    # we don't need to count multiple mutants if they have the same number, since that means they must be mutually exclusive
    explode_mutants = explode_mutants.with_columns(
        pl.col("mutant").str.extract_all(r"[A-Z](\d{1,4})[A-Z]").alias("positions")
    )
    # explode_mutants

    explode_mutants = explode_mutants.with_columns(
        pl.col("mutant").str.split("; ").alias("mutant")
    ).explode("mutant")

    # if it is unduplicated, then that is a good sign because
    # then something like "R45D; D56E" might truly belong
    # to the mutants R45D and D56E separately, and not the
    # combination R45D/D56E (because if the combination)
    # is reported, then very likely each single mutant
    # will have been investigated

    # hence only look for duplicated mutants
    exploded_dup_mutants = explode_mutants.filter(
        pl.struct('pmid', 'mutant').is_duplicated()
        # explode_mutants.is_duplicated(["pmid", "mutant"])
    )
    return exploded_dup_mutants["pmid"].unique()
    # suspect_brenda = brenda_df.filter(
    #     pl.col("pmid").is_in(exploded_dup_mutants["pmid"])
    # )
    # # print(suspect_brenda)
    # return suspect_brenda

def script_construct_brenda_kcat():
    """
    End-to-end script to construct the brenda kcat dataset
    NOTE: temporarily, the starting point is the intermediate data/brenda/brenda_km_kcat_key_v2.parquet, 
    but in the future the starting point should be the JSON file.

    Prerequisites:
    - data/brenda/brenda_km_kcat_key_v2.parquet
    - data/brenda/brenda_proteins.parquet
    - data/brenda/brenda_references.parquet
    """
    pass

def script_apply_accessions_to_brenda():
    """
    Given BRENDA, apply these
    - protein ID
    - reference ID
    - accessions

    Prerequisites:
    - data/brenda/brenda_km_kcat_key_v2.parquet
    - data/brenda/brenda_proteins.parquet
    - data/brenda/brenda_references.parquet

    Generates these parquet files:
    # - brenda_sequenced.parquet (debug)
    - brenda_kcat_v3.parquet

    """
    old_brenda_df = pl.read_parquet('data/brenda/brenda_km_kcat_key_v2.parquet')
    # split comments by '; ' into their own rows

    brenda_df = old_brenda_df.with_columns(
        pl.col("comments").str.split("; ")
    ).explode("comments")

    brenda_df = brenda_df.with_columns([
        pl.col("comments").str.extract(r"#(\d+)#", 1).alias("protein_id"),
        pl.col("comments").str.extract(r"<(\d+(?:,\d+)*)>", 1).str.split(",").alias("reference_ids")
    ])

    protein_df = pl.read_parquet('data/brenda/brenda_proteins.parquet')

    # merge on ec and protein_id
    brenda_df = brenda_df.join(protein_df, on=["ec", "protein_id"], how="left")

    accessions_df = protein_df.explode("protein_data").with_columns(
        pl.col("protein_data").struct.field("accessions").alias("accessions")
    ).explode("accessions").drop_nulls("accessions")
    accessions_df = accessions_df.group_by("ec", "protein_id").agg(pl.col("accessions"))


    brenda_df = brenda_df.join(accessions_df, on=["ec", "protein_id"], how="left")
    # expand protein_data[].accessions

    # now expand references
    references_df = pl.read_parquet('data/brenda/brenda_references.parquet').rename({"pmids": "reported_pmid"})
    references_df.drop_in_place("authors")

    # explode reference_ids, while keeping a copy
    pmid2ref = brenda_df[['ec', 'pmid', 'reference_ids']].with_columns(
        pl.col("reference_ids").alias("reference_ids_to_split")
    )
    pmid2ref = pmid2ref.explode("reference_ids_to_split").rename({"reference_ids_to_split": "reference_id"})

    # map ec, reference_id to its reported_pmid
    pmid2ref = pmid2ref.join(references_df, on=["ec", "reference_id"], how="left")

    # require that the pmid match
    pmid2good = pmid2ref.filter(
        pl.col('pmid').is_null() |
        (pl.col('pmid') == pl.col('reported_pmid').cast(pl.Utf8))
    )

    # now aggregate the reference_ids where the pmids did indeed match
    pmid2good = pmid2good.group_by(['ec', 'pmid', 'reference_ids']).agg(
        pl.col("reference_id").drop_nulls().unique().alias("good_reference_ids")
    )

    # brenda_df = brenda_df.explode("reference_ids").rename({"reference_ids": "reference_id"})
    brenda_df = brenda_df.join(pmid2good, on=["ec", "pmid", "reference_ids"], how="left")

    brenda_df = brenda_df.with_columns(
        (pl.col("reference_ids").is_not_null() &
        pl.col("good_reference_ids").is_null()).alias("suspect_stranded")
    )

    # brenda_df.drop_in_place("stranded")
    brenda_df.drop_in_place("protein_data")
    # brenda_df.drop_in_place("reported_pmid")

    from enzyextract.hungarian.csv_fix import pl_widen_df
    brenda_df = pl_widen_df(brenda_df)
    print(brenda_df)

    suspect_pmids = script_identify_bad_mutants(brenda_df)
    # suspect_df = brenda_df.filter(pl.col("pmid").is_in(suspect_pmids))

    brenda_df = brenda_df.with_columns(
        pl.col("pmid").is_in(suspect_pmids).alias("suspect_mutant")
    )

    brenda_df = brenda_df.with_columns(
        pl.when(~pl.col("suspect_mutant"))  # Check if 'suspect_mutant' is False
        .then(pl.col("mutant").str.split("; "))     # Split 'mutant' on '; ' if condition is met
        .otherwise(pl.lit(None))                   # Otherwise set it to None
        .alias("mutant_split")                     # Create a new column with split results
    ).explode("mutant_split")                      # Explode the new column

    brenda_df = brenda_df.with_columns(
        pl.coalesce(pl.col("mutant_split"), pl.col("mutant")).alias("mutant")
    )
    brenda_df.drop_in_place("mutant_split")
    # 157130 -> 157576
    # weird = brenda_df.filter(
    #     pl.col("ref") != pl.col("reference_id").cast(pl.Int64)
    # )

    # multiple_valid_refs = brenda_df.group_by(["pmid", "ref"]).agg(pl.col("reference_id").drop_nulls().unique().alias("reference_id"))
    # multiple_valid_refs = multiple_valid_refs.filter(pl.col("reference_id").list.len() > 1)

    mistaken_ref = brenda_df.filter(
        pl.col("comments").is_not_null() &
        ~pl.col("good_reference_ids").list.contains(pl.col("ref").cast(pl.Utf8))
    )

    # print(brenda_df)
    
    brenda_slim_df = brenda_df.drop(["organism_comments"]) # , "title"])

    brenda_df.write_parquet('data/brenda/brenda_kcat_v3.parquet')
    brenda_slim_df.write_parquet('data/brenda/brenda_kcat_v3slim.parquet')

    # incorrectly uses comma to denote combination of mutants:
    # 7758465

    # suspect pmids are those defined as so:
    # 1. at least mutant has the character ";"
    # 2. no mutant has the character "/"



def obtain_existing_sequenced():
    """
    Prerequisites:
    - data/brenda/brenda_kcat_v3.parquet

    Generates these parquet files:
    - brenda_sequenced.parquet
    """

    brenda_df = pl.read_parquet('data/brenda/brenda_kcat_v3.parquet')
    existing_sequenced = brenda_df.filter(
        pl.col("accessions").is_not_null() &
        pl.col("turnover_number").is_not_null()
    )
    existing_sequenced.drop_in_place("protein_data")
    print(existing_sequenced.shape)
    existing_sequenced.write_parquet('data/brenda/brenda_sequenced.parquet')

def script_enzyme_name_to_ec():
    """
    Generate a mapping from enzyme name to EC number
    These parquet files are created:
    - brenda_to_ec.parquet
    - (no longer) runeem_20241125_ec.csv
    """
    # try to find ecs for either the enzyme or enzyme_full
    enzyme_df = pl.read_parquet('data/brenda/brenda_enzyme_synonyms.parquet')

    pure_name_to_ec = enzyme_df[['name', 'ec']].rename({"name": "alias"})

    systematic_name_to_ec = enzyme_df[['systematic_name', 'ec']].rename({"systematic_name": "alias"})

    enzyme_df = enzyme_df.explode("synonyms")
    enzyme_df = enzyme_df.with_columns(
        pl.col("synonyms").struct.field("value").alias("alias")
    )

    # also put in the enzyme name and systematic name into the "alias" column
    # enzyme_df = enzyme_df.with_columns(
    #     pl.col("alias").append(pl.col("name")).append(pl.col("systematic_name")).alias("alias")
    # )

    alias_df = enzyme_df[['alias', 'ec']]
    # concat alias_df, pure_name_to_ec, and systematic_name_to_ec
    alias_df = pl.concat([alias_df, pure_name_to_ec, systematic_name_to_ec]).drop_nulls("alias")

    alias_df = alias_df.with_columns([
        pl.col('ec').str.extract(r"(\d+\.\d+\.\d+)\.\d+").alias('enzyme_ecs_top3')
    ])
    to_ec_df = alias_df.group_by("alias").agg(
        pl.col("ec").unique().drop_nulls(),
        pl.col('enzyme_ecs_top3').unique().drop_nulls()
    ).rename({"ec": "viable_ecs"})

    to_ec_df.write_parquet('data/brenda/brenda_to_ec.parquet')
    return

    # sort by length of ec
    duplicates = to_ec_df.sort(pl.col("viable_ecs").list.len(), descending=True)

    # sort by synonym
    # duplicates = duplicates.sort("synonym")
    

    runeem_df = pl.read_csv('data/humaneval/runeem/runeem_20241125.csv')

    # merge on enzyme_full if it exists, otherwise merge on enzyme
    from enzyextract.thesaurus.convert_ec import add_ecs
    runeem_df = add_ecs(runeem_df, to_ec_df)

    runeem_df_csv = runeem_df.with_columns(
        pl.col("viable_ecs").list.join("; ").alias("viable_ecs")
    )

    runeem_df_csv.write_csv('data/humaneval/runeem/runeem_20241205_ec.csv')
    print(runeem_df)

if __name__ == '__main__':
    # script_detect_combined_mutants()
    # script_generate_enzyme_synonyms()
    # exit(0)
    
    # script_apply_accessions_to_brenda()
    script_enzyme_name_to_ec()
    pass


    