import os
from enzyextract.fetch_sequences.read_pdfs_for_idents import amino1, amino3, mutant_pattern, mutant_v3_pattern
import pymupdf
from tqdm import tqdm
import polars as pl

def search_mutants(all_txt: str):
    # search text for mutant codes

    # mutant v2 is depr
    mutant_matches = mutant_pattern.findall(all_txt) + mutant_v3_pattern.findall(all_txt)
    return list(set(mutant_matches))




def look_for_mutants(pdfs_folder, recursive=False, pmid_whitelist=None):
    # look for EC numbers in the PDFs
    

    # round up all the PDFs
    pdfs = []
    if not recursive:
        for filename in os.listdir(pdfs_folder):
            if filename.endswith('.pdf'):
                pmid = filename[:-4]
                pdfs.append((pdfs_folder, filename, pmid))
    else:
        for dirpath, dirnames, filenames in os.walk(pdfs_folder):
            for filename in filenames:
                if filename.endswith('.pdf'):
                    pmid = filename[:-4]
                    pdfs.append((dirpath, filename, pmid))
    
    good = []
    for item in tqdm(pdfs):
        _, _, pmid = item
        if pmid_whitelist is None:
            good.append(item)
        elif pmid in pmid_whitelist:
            good.append(item)
            # continue
    pdfs = good

    # begin reading PDFs
    mu_matches = []
    for dirpath, filename, pmid in tqdm(pdfs):
        found_something = False
        try:
            pdf = pymupdf.open(os.path.join(dirpath, filename))
        except:
            continue
        text = ""
        for page in pdf:
            text += page.get_text()

        matches = search_mutants(text)
        for match in matches:
            mu_matches.append((pmid, match))
            found_something = True
        if not found_something:
            mu_matches.append((pmid, None))
            
        pdf.close()

    # form df
    df = pl.DataFrame(mu_matches, schema=['pmid', 'mutant'], orient='row', schema_overrides={
        'pmid': pl.Utf8,
        'mutant': pl.Utf8
    })
    return df

def script_look_for_all_ecs():
    whitelist = set(pl.read_parquet('data/pmids/apogee_nonbrenda_numerical.fn.parquet')['pmid'])
    # dfs = []
    # df = look_for_mutants("D:/papers/brenda", recursive=True)
    # df.write_parquet('data/enzymes/ec/brenda_ec_matches.parquet')
    df = look_for_mutants("D:/papers/scratch", recursive=True, pmid_whitelist=whitelist)
    print(df)
    return df
    # df.write_parquet('data/enzymes/ec/scratch_ec_matches.parquet')
    # df = script_look_for_ecs("D:/papers/topoff", recursive=True)
    # df.write_parquet('data/enzymes/ec/topoff_ec_matches.parquet')
    # df = script_look_for_ecs("D:/papers/wos", recursive=True)
    # df.write_parquet('data/enzymes/ec/wos_ec_matches.parquet')

def script_look_for_mutants_plified():
    dfs = []

    in_df_names = ['topoff', 'scratch', 'brenda', 'wos']

    for name in in_df_names:
        print("Looking for mutants in", name)
        df = pl.scan_parquet(f'data/scans/{name}.parquet')

        df = (
            df.with_columns([
                pl.col('text').str.extract_all(mutant_pattern.pattern).list.unique().alias('mutant1'),
                pl.col('text').str.extract_all(mutant_v3_pattern.pattern).list.unique().alias('mutant2')
            ])
            .select('pmid', 'page_number', 'mutant1', 'mutant2')
            .with_columns([
                pl.when(pl.col('mutant1').list.len() == 0)
                    .then(None).otherwise(pl.col('mutant1')).alias('mutant1'),
                pl.when(pl.col('mutant2').list.len() == 0)
                    .then(None).otherwise(pl.col('mutant2')).alias('mutant2')
            ])
            # .filter((pl.col('mutant1').list.len() > 0)
                    # | (pl.col('mutant2').list.len() > 0))
            .filter(pl.col('mutant1').is_not_null() | pl.col('mutant2').is_not_null())
        )
        
        df = df.collect() # .unique(maintain_order=True)
        df.write_parquet(f'data/enzymes/mutants/{name}_mutant_matches.parquet')
        del df
        # dfs.append(df)
        # df.write_parquet(f'data/enzymes/mutants/{name}_mutant_matches.parquet')
    # big_df = pl.concat(dfs)
    # big_df.write_parquet('data/enzymes/mutants/apogee_mutants.parquet')

def script_look_for_full_sequences():
    dfs = []

    in_df_names = ['topoff', 'scratch', 'brenda', 'wos']

    for name in in_df_names:
        print("Looking for mutants in", name)
        df = pl.scan_parquet(f'data/scans/{name}.parquet')

        df = (
            df.with_columns([
                pl.col('text').str.extract_all(f'[{amino1}\n]{{50,}}').list.unique().alias('sequence')
            ])
            .select('pmid', 'page_number', 'sequence')
            .with_columns([
                pl.when(pl.col('sequence').list.len() == 0)
                    .then(None).otherwise(pl.col('sequence')).alias('sequence')
            ])
            # .filter((pl.col('mutant1').list.len() > 0)
                    # | (pl.col('mutant2').list.len() > 0))
            .filter(pl.col('sequence').is_not_null())
            .explode('sequence')

        ).with_columns([
            (pl.col('sequence').str.replace_all(r'[\nACGTU]', '').str.len_chars() == 0).alias('DNA')
        ])
        
        df = df.collect() # .unique(maintain_order=True)
        df.write_parquet(f'data/enzymes/mutants/{name}_mutant_matches.parquet')
        del df

if __name__ == "__main__":
    # script_look_for_all_ecs()
    # script_look_for_mutants_plified()
    script_look_for_full_sequences()