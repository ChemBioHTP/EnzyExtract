import polars as pl
import polars.selectors as cs
from Bio.Data.IUPACData import protein_letters_3to1_extended
from enzyextract.fetch_sequences.read_pdfs_for_idents import mutant_pattern, mutant_v3_pattern, mutant_v4_pattern

def clean_mutants(df):
    df = df.with_columns([
        pl.col("mutant").str.extract_all(mutant_pattern.pattern).alias("mutant1"),
        pl.col("mutant").str.extract_all(mutant_v3_pattern.pattern).alias("mutant3"),
        pl.col("mutant").str.extract_all(mutant_v4_pattern.pattern).alias("unclean_mutant")
    ])

    df = df.with_columns([
        pl.col("mutant3").list.eval(pl.element().str.replace_many(protein_letters_3to1_extended)).alias("mutant3"),
    ]).with_columns([
        pl.col("mutant1").list.concat(pl.col("mutant3")).alias("clean_mutant")
    ]).select(cs.exclude('mutant1', 'mutant3'))
    return df
def preview_mutants():
    """
    Preview how often GPT gives unusable mutants.
    """
    # df = pl.read_parquet('data/_compiled/apogee_all.parquet')

    # keep = [x for x in df.columns if (not x.endswith('_feedback') and not x.endswith('_2'))]
    # df = df.select(keep)

    # df = df.filter(~pl.all_horizontal(pl.exclude("pmid").is_null()))

    # df.write_parquet('data/_compiled/_valid_apogee_all.parquet')
    df = pl.read_parquet('data/_compiled/_valid_apogee_all.parquet')
    df = df.filter(
        pl.col("mutant").is_not_null()
    )
    df = clean_mutants(df)

    # clean mutants: 49880 rows, 5322 pmids
    # dirty mutants: 1635 rows, 188 pmids
    # we can also write a regex to manually convert dirty mutants, so this is not a big deal.
    print(df)

def preview_wide_tables():
    # wide tables could be impacting performance. let's measure how many of these there are

    df = pl.scan_parquet('data/ingest/apogee_ingest.parquet')
    df = df.filter(
        pl.col('content').str.contains('\|:?-*:?(\|:?-*:?){6,40}\|') # matches tables with 7-40 columns
    ).collect()
    df.select(['pmid']).write_parquet('data/pmids/apogee_wide_tables_7plus.parquet')
    print(df)

def preview_hallucinate_micro():
    # detect if GPT hallucinates a micro which is nowhere in the text

    valid = pl.scan_parquet('data/gpt/apogee_gpt.parquet').filter(
        pl.col('content').str.contains('[µμ]')
    ).collect()
    
    hallucinated = pl.scan_parquet('data/ingest/apogee_ingest.parquet').filter(
        ~pl.col('content').str.contains('[µμ]')
        & pl.col('pmid').is_in(valid['pmid'])
        & ~(pl.col('secondlevel') == 'asm')
    ).collect()

    # print(hallucinated) # 701 pmids
    hallucinated.select(['pmid']).write_parquet('data/pmids/apogee_hallucinated_micro.parquet')

def preview_asm_micro_error():
    # common asm error: (\u0001M) represents micromolar
    # let's see how often this error occurs in the asm papers

    # 'm\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0011\u0012\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f'
    # strange = '\([\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0011\u0012\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f]M\)'
    strange = '\(\x7F-\x9FM\)' # nothing!
    readable_dfs = []
    for filename in ['brenda', 'scratch', 'topoff', 'wos']:
        df = pl.scan_parquet(f'data/scans/{filename}.parquet').filter(
            pl.col('text').str.contains(strange)
        ).collect()
        readable_dfs.append(df)
    df = pl.concat(readable_dfs) 
    print(df) # 705 pmids, the majority we expect to be micromolar
    df.select(['pmid']).unique().write_parquet('data/pmids/apogee_asm_micro_error.parquet')

    df = df.select(['pmid'])

def preview_prompt_regurgitate():
    # detect gpt regurgitating the example data from the prompt
    prompt_fragment = """
    - descriptor: R190Q cat-1; 25°C
      substrate: H2O2
      kcat: 33 ± 0.3 s^-1
      Km: 2.3 mM
      kcat/Km: null"""
    df = pl.scan_parquet('data/gpt/apogee_gpt.parquet').filter(
        pl.col('content').str.contains(prompt_fragment, literal=True)
    ).collect()
    print(df) # 32 examples

def preview_super_long():
    # preview pdfs that are excessively long (>=50 pages)
    readable_dfs = []
    for filename in ['brenda', 'scratch', 'topoff', 'wos']:
        df = pl.scan_parquet(f'data/scans/{filename}.parquet').filter(
            pl.col('page_number') >= 50
        ).select(['pmid', 'page_number']).collect()
        readable_dfs.append(df)
    df = pl.concat(readable_dfs) 
    # print(df) # 3400 pmids! that's a ton

    df = df.group_by('pmid').agg(pl.col('page_number').max().alias('max_page_number'))

    # compare that with how many in manifest are long
    manifest = pl.read_parquet('data/manifest.parquet')
    manifest = manifest.filter(
        pl.col('apogee_kinetic')
    )
    manifest = manifest.join(df, left_on='filename', right_on=(pl.col('pmid') + '.pdf'), how='inner')
    # 360 kinetic pmids above 50 pages, that is not insubstantial
    # going up to 297 pages!!
    # no brenda though!
    print(manifest)

if __name__ == "__main__":
    preview_mutants()
    # preview_wide_tables()
    # preview_hallucinate_micro()
    # preview_asm_micro_error()
    # preview_prompt_regurgitate()
    # preview_super_long()

    # df = pl.read_parquet('data/pmids/apogee_asm_micro_error.parquet')
    # manifest = pl.read_parquet('data/manifest.parquet')
    # manifest = manifest.join(df, left_on=pl.col('filename').str.replace('.pdf$', ''), right_on='pmid', how='inner')
    # print(manifest)