import polars as pl

def main() -> pl.DataFrame:
    so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}

    df = pl.read_csv('data/_compiled/apogee-nonbrenda.tsv', separator='\t', schema_overrides=so)

    # select only rows where one of these is true
    # 1. kcat has at least 1 numeric value
    # 2. km has at least 1 numeric value
    df = df.filter(
        (pl.col('kcat').str.contains(r'\d+').cast(pl.Boolean)) | 
        (pl.col('km').str.contains(r'\d+').cast(pl.Boolean))
    )
    pmids = df['pmid'].unique()
    print(len(pmids))
    return df

if __name__ == '__main__':
    df = main() # 12421 pmids remain (57% of the papers)
    df = df[['pmid']].unique(['pmid'])

    # these are filenames
    df.write_parquet('data/pmids/apogee_nonbrenda_numerical.fn.parquet')


    