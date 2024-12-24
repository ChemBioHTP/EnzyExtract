# see how well certain keywords are able to cover kinetic papers
import polars as pl

def script_look_for_bad_ocr():
    # Assuming we have parquet files with columns: pmid, page_number, text
    manifest = pl.read_parquet('data/manifest.parquet')

    readable = manifest.filter(
        pl.col('apogee_processed')
    ).select('filename').unique()
    dfs = []
    
    in_df_names = ['scratch', 'brenda', 'wos', 'topoff']  # adjust as needed
    keywords = [
        r'(?i)kcat',
        r'(?i)michaelis',
        r'(?i)turnover',
        r'\bKm\b',
        'e',
    ]
    # 0: precision 45.7% recall 65.3%
    # 0, 1: precision 42.8% recall 82.7%
    # 0, 1, 2: Precision 41.2% Recall 87.0%
    # 0, 1, 2, 3: Precision 38.2% Recall 90.8%
    # (infinity) Precision 32.6% Recall 99.7% (maybe some foreign language papers?)
    
    # expr = pl.col('text').str.contains(keywords[0])
    # expr = pl.col('')
    # pmid, page_number, text
    expr_a = (pl.col('pmid') + '.pdf').is_in(readable['filename'])
    expr_b = pl.col('text').str.contains(keywords[0])
    for keyword in keywords[1:]:
        expr_b |= pl.col('text').str.contains(keyword)
    for name in in_df_names:
        print(f'Scanning {name}...')
        df = pl.scan_parquet(f'data/scans/{name}.parquet')
        # print(df.columns)
        df = df.filter(
            expr_a & expr_b
        ).select('pmid').collect()
        dfs.append(df)
    df = pl.concat(dfs)
    df: pl.DataFrame
    df = df.unique()
    
    
    kinetic = manifest.filter(
        pl.col('apogee_kinetic')
    )
    kinetic_pmids = kinetic['filename'].str.replace('.pdf$', '').unique()

    # print precision, recall of our choice of keywords
    tp = df.filter(pl.col('pmid').is_in(kinetic_pmids)).height
    fp = df.height - tp
    fn = kinetic_pmids.len() - tp
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)
    print(f'Precision: {precision} = {tp} / {tp + fp}')
    print(f'Recall: {recall} = {tp} / {tp + fn}')

if __name__ == "__main__":
    script_look_for_bad_ocr()

