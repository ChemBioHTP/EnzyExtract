import polars as pl

def script_look_for_bad_ocr():
    # Assuming we have parquet files with columns: pmid, page_number, text
    dfs = []
    
    in_df_names = ['scratch', 'brenda', 'wos', 'topoff']  # adjust as needed
    
    for name in in_df_names:
        print(f"Looking for bad ocr in {name}")
        df = pl.scan_parquet(f'data/scans/{name}.parquet')
        
        # bad ocr: get count of unicode control codes such as \x02, \x03, \x04, etc.
        df_content = (
            # join by pmid
            df.group_by('pmid').agg('text').with_columns([
                pl.col('text').list.join('\n').alias('text')
            ]).with_columns([
                pl.col('text').str.len_chars().alias('text_length'),
                # Count control characters (Unicode ranges 0x00-0x1F and 0x7F-0x9F)
                pl.col('text').str.count_matches(r'[\x00-\x1F\x7F-\x9F]').alias('control_ct'),
                pl.col('text').str.replace_all(r'[^ -~]', '').str.len_chars().alias('ascii_ct'),
                pl.lit(name).alias('toplevel')
            ]).with_columns([
                # Calculate percentage of control characters
                (pl.col('control_ct') / pl.col('text_length')).alias('control_pct'),
                (pl.col('ascii_ct') / pl.col('text_length')).alias('ascii_pct'),
            ]).filter(
                # pl.col('control_char_percentage') > 0.1
                (pl.col('control_pct') > 0.4)
                # & (pl.col('ascii_pct') < 0.5) # exclude those where ascii is between 40% and 50%
            )
        ).collect()

        # Save results
        dfs.append(df_content)
        # df_content.write_parquet(f'data/thesaurus/abbr/{name}_bad_ocr.parquet')
    
    # Combine all results if needed
    final_df = pl.concat(dfs)
    return final_df

# there is one bad paper in particular
def script_horrible_ocr():
    import pymupdf
    doc = pymupdf.open('C:/conjunct/tmp/eval/arctic/11532948.pdf')
    text = ''
    for page in doc:
        text += page.get_text()
    
    # get distribution of characters
    char_counts = {}
    for char in text:
        char_counts[char] = char_counts.get(char, 0) + 1
    print(char_counts)

    # {'\x02': 190, '\x03': 3629, '\x04': 885, '\x05': 2791, '\x06': 1901, ' ': 6259, '\x07': 3112, '\x08': 1205, '\t': 2364, '\n': 3137, '\x0b': 2424, '\x0c': 728, '\r': 181, '\x0e': 1357, '\x0f': 1499, '\x10': 808, '\x11': 267, '\x12': 2578, '\x13': 599, '\x14': 616, '\x15': 1035, '\x16': 266, '\x17': 1319, '\x18': 281, '\x19': 280, '\x1a': 127, '\x1b': 156, '\x1c': 645, '\x1d': 280, '\x1e': 372, '\x1f': 204,
    return char_counts

# wait a sec, what if 

if __name__ == '__main__':
    # script_horrible_ocr()
    # final_df = script_look_for_bad_ocr()
    # final_df.write_parquet('data/scans/ocr/bad_ocr.parquet')

    # browse manifest
    manifest = pl.read_parquet('data/manifest.parquet')
    bad_ocr = pl.read_parquet('data/scans/ocr/bad_ocr.parquet').with_columns([
        (pl.col('pmid') + '.pdf').alias('filename')
    ])
    manifest = manifest.join(bad_ocr, on=['filename', 'toplevel'], how='left')
    print(manifest)

    # look at indistinct
    indistinct = manifest.filter(manifest.select('filename').is_duplicated())
    pass