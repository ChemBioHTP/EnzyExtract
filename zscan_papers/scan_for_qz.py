import polars as pl

def script_look_for_bad_ocr():
    # Assuming we have parquet files with columns: pmid, page_number, text
    dfs = []
    
    in_df_names = ['scratch', 'brenda', 'wos', 'topoff']  # adjust as needed
    
    for name in in_df_names:
        print(f"Looking for abbreviations in {name}")
        df = pl.scan_parquet(f'data/scans/{name}.parquet')
        
        # bad ocr: get count of unicode control codes such as \x02, \x03, \x04, etc.
        df_content = (
            # join by pmid
            df.group_by('pmid').agg('text').with_columns([
                pl.col('text').list.join('\n').alias('text')
            ]).with_columns([
                pl.col('text').str.contains('(?i)saturation\s*mutagenesis').alias('has_saturation_mutagenesis'),
                pl.col('text').str.contains('(?i)site[-–—\s]*saturation').alias('has_site_saturation'),
                pl.col('text').str.contains('(?i)\bNNK\b').alias('has_NNK'),
            ]).filter(
                pl.col('has_saturation_mutagenesis') 
                # | pl.col('has_site_directed_mutagenesis') 
                | pl.col('has_site_saturation')
                | pl.col('has_NNK')
            )
        ).collect()

        # Save results
        dfs.append(df_content)
        # df_content.write_parquet(f'data/synonyms/abbr/{name}_bad_ocr.parquet')
    
    # Combine all results if needed
    final_df = pl.concat(dfs)
    return final_df


# wait a sec, what if 

if __name__ == '__main__':
    # script_horrible_ocr()
    final_df = script_look_for_bad_ocr()
    print(final_df)
    final_df.write_parquet('data/scans/misc/site_saturation.parquet')