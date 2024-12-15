import re
import os
import polars as pl
import pymupdf
from tqdm import tqdm

abbr_re = re.compile(r'\babbreviations?\b', re.IGNORECASE)

# abbr_harder_re = re.compile(r'\babbreviations', re.IGNORECASE)
structured_abbr_re = re.compile(r'\babbreviations?\s*used(\s*are|\s*is)?:\s*', re.IGNORECASE)
structured_abbr2_re = re.compile(r'\babbreviations[:.]\s+', re.IGNORECASE)
structured_end_re = re.compile(r'\.\s*\n')


structured_comma_re = re.compile(r',\s+')
structured_eq_re = re.compile(r'=\s+')
remove_hyphenation_re = re.compile(r'(\w)-\s*\n\s*(\w)')

def auto_extract_abbr(content: str):
    # 355/424 is auto-extractable
    if not content:
        return []
    # try the structured re
    match = structured_abbr_re.search(content)
    if not match:
        match = structured_abbr2_re.search(content)
    if not match:
        return []
    
    start = match.end()
    content = content[start:]

    end = structured_end_re.search(content)
    if end:
        content = content[:end.start()]
    
    if ';' not in content:
        return [] # give up
    # split by semicolon
    semiparts = content.split(';')
    abbrs = []

    delim_thres = .8

    need_rstrip = False
    delim_zoo = ['=', '–', '—', ':', '(']
    for delim in delim_zoo:
        if (sum([1 if p.count(delim) == 1 else 0 for p in semiparts]) 
                >= len(semiparts) * delim_thres):
            lax_delimiter_re = delim
            if delim == '(':
                delimiter_re = re.compile(f'\(\s+')
                need_rstrip = True
            else:
                delimiter_re = re.compile(f'{delim}\s+')
            break
    else:
        delimiter_re = structured_comma_re
        lax_delimiter_re = ','
    
    for semipart in semiparts:
        # split by comma
        # parts = semipart.split(', ', 1)

        # remove the hyphenation
        semipart = remove_hyphenation_re.sub(r'\1\2', semipart)

        parts = delimiter_re.split(semipart, maxsplit=1)
        if len(parts) <= 1:
            parts = semipart.split(lax_delimiter_re, 1)
        
        if len(parts) <= 1: # strange
            abbrs.append((semipart, None))
        else:
            rhs = parts[1].strip()
            if need_rstrip:
                rhs = rhs.rstrip(')')
            abbrs.append((parts[0].strip(), rhs))
                
    return abbrs

def script_look_for_abbreviations(pdfs_folder="C:/conjunct/tmp/eval/arctic", recursive=False):
    # look for EC numbers in the PDFs

    
    ec_matches = []
    abbr_matches = []
    # for filename in tqdm(os.listdir(pdfs_folder)):
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
    for dirpath, filename, pmid in tqdm(pdfs):
        found_something = False
        try:
            pdf = pymupdf.open(os.path.join(dirpath, filename))
        except:
            continue
        # only look at the first page
        # page = pdf[0]
        # text = page.get_text()

        # okay, let's look at subsequent pages
        for page in pdf[0:]:
            text = page.get_text()
            matches = abbr_re.findall(text)
            if matches:
                # get the content from the match to the end of the page
                start = text.find(matches[0])
                content = text[start:]
                ec_matches.append((pmid, content))
                found_something = True

                # try to find structured abbreviations
                abbrs = auto_extract_abbr(content)
                abbr_matches.extend([(pmid, a, b) for a, b in abbrs])
                # break
        if not found_something:
            ec_matches.append((pmid, None))
            
        pdf.close()

    df = pl.DataFrame(ec_matches, schema=['pmid', 'content'], orient='row')
    df2 = pl.DataFrame(abbr_matches, schema=['pmid', 'a_name', 'b_name'], orient='row')


    interesting_df = df.filter(
        (pl.col('content').is_not_null())
        & ~(pl.col('pmid').is_in(set(df2['pmid'])))
    )
    # df.to_csv('C:/conjunct/tmp/eval/arctic_dev/ec_matches.csv', index=False)
    df.write_parquet('data/synonyms/abbr/beluga_paper_abbrs.parquet')
    df2.write_parquet('data/synonyms/abbr/beluga_abbrs.parquet')

    # wow, 366/(366+58) = 86% of the papers can be automatically processed!
    return df

def script_look_for_abbreviations_polars():
    # Assuming we have parquet files with columns: pmid, page_number, text
    dfs = []
    
    in_df_names = ['scratch', 'brenda', 'wos', 'topoff']  # adjust as needed
    
    for name in in_df_names:
        print(f"Looking for abbreviations in {name}")
        df = pl.scan_parquet(f'data/scans/{name}.parquet')
        
        # First pass: find matches using regex
        df_content = (
            df.with_columns([
                # Replace this with your abbr_re pattern
                pl.col('text').str.extract(r'(?i)\b(abbreviations?(?:.|\n)*)$').alias('content')
            ])
            .select('pmid', 'page_number', 'content') # , 'text')
            .filter(pl.col('content').is_not_null())
            # .explode('content')
        ).collect()

        # Extract structured abbreviations
        # You'll need to modify this part based on your auto_extract_abbr function
        
        # Collect and save results

        df_abbrs = process_abbreviations(df_content)
        # df_abbrs = df_abbrs.collect()
        
        # Save results
        df_content.write_parquet(f'data/synonyms/abbr/{name}_paper_abbrs.parquet')
        df_abbrs.write_parquet(f'data/synonyms/abbr/{name}_abbrs.parquet')
        
        # Find interesting cases (papers with content but no structured abbreviations)
        interesting_df = (
            df_content
            .filter(
                (pl.col('content').is_not_null()) 
                & ~(pl.col('pmid').is_in(df_abbrs.select('pmid')))
            )
        )
        
        dfs.append(interesting_df)
    
    # Combine all results if needed
    final_df = pl.concat(dfs)
    return final_df

def auto_extract_abbr_wrapper(content: str) -> list[tuple[str, str]]:
    """Wrapper to handle None values and convert tuples to dicts"""
    if not content:
        return []
    results = auto_extract_abbr(content)
    # Convert tuples to structs that Polars can understand
    return [{'a_name': a, 'b_name': b} for a, b in results]

def process_abbreviations(df):
    """
    Process a Polars DataFrame to extract abbreviations using the original auto_extract_abbr function.
    Expects a DataFrame with a 'content' column containing the text to process.
    """
    return (
        df
        .filter(pl.col('content').is_not_null())
        .with_columns([
            pl.col('content')
            .map_elements(auto_extract_abbr_wrapper)
            .alias('abbr_pairs')
        ])
        # Filter out rows where we couldn't extract abbreviations
        .filter(pl.col('abbr_pairs').list.len() > 0)
        # Explode the list of abbreviation pairs
        .explode('abbr_pairs')
        .unnest('abbr_pairs')
    )

if __name__ == '__main__':
    # df = script_look_for_abbreviations()
    script_look_for_abbreviations_polars()
    # print(df)