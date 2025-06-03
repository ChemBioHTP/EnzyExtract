import zipfile
import os
import polars as pl
from enzyextract.dependency.injection import REQUIRE, resolve
from enzyextract.dependency.prereqs import export, require
import io



@require("data/thesaurus/organism/taxdmp.zip", instructions="Download the NCBI taxonomy data from https://ftp.ncbi.nih.gov/pub/taxonomy/")
@export("data/thesaurus/organism/ncbi_taxonomy.parquet")
def load_ncbi_taxonomy(
    zip_path = "data/thesaurus/organism/taxdmp.zip"
):
    """
    Load the NCBI taxonomy from the taxdmp.zip file.

    To obtain the file, download it from https://ftp.ncbi.nih.gov/pub/taxonomy/.
    """
    
    with zipfile.ZipFile(zip_path, 'r') as zip_file:
        # List all files in the zip
        # file_list = zip_file.namelist()
        # Access a specific file (example: names.dmp)
        # with zip_file.open('names.dmp') as file:
            # content = file.read().decode('utf-8')
        
        # Or extract to memory
        names_data = zip_file.read('names.dmp').decode('utf-8')

    df = pl.read_csv(
        io.BytesIO(names_data.encode('utf-8')),
        separator='\t',
        quote_char=None,
        has_header=False,
        schema={
            'tax_id': pl.Int64,
            '|1': pl.Utf8,
            'name_txt': pl.Utf8,
            '|2': pl.Utf8,
            'unique_name': pl.Utf8,
            '|3': pl.Utf8,
            'name_class': pl.Utf8,
            '|4': pl.Utf8,
        }
    )
    # drop the "|" headers
    df = df.select(
        pl.selectors.exclude(pl.selectors.contains("|"))
    )
    df.write_parquet("data/thesaurus/organism/ncbi_taxonomy.parquet")

@resolve
@export("data/thesaurus/organism/ncbi_binomial_taxonomy.parquet")
def ncbi_binomial_taxonomy(
    df = REQUIRE("data/thesaurus/organism/ncbi_taxonomy.parquet", eager=True)
):
    """
    Load the NCBI taxonomy and filter for scientific names (binomial nomenclature).
    """
    # Filter for binomial nomenclature exactly (no species, subspecies, etc.)
    scientific_df = df.filter(
        pl.col('name_txt')
        .str.contains(r'^[A-Z][a-z]+ [a-z]+$')  
    ).select('tax_id', 'name_txt', 'name_class')

    scientific_df = scientific_df.filter(pl.col("name_class").is_in({"scientific name", "synonym"}))

    # .str.replace(r' sp\. .*$', '')  # Remove species suffix
    # .str.replace(r' subsp\. .*$', '')  # Remove subspecies suffix
    # .str.replace(r' var\. .*$', '')  # Remove variety suffix

    scientific_df = scientific_df.with_columns(
        pl.col('name_txt').str.split(' ').alias('binomial_name_parts')
    ).with_columns(
        pl.col('binomial_name_parts').list.first().alias('genus'),
        pl.col('binomial_name_parts').list.last().alias('species'),
    ).with_columns(
        (pl.col('genus').str.slice(0, 1) + '. ' + pl.col('species')).alias('short_genus')
    ).drop('binomial_name_parts', 'genus', 'species')
    scientific_df.write_parquet("data/thesaurus/organism/ncbi_binomial_taxonomy.parquet")

if __name__ == "__main__":
    # load_ncbi_taxonomy()
    ncbi_binomial_taxonomy()