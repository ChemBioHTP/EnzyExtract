import polars as pl

context_df = pl.read_parquet("data/recontext/1_fromyaml/context.parquet")
enzyme_synonyms = context_df.select("pmid", "enzymes").explode("enzymes").unnest("enzymes").explode("synonyms")
print(enzyme_synonyms)

substrate_synonyms = context_df.select("pmid", "substrates").explode("substrates").unnest("substrates").explode("synonyms")
print(substrate_synonyms)
enzyme_synonyms.write_parquet("data/recontext/1_fromyaml/enzyextract_enzyme_synonyms.parquet")
substrate_synonyms.write_parquet("data/recontext/1_fromyaml/enzyextract_substrate_synonyms.parquet")