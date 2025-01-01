import polars as pl

from enzyextract.thesaurus.mutant_patterns import mutant_pattern, mutant_v3_pattern, mutant_v5_pattern, standardize_mutants1_re, amino3to1, mutant_to_wt

mutant_re = '      mutants: (.*)\n'
df = pl.read_parquet('data/gpt/latest_gpt.parquet')
df = df.with_columns([
    pl.col('content').str.extract(mutant_re).replace('null', None).alias('mutants')
]).filter(
    pl.col('mutants').is_not_null()
).select('zerolevel', 'pmid', 'mutants')
df = df.with_columns([
    pl.col('mutants').str.extract_all(mutant_pattern.pattern).list.unique().alias('mutant1'),
    pl.col('mutants').str.extract_all(mutant_v3_pattern.pattern).list.unique().alias('mutant2'),
    pl.col('mutants').str.extract_all(mutant_v5_pattern.pattern).list.unique().alias('mutant3')
]).with_columns([
    pl.col('mutant1').list.concat(
        pl.col('mutant2').list.eval(
            pl.element().str.replace_many(amino3to1)
        )
    ).list.concat(
        pl.col('mutant3').list.eval(
            pl.element().str.replace(standardize_mutants1_re.pattern, '$1$2$4').str.replace_many(amino3to1)
        )
    ).alias('clean_mutant')
]).with_columns([
    pl.col('clean_mutant').list.eval(
        pl.element().str.extract(mutant_to_wt.pattern)
    ).list.unique().alias('wildtype')
])

print(df)