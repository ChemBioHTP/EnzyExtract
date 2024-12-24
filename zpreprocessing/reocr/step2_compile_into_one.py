import polars as pl
import os
dfs = []

root = r'C:\conjunct\tmp\eval\cherry_prod\mM'
for folder in os.listdir(root):
    filename = f'{root}/{folder}/mMcurated.parquet'
    if not os.path.exists(filename):
        continue
    df = pl.read_parquet(filename)

    toplevel, secondlevel = folder.split('_', 1)
    df = df.with_columns(
        pl.lit(toplevel).alias('toplevel'),
        pl.lit(secondlevel).alias('secondlevel')
    )
    dfs.append(df)

df = pl.concat(dfs)

dfformer = pl.read_parquet(r'zpreprocessing/data/pdf_mM_former.parquet')
dfformer = dfformer.with_columns([
    # pl.lit('former').alias('origin')
]).rename({'lettery0': 'letter_y0'})
df2 = pl.concat([df, dfformer], how='diagonal')

df2.write_parquet(r'zpreprocessing/data/pdf_mM.parquet')
# pass

# wow! "\u0001M" is micromolar a whopping 46067/47497 (97%) of the time!
# would be easier to flag the 3% that are not micromolar (by recording those pmids for instance)