# micros_path = f"C:/conjunct/vandy/yang/reocr/cache/iter3/mM_{toplevel}_{secondlevel}.csv"
import os
import polars as pl
collector = []
root = r'C:/conjunct/vandy/yang/reocr/cache/iter3'
for filename in os.listdir(root):
    if not filename.endswith('.csv'):
        continue
    _, toplevel, secondlevel = filename[:-4].split('_', 2)
    df = pl.read_csv(f"{root}/{filename}", schema_overrides={'pdfname': pl.Utf8})
    df = df.with_columns([
        pl.lit(toplevel).alias("toplevel"),
        pl.lit(secondlevel).alias("secondlevel"),
    ])
    collector.append(df)
df = pl.concat(collector)
print(df)
df.write_parquet('zpreprocessing/data/pdf_mM.parquet')
