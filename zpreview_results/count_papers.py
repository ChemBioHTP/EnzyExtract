# count the size of each
import polars as pl

pdf_manifest = pl.read_parquet('data/manifest.parquet')
xml_manifest = pl.read_parquet('data/xml_manifest.parquet')

all_pdf = pdf_manifest.select('canonical').unique() # 59409
all_xml = xml_manifest.select('canonical').unique() # 82468

# get union of both
all_canonical = pl.concat([all_pdf, all_xml]).unique()
print(all_canonical.height) # 137892

pass