import polars as pl
from tqdm import tqdm
from enzyextract.utils.xml_pipeline import xml_get_soup, xml_abstract_processing, xml_raw_text_processing, xml_table_processing

manifest = pl.read_parquet('data/xml_manifest.parquet')

manifest = manifest.select(['fileroot', 'filename', 'pmid'])

abstracts = []
raw_txts = []
all_tables = []
ctr = 0
for fileroot, filename, _ in tqdm(manifest.iter_rows(), total=manifest.height):
    ctr += 1
    # if ctr > 100:
        # break
    filepath = f"{fileroot}/{filename}"

    with open(filepath, "r", encoding='utf-8') as f:
        soup = xml_get_soup(f.read())

    # see EnzyXMLDocumentLoader.lazy_load
    # TODO search for xocs:ucs-locator
    if not soup:
        raw_txts.append(None)
        abstracts.append(None)
        all_tables.append(None)
        continue

    # assume that the abstract fits into one chunk ;-;
    abstract = xml_abstract_processing(soup)
    abstracts.append(abstract)
    
    # extract raw texts
    raw_txt = xml_raw_text_processing(soup)
    raw_txts.append(raw_txt)

    # extract tables
    tables = soup.find_all('ce:table')
    # give each table directly, as raw xml
    docs = []
    for table in tables:
        raw_table = str(table)
        raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/dtd"', '')
        raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/cals/dtd"', '')
        if raw_table.strip():
            docs.append(raw_table)
    all_tables.append(docs)


manifest = manifest.with_columns([
    pl.Series("content", raw_txts),
    pl.Series("abstract", abstracts),
    pl.Series("tables", all_tables)
])
manifest.write_parquet('data/scans/xml.parquet')

df = pl.scan_csv("data/smiles/CID-SMILES", separator='\t', has_header=False).rename(["CID", "SMILES"])
