"""
Read PDFs into text, for compact storage
"""

import os
from tqdm import tqdm
import pymupdf
import polars as pl

from enzyextract.dependency.injection import REQUIRE, resolve
from enzyextract.dependency.prereqs import export
from enzyextract.utils.xml_pipeline import xml_abstract_processing, xml_get_soup, xml_raw_text_processing

def scan_papers(pdfs_folder, recursive=False):
    # round up all the PDFs
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
    

    # begin reading PDFs
    content = []
    for dirpath, filename, pmid in tqdm(pdfs):
        found_something = False
        try:
            pdf = pymupdf.open(os.path.join(dirpath, filename))
        except:
            continue
        for i, page in enumerate(pdf):
            text = page.get_text()
            content.append((pmid, i, text))
            
        pdf.close()

    # form df
    df = pl.DataFrame(content, schema=['pmid', 'page_number', 'text'], orient='row', schema_overrides={
        'pmid': pl.Utf8,
        'page_number': pl.UInt32,
        'text': pl.Utf8
    })
    return df


@export("data/scans/{alias}.parquet")
def script_scan_papers(pdf_folder, alias, *, recursive=True):
    df = scan_papers(pdf_folder, recursive=recursive)
    os.makedirs('data/scans', exist_ok=True)
    df.write_parquet(f'data/scans/{alias}.parquet')

@export('data/scans/xml.parquet')
def scan_xmls_by_folder(xml_folder, recursive=False):
    # round up all the XMLs
    xmls = []
    if not recursive:
        for filename in os.listdir(xml_folder):
            if filename.endswith('.xml'):
                pmid = filename[:-4]
                xmls.append((xml_folder, filename, pmid))
    else:
        for dirpath, dirnames, filenames in os.walk(xml_folder):
            for filename in filenames:
                if filename.endswith('.xml'):
                    pmid = filename[:-4]
                    xmls.append((dirpath, filename, pmid))

    # begin reading XMLs
    xml_contents = []
    for dirpath, filename, pmid in tqdm(xmls):
        filepath = f"{dirpath}/{filename}"

        with open(filepath, "r", encoding='utf-8') as f:
            soup = xml_get_soup(f.read())
        
        if not soup:
            xml_contents.append((dirpath, filename, pmid, None, None, None))
            continue

        # assume that the abstract fits into one chunk ;-;
        abstract = xml_abstract_processing(soup)
        # abstracts.append(abstract)
        
        # extract raw texts
        raw_txt = xml_raw_text_processing(soup)
        # raw_txts.append(raw_txt)

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
        # all_tables.append(docs)
        xml_contents.append((dirpath, filename, pmid, raw_txt, abstract, docs))
    
    df = pl.DataFrame(xml_contents, schema={
        'fileroot': pl.Utf8,
        'filename': pl.Utf8,
        'pmid': pl.Utf8,
        'content': pl.Utf8,
        'abstract': pl.Utf8,
        'tables': pl.List(pl.Utf8)
    }, orient='row')
    return df

@resolve
def scan_xmls_by_manifest(
    manifest = REQUIRE('data/xml_manifest.parquet')
):
    
    manifest = manifest.select(['fileroot', 'filename', 'pmid'])

    abstracts = []
    raw_txts = []
    all_tables = []
    for fileroot, filename, _ in tqdm(manifest.iter_rows(), total=manifest.height):
        filepath = f"{fileroot}/{filename}"

        with open(filepath, "r", encoding='utf-8') as f:
            soup = xml_get_soup(f.read())

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
        pl.Series("text", raw_txts),
        pl.Series("abstract", abstracts),
        pl.Series("tables", all_tables)
    ])
    return manifest

@resolve
@export("data/scans/xml_slim.parquet")
def script_slim_xml(
    df = REQUIRE('data/scans/xml.parquet', eager=False),
    thedata = REQUIRE('data/export/TheData.parquet', eager=False)
):
    good_pmids = set(thedata.select('pmid').collect().to_series())
    df = df.filter(pl.col('pmid').is_in(good_pmids))
    df.sink_parquet('data/scans/xml_slim.parquet')

if __name__ == "__main__":
    script_scan_papers("D:/papers/brenda/wiley", "brenda_wiley")

    scan_xmls_by_manifest()
    # script_scan_xml()
    pass