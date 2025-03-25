import json
import os
import re
import importlib
from tqdm import tqdm

import gmft
import gmft.table_detection
import gmft.table_visualization
import gmft.table_function
import gmft.table_function_algorithm
import gmft.pdf_bindings
import gmft.common

from gmft_pymupdf import PyMuPDFDocument
from gmft.table_detection import CroppedTable, TableDetector, TableDetectorConfig
from gmft.pdf_bindings import PyPDFium2Document
from gmft.auto import AutoFormatConfig, AutoTableFormatter
from enzyextract.pre.table.reocr_for_gmft import load_correction_df, PyMuPDFDocument_REOCR
from gmft.table_function import TATRFormattedTable
from pypdfium2 import PdfiumError

# Reload modules
for module in [gmft, gmft.common, gmft.table_detection, gmft.table_visualization,
               gmft.table_function_algorithm, gmft.table_function, gmft.pdf_bindings]:
    importlib.reload(module)

# Regular expressions for kinetic term detection
kinetics_re_s = kcat_re_s = [re.compile(x, re.IGNORECASE) for x in \
    [r'\bk[\s_\-\'−–]*cat', r'\bk[\s_-]*m\b',
    r'turnover\s*(rate|frequency|number|value)', 
    r'catalytic\s*(efficiency|eﬀiciency|rate|constant|number)',  # |efﬁciency
    r'\bk\s?,,+', 
    r'\bk ?0(?!\.)\b', # '\bk\s?0\b', but k0.5 and k\n0\n ends up being roped in
    r'enzyme\s*turnover',
    r'[mμ]M[\^\s]*[-−–]1[\^\s]*(s|min)',
    # r'kinetic.parameter',
    r'kinetic', # v1.0
    r'\bmm\b', # v1.0
    r'Michaelis',
    # r'steady.state.rate.constant',
    r'steady.state' # v1.1
]]

def setup_directories(save_dir):
    """
    Structure:
    - save_dir/
        - false_positives/ (for false positives)
            - <pdfname>_<idx>.jpg
            - <pdfname>_<idx>.info
        - markdown/
        - info/
        - seen.txt

    """
    if not os.path.exists(save_dir):
        print(f"Making directory {save_dir}")
        os.makedirs(save_dir)
    os.makedirs(f"{save_dir}/false_positives", exist_ok=True)
    os.makedirs(f"{save_dir}/markdown", exist_ok=True)
    os.makedirs(f"{save_dir}/info", exist_ok=True)

def ingest_pdf(pdf_path, correction_df, detector):
    try:
        doc = PyMuPDFDocument_REOCR(pdf_path, correction_df=correction_df)
    except Exception:
        return [], None
    
    tables = [table for page in doc for table in detector.extract(page)]
    return tables, doc

def create_md(table: TATRFormattedTable, config):
    tbl_content = table.df(config_overrides=config).to_markdown(index=False)
    captions = table.captions()
    return f"""{captions[0]}\n\n{tbl_content}\n\n{captions[1]}"""

def process_pdfs(pdf_root, write_dir, micros_path):
    setup_directories(write_dir)
    
    all_pdfs = sorted([f for f in os.listdir(pdf_root) if f.endswith(".pdf")])
    correction_df = load_correction_df(micros_path, all_pdfs)
    
    detector = TableDetector()
    formatter = AutoTableFormatter(config=AutoFormatConfig())
    
    md_path = f"{write_dir}/markdown"
    info_path = f"{write_dir}/info"
    fp_path = f"{write_dir}/false_positives"
    seen_path = f"{write_dir}/seen.txt"

    if os.path.exists(seen_path):
        with open(seen_path, "r") as f:
            seen = set(f.read().splitlines())
        all_pdfs = [f for f in all_pdfs if f not in seen]

    assert len(all_pdfs) > 0, "No PDFs to process. Check the directory."
    for filename in tqdm(all_pdfs):
        
        pdfname = filename[:-4]
        aok = False
        try:
            tables, doc = ingest_pdf(f"{pdf_root}/{filename}", correction_df, detector)
            for i, table in enumerate(tables):
                text = table.text() + '\n'.join(table.captions())
                if not any(re.search(x, text) for x in kinetics_re_s):
                    continue
                
                rotated = table.label == 1
                out_name = f"{pdfname}_{i}{'.rotated' if rotated else ''}"
                
                try:
                    ft = formatter.extract(table)
                    with open(f"{md_path}/{out_name}.md", "w", encoding='utf-8') as f:
                        f.write(create_md(ft, formatter.config))
                    with open(f"{info_path}/{out_name}.info", "w") as f:
                        json.dump(ft.to_dict(), f)
                except ValueError:
                    table.image(dpi=36).save(f"{fp_path}/{out_name}.jpg")
                    with open(f"{fp_path}/{out_name}.info", "w") as f:
                        json.dump(table.to_dict(), f)
            aok = True

        except PdfiumError:
            print(f"Error processing {filename}")
            aok = True # pdfium is not our fault
        except Exception as e:
            raise e
        finally:
            if doc is not None:
                doc.close()
        if aok:
            with open(seen_path, "a") as f:
                f.write(f"{filename}\n")
        


