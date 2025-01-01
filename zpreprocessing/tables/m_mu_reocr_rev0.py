# feat: use the micromolar correction

import os
from gmft_pymupdf import PyMuPDFPage, PyMuPDFDocument
from gmft.pdf_bindings.common import BasePage
from gmft.table_function_algorithm import _iob

import pandas as pd
import polars as pl
import re

# correction_df = pd.read_csv("C:/conjunct/vandy/yang/reocr/results/micros_resnet_v1.csv")
def load_correction_df(micros_path: str, all_pdfs_for_sanity: list[str]):
    if not micros_path.endswith(".parquet"):
        correction_df = pd.read_csv(micros_path)
        correction_df = correction_df.astype({'pdfname': str})
    else:
        correction_df = pl.read_parquet(micros_path)
        # correction_df = correction_df.with_columns([
        #     pl.col('pdfname').str.replace("\.pdf$", "")
        # ])
        correction_df = correction_df.to_pandas()
        print(correction_df.columns)

    # only get those where cls is "mu" and confidence > 0.98

    correction_df = correction_df[(correction_df['real_char'] == "mu") & (correction_df['confidence'] > 0.98)]

    # sanity check
    expect_pdf_suffix = any([x.endswith(".pdf") for x in all_pdfs_for_sanity])
    for pdfname in correction_df['pdfname']:
        if isinstance(pdfname, str):
            if expect_pdf_suffix:
                assert pdfname.endswith(".pdf"), f"{pdfname} should end with .pdf"
            else:
                assert not pdfname.endswith(".pdf"), f"{pdfname} should not end with .pdf"
        # assert f"{pdfname}.pdf" in all_pdfs_for_sanity, f"{pdfname}.pdf not found in the list of all pdfs"
    commonality = set(correction_df['pdfname']).intersection(set(all_pdfs_for_sanity))
    assert commonality, "No common pdfs found between all_pdfs and correction_df"
    print(f"Common pdfs: {len(commonality)} / {len(all_pdfs_for_sanity)}")
    return correction_df

class PyMuPDFDocument_REOCR(PyMuPDFDocument):
    
    _re_mM = re.compile(r"\bmM\b", re.IGNORECASE)
    def __init__(self, filename: str, correction_df: pd.DataFrame):
        super().__init__(filename)
        self.filename = filename
        self.correction_df = correction_df
    
    def _correct_page(self, page: PyMuPDFPage, subset: list[tuple]):
        # monkeypatch page.get_positions_and_text
        def rectify_func(old_func):
            def result():
                for x0, y0, x1, y1, text in old_func():
                    # check if match \bmM\b
                    if PyMuPDFDocument_REOCR._re_mM.search(text):
                        # check IOB
                        for rect in subset:
                            score = _iob(rect, (x0, y0, x1, y1)) # if the rect mostly covers the mu
                            if score > 0.5:
                                # print("FOUND: ", score)
                                # replace mM with µM
                                good_text = PyMuPDFDocument_REOCR._re_mM.sub("µM", text)
                                yield x0, y0, x1, y1, good_text
                                break
                        else:
                            yield x0, y0, x1, y1, text # + f" ({(x0, y0, x1, y1)})"  
                    else:
                        yield x0, y0, x1, y1, text
            return result
        page.get_positions_and_text = rectify_func(page.get_positions_and_text)
            
        
    def get_page(self, n: int) -> BasePage:
        prev = super().get_page(n)
        # df has columns pdfname,pageno,ctr,real_char,x0,y0,x1,y1
        # need to match pdfname, pageno
        basename = os.path.basename(self.filename)
        if basename.endswith(".pdf"):
            basename = basename[:-4]
        good_df = self.correction_df[(self.correction_df['pdfname'] == basename) & (self.correction_df['pageno'] == n)]
        
        tuples = good_df[['x0', 'y0', 'x1', 'y1']].apply(tuple, axis=1)
        
        self._correct_page(prev, tuples)
        return prev