# feat: use the micromolar correction

import os
from typing import Generator
from gmft_pymupdf import PyMuPDFPage, PyMuPDFDocument
from gmft.pdf_bindings.common import BasePage
from gmft.algo.table_function_algorithm import _iob

import pandas as pd
import polars as pl
import re

import pymupdf

# correction_df = pd.read_csv("C:/conjunct/vandy/yang/reocr/results/micros_resnet_v1.csv")
def load_correction_df(micros_path: str, all_pdfs_for_sanity: list[str]):
    if not micros_path.endswith(".parquet"):
        # correction_df = pd.read_csv(micros_path)
        # correction_df = correction_df.astype({'pdfname': str})
        correction_df = pl.read_csv(micros_path, schema_overrides={'pdfname': pl.Utf8})
    else:
        correction_df = pl.read_parquet(micros_path)
        # correction_df = correction_df.with_columns([
        #     pl.col('pdfname').str.replace("\.pdf$", "")
        # ])
        # correction_df = correction_df.to_pandas()
        print(correction_df.columns)

    # only get those where cls is "mu" and confidence > 0.98

    # correction_df = correction_df[(correction_df['real_char'] == "mu") & (correction_df['confidence'] > 0.98)]
    correction_df = correction_df.filter(
        (pl.col('real_char') == "mu") 
        & (pl.col('confidence') > 0.98)
    )

    # sanity check
    list_of_pdfs_has_suffix = any([x.endswith(".pdf") for x in all_pdfs_for_sanity])
    df_pdfs_has_suffix = any(correction_df['pdfname'].str.ends_with(".pdf"))
    if list_of_pdfs_has_suffix != df_pdfs_has_suffix:
        # pain
        if list_of_pdfs_has_suffix and not df_pdfs_has_suffix:
            # then df needs suffix
            print("Adding suffix to correction_df")
            correction_df = correction_df.with_columns([
                (pl.col('pdfname') + ".pdf").alias('pdfname')
            ])
        elif not list_of_pdfs_has_suffix and df_pdfs_has_suffix:
            # then df needs no suffix
            print("Removing suffix from correction_df")
            correction_df = correction_df.with_columns([
                pl.col('pdfname').str.replace("\.pdf$", "").alias('pdfname')
            ])
        # assert f"{pdfname}.pdf" in all_pdfs_for_sanity, f"{pdfname}.pdf not found in the list of all pdfs"
    commonality = set(correction_df['pdfname']).intersection(set(all_pdfs_for_sanity))
    assert commonality, "No common pdfs found between all_pdfs and correction_df"
    print(f"Common pdfs: {len(commonality)} / {len(all_pdfs_for_sanity)}")
    return correction_df


widest_mM_re = re.compile(r'\bmm(?=$|[\Wo2])', re.IGNORECASE)
ascii_control_re = re.compile(r'[\x00-\x08\x11\x12\x14-\x1F\x7F-\x9F]') # \x7F-\x9F
ends_with_ascii_control_re = re.compile(r'[\x00-\x08\x11\x12\x14-\x1F\x7F-\x9F]$') # \x7F-\x9F

def duplex_correction(orig_text: str, gen: Generator[tuple, None, None], micro_subset: pl.DataFrame) -> list[tuple]:
    """
    Turns a generator of 8-tuples (*bbox, word, *paragraphno) into a list of 8-tuples, 
    with the micro correction.

    The duplex method is used, so the full text contains any unicode control characters.
    """

    words = list(gen)
    # micro_subset = subset[subset['real_char'] == 'mu']

    if micro_subset.is_empty():
        return words

    # there is at least 1 correction to be made
    _re = widest_mM_re

    scrolling_cursor = 0
    result = []
    for i, tup in enumerate(words):
        x0, y0, x1, y1, word, blockno, lineno, wordno = tup

        # retain original whitespace
        up_to = orig_text.index(word, scrolling_cursor)
        # must exist, or else will throw
        whitespace = orig_text[scrolling_cursor:up_to]
        scrolling_cursor = up_to + len(word)

        # remove all true whitespace
        whitespace = whitespace.strip(' \t\n\r\v\f')


        repl = word
        if _re.search(word):
            for bbox in micro_subset.select(['x0', 'y0', 'x1', 'y1']).iter_rows():
            # for bbox in micro_subset.select(['letter_x0', 'letter_y0', 'letter_x1', 'letter_y1']).iter_rows():
                # bbox = (row['x0'], row['y0'], row['x1'], row['y1'])
                if _iob(bbox, (x0, y0, x1, y1)) > 0.5:
                    # replace
                    repl = _re.sub('µM', word)
                    # fix µMo to µmo
                    repl = repl.replace("µMo", "µmo")
                    break
        if whitespace:
            # repl = repl if repl is not None else word
            if ends_with_ascii_control_re.search(whitespace) and (repl.startswith('m') or repl.startswith('M')):
                # consider that a micro M.
                repl = 'µ' + repl
                whitespace = whitespace[:-1]
            # whitespace = whitespace.strip()

            if whitespace.strip():
                repl = whitespace + repl
                # print(f"Added whitespace: >{whitespace}<")
        # if repl != word:
            # print("Change to word:", repl)
        


        # if repl is not None:
            # words[i] = x0, y0, x1, y1, repl, blockno, lineno, wordno
        result.append((x0, y0, x1, y1, repl, blockno, lineno, wordno))
        # the 
    return result


class PyMuPDFDocument_REOCR(PyMuPDFDocument):
    
    _re_mM = re.compile(r"\bmM\b", re.IGNORECASE)
    def __init__(self, filename: str, correction_df: pl.DataFrame):
        super().__init__(filename)
        self.filename = filename
        # just to be safe
        basename = os.path.basename(self.filename)
        if basename.endswith(".pdf"):
            basename = basename[:-4]
        self.correction_df = correction_df.filter(
            pl.col("pdfname").str.replace('\.pdf$', '') == basename
        )

    
    def _correct_page(self, page: PyMuPDFPage):
        # monkeypatch page.get_positions_and_text
        # micro_subset = self.correction_df[
        #     (self.correction_df['pdfname'] == os.path.basename(self.filename)) 
        #     & (self.correction_df['pageno'] == page.number)
        #     & (self.correction_df['real_char'] == 'mu')
        # ]
        micro_subset = self.correction_df.filter(
            (pl.col('pageno') == page.page_number)
            & (pl.col('real_char') == 'mu')
        ) # .to_pandas()
        words = duplex_correction(page.page.get_text('text'), page.get_positions_and_text_mu(), micro_subset)
        # Define the method to yield from `self.words`
        def get_positions_and_text_mu():
            return words

        # Bind the function to the page object as a method
        page.get_positions_and_text_mu = get_positions_and_text_mu
        page._get_positions_and_text_and_breaks = page.get_positions_and_text_mu
            
    _cache = {}
    def get_page(self, n: int) -> BasePage:
        prev = super().get_page(n)
        # df has columns pdfname,pageno,ctr,real_char,x0,y0,x1,y1
        # need to match pdfname, pageno

        # good_df = self.correction_df[(self.correction_df['pdfname'] == basename) & (self.correction_df['pageno'] == n)]
        
        # tuples = good_df[['x0', 'y0', 'x1', 'y1']].apply(tuple, axis=1)
        # if n in self._cache:
            # return self._cache[n]
        self._correct_page(prev)
        # self._cache[n] = prev
        return prev