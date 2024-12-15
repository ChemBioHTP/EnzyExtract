# fix pymupdf document by applying redactions. targeting mM -> microM for certain detected
import re
from typing import Generator
import pandas as pd
import pymupdf

_re_mM = re.compile(r"\bmM\b")
_re_mM_i = re.compile(r"\bmM\b", re.IGNORECASE)

def _iob(bbox1: tuple[float, float, float, float], bbox2: tuple[float, float, float, float]):
    """
    Compute the intersection area over box area, for bbox1.
    """
    intersection = pymupdf.Rect(bbox1).intersect(bbox2)
    
    bbox1_area = pymupdf.Rect(bbox1).get_area()
    if bbox1_area > 0:
        return intersection.get_area() / bbox1_area
    
    return 0

def build_paragraph(words: Generator[tuple, None, None]):
    result = ""
    prev_block = 0
    for x0, y0, x1, y1, word, blockno, lineno, wordno in words:
        # if prev_block != blockno:
        #     prev_block = blockno
        #     result += "\n\n"
        # el
        if wordno == 0:
            result += "\n"
        else:
            result += ' '
        result += word
    
    # remove preceding \n if present
    if result:
        assert result[0] == '\n'
        result = result[1:]
    return result

def fix_generator(gen: Generator[tuple, None, None], subset: pd.DataFrame, allow_lowercase=True) -> Generator[tuple, None, None]:
    _re = _re_mM_i if allow_lowercase else _re_mM

    micro_subset = subset[subset['real_char'] == 'mu']
    mM_subset = subset[subset['real_char'] == 'm']
    for x0, y0, x1, y1, word, blockno, lineno, wordno in gen:
        if _re.search(word):
            for _, row in micro_subset.iterrows():
                bbox = (row['x0'], row['y0'], row['x1'], row['y1'])
                if _iob(bbox, (x0, y0, x1, y1)) > 0.5:
                    word = _re.sub('µM', word) # print("Swapped")
                    break
            else:
                for _, row in mM_subset.iterrows():
                    bbox = (row['x0'], row['y0'], row['x1'], row['y1'])
                    if _iob(bbox, (x0, y0, x1, y1)) > 0.5:
                        word = _re.sub('mM', word)
                        break
        yield x0, y0, x1, y1, word, blockno, lineno, wordno
        

def mM_corrected_text(doc: pymupdf.Document, pdfname: str, micro_df: pd.DataFrame, allow_lowercase=True) -> list[str]:
    pmid_subset = micro_df[micro_df['pdfname'] == pdfname]
    result = []
    for pageno, page in enumerate(doc):
        subset = pmid_subset[pmid_subset['pageno'] == pageno]
        result.append(build_paragraph(fix_generator(page.get_text('words'), subset, allow_lowercase=allow_lowercase)))
    return result
                  
    # apply redaction
    # annot = page.add_redact_annot(bbox)
    
    # insert again
    # page.insert_textbox(bbox, "µM")
    # only necessary when visualizing
    # page.apply_redactions(images=pymupdf.PDF_REDACT_IMAGE_NONE)  # don't touch images


def script0():
    # goal: compare manual building versus pymupdf default newline insert
    pmid = 10026218
    doc = pymupdf.open(f"C:/conjunct/tmp/brenda_rekcat_pdfs/{pmid}.pdf")
    
    # print("Manual build")
    manual = build_paragraph(doc[1].get_text('words'))
    
    auto = doc[1].get_text('text')
    
    with open("_debug/manual_pymupdf_build.txt", 'w', encoding='utf-8') as f:
        f.write(manual)
        
    with open("_debug/auto_pymupdf_build.txt", 'w', encoding='utf-8') as f:
        f.write(auto)
    exit(0)

    
def script1():
    # goal: compare manual building versus pymupdf default newline insert
    pmid = 10026218
    doc = pymupdf.open(f"C:/conjunct/tmp/brenda_rekcat_pdfs/{pmid}.pdf")
    
    # print("Manual build")
    micro_df = pd.read_csv("C:/conjunct/vandy/yang/reocr/results/micros_resnet_v1.csv")
    micro_df = micro_df.astype({'pdfname': 'str'})
    
    pmid_subset = micro_df[micro_df['pdfname'] == str(pmid)]
    
    # for pageno in pmid_subset['pageno'].unique():
        # pageno = int(pageno) # convert np.int64 to int
        # page = doc[pageno]
        # subset = pmid_subset[pmid_subset['pageno'] == pageno]
    subset = pmid_subset[pmid_subset['pageno'] == 1]
    manual = build_paragraph(fix_generator(doc[1].get_text('words'), subset))
    
    auto = doc[1].get_text('text')
    
    with open("_debug/manual_pymupdf_build.txt", 'w', encoding='utf-8') as f:
        f.write(manual)
        
    with open("_debug/auto_pymupdf_build.txt", 'w', encoding='utf-8') as f:
        f.write(auto)
    exit(0)

def script2():
    pmid = 10026218
    doc = pymupdf.open(f"C:/conjunct/tmp/brenda_rekcat_pdfs/{pmid}.pdf")
    
    print("Before: ")
    txt = doc[1].get_text("text")
    needle = txt.index("(PIC consisted of leup")
    print(txt[needle:needle+100])
    micro_df = pd.read_csv("C:/conjunct/vandy/yang/reocr/results/micros_resnet_v1.csv")
    micro_df.astype({'pdfname': 'str'})
    mM_corrected_text(doc, pmid, micro_df)
    
    print("After: ")
    txt = doc[1].get_text("text")
    needle = txt.index("(PIC consisted of leup")
    print(txt[needle:needle+100])
    
    
    # save
    # doc.save(f"{pmid}_fixed.pdf")
    
def script3():
    pmid = 10026218
    doc = pymupdf.open(f"C:/conjunct/tmp/brenda_rekcat_pdfs/{pmid}.pdf")
    
    # print("Manual build")
    micro_df = pd.read_csv("C:/conjunct/vandy/yang/reocr/results/micros_resnet_v1.csv")
    micro_df = micro_df.astype({'pdfname': 'str'})
    
    fixed = mM_corrected_text(doc, str(pmid), micro_df)
    assert 'µM' in fixed[1]
    assert 'µM' not in fixed[2]
    assert 'µM' in fixed[5]

if __name__ == '__main__':
    script3()
    
    
    