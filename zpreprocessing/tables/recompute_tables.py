from gmft.auto import AutoFormatConfig, AutoTableFormatter
from tqdm import tqdm

import os


# make correction_df an empty df
# correction_df = correction_df.head(0)

import glob
import shutil
import polars as pl
from pypdfium2 import PdfiumError
import json
import pymupdf
from gmft_pymupdf import PyMuPDFDocument
from gmft.pdf_bindings.bindings_pdfium import PyPDFium2Document
from gmft.table_function import TATRFormattedTable

from m_mu_reocr import PyMuPDFDocument_REOCR

def reprocess_pdfs(src_folder, all_pdfs: list[tuple, tuple], save_dir, config, correction_df, old_dir=None, diffs_only=False):
    """
    root: root folder where the pdfs are
    src_folder: folder where the .info files are
    all_pdfs: list of all pdfs. tuple of (fileroot, filename)
    save_dir: directory where the new markdown files will be saved
    config: AutoFormatConfig for gmft
    old_dir: directory where the old markdown files are (optional, to look at diffs)
    diffs_only: only save files that are different from the old ones
    """
    for fileroot, pdfname in tqdm(sorted(all_pdfs)):

        if pdfname.endswith(".pdf"):
            pdfname = pdfname[:-4]

        doc = None
        try:
            doc = PyMuPDFDocument_REOCR(f"{fileroot}/{pdfname}.pdf", correction_df=correction_df)
            
            for filepath in glob.glob(f"{src_folder}/{pdfname}*.info"):
                # read this thing
                with open(filepath, "r") as f:
                    info = json.load(f)
                page_no = info['page_no']
                table = TATRFormattedTable.from_dict(info, doc[page_no])

                # now compare the new and old csvs. If they are different, also copy the old one to .old.csv
                try:
                    # write csv
                    filename = os.path.basename(filepath)[:-5]
                    

                    tbl_content = table.df(config_overrides=config).to_markdown(index=False)
                    captions = table.captions()
                    content = f"""{captions[0]}

{tbl_content}

{captions[1]}
"""
                    # compare new vs old
                    want_to_save = False
                    
                    if old_dir is not None:
                        with open(f"{old_dir}/{filename}.md", "r", encoding='utf-8') as f:
                            old_text = f.read()
                        if old_text != content:
                            # old_df.to_csv(f"{save_dir}/{filename}.old.csv", index=False)
                            # directly copy file
                            shutil.copy(f"{old_dir}/{filename}.md", f"{save_dir}/{filename}.old.md")
                            want_to_save = True

                    
                    # save 
                    if (not diffs_only) or want_to_save:
                        with open(f"{save_dir}/{filename}.md", "w", encoding='utf-8') as f:
                            f.write(content)

                    
                except FileNotFoundError as e:
                    print("File not found for", e)
                
                except ValueError as e:
                    print(f"Error processing {pdfname}, {filename}, {e}")
                    # also move the image to false_positives
        except PdfiumError as e:
            print(f"Error processing {pdfname}, {e}")
            continue
        except Exception as e:
            raise e
        finally:
            if doc is not None:
                doc.close()
            
            last_processed = pdfname # pdfs = [37, 234, 373, 382, 394, 426, 519, 539, 551, 567, 593]



def main():
    

    config = AutoFormatConfig()
    config.verbosity = 0
    config.enable_multi_header = True
    config.semantic_spanning_cells = True
    config.semantic_hierarchical_left_fill = None # 'algorithm' # 'deep' # v0.4
    # formatter = AutoTableFormatter(config=config)


    # toplevel = 'scratch'
    # secondlevel = 'asm'
    old_dir = None

    # root = f"D:/{toplevel}/{secondlevel}"
    # src_folder = f"C:/conjunct/vandy/yang/corpora/tabular/{toplevel}/{secondlevel}"
    # save_dir = f"C:/conjunct/vandy/yang/corpora/tabular/{toplevel}/{secondlevel}"
    # micros_path = f"C:/conjunct/vandy/yang/reocr/cache/iter3/mM_{toplevel}_{secondlevel}.csv"

    #hotfix
    # root = f"C:/conjunct/tmp/eval/arctic"
    # src_folder = f"C:/conjunct/tmp/eval/beluga_dev/tables"
    # save_dir = f"C:/conjunct/tmp/eval/cherry_dev/tables"
    # micros_path = f"C:/conjunct/tmp/eval/cherry_dev/mMall.parquet"

    # micros_path = f"zpreprocessing/data/pdf_mM.parquet"
    micros_path = f"C:/conjunct/tmp/eval/cherry_prod/mM/wos_remote_all/mMall.parquet"

    # src_folder = f"C:/conjunct/vandy/yang/corpora/tabular/topoff/open"
    # save_dir = f"C:/conjunct/tmp/eval/cherry_prod/tables/topoff/open"
    
    # src_folder = f"C:/conjunct/vandy/yang/corpora/tabular/wos/local_shim"
    # save_dir = f"C:/conjunct/tmp/eval/cherry_prod/tables/wos/local_shim"

    src_folder = f"C:/conjunct/vandy/yang/corpora/tabular/wos/remote_all"
    save_dir = f"C:/conjunct/tmp/eval/cherry_prod/tables/wos/remote_all"
    
    # src_folder = f"C:/conjunct/tmp/eval/manifold_tune/tables_src"
    # save_dir = f"C:/conjunct/tmp/eval/manifold_tune/tables"

    assert os.path.exists(micros_path)
    last_processed = None # list(os.listdir(root))[4020]

    all_pdfnames = set()
    for pdfname in os.listdir(src_folder):
        if pdfname.endswith(".info"):
            real_filename = pdfname[:-5]
            pdf = real_filename.rsplit("_", 1)[0] + '.pdf'
            all_pdfnames.add(pdf)
    
    # whitelist = pl.read_parquet('data/pmids/manifold_tune.parquet')['filename']
    
    # file location lookup
    manifest = pl.read_parquet('data/manifest.parquet')
    manifest = manifest.filter(
        pl.col('filename').is_in(all_pdfnames)
        # & pl.col('filename').is_in(whitelist)
        & pl.col('readable')
    ).select(['fileroot', 'filename']).unique('filename')

    all_pdfs = sorted(manifest.iter_rows(), key=lambda x: x[1])

    from m_mu_reocr import load_correction_df

    # _all_possible_pdfs = list(os.listdir(root))
    correction_df = load_correction_df(micros_path, all_pdfnames)

    if not os.path.exists(save_dir):
        print(f"Making directory {save_dir}")
        os.makedirs(save_dir)

    if last_processed is not None:
        # continue from last processed, possibly redoing last_processed
        all_pdfs = [x for x in all_pdfs if x >= last_processed]

    reprocess_pdfs(src_folder=src_folder, all_pdfs=all_pdfs, save_dir=save_dir, 
                   config=config, correction_df=correction_df, old_dir=old_dir, diffs_only=False)

if __name__ == '__main__':
    main()