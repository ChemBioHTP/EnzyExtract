import os
from enzyextract.pre.reocr.m_mu_reocr import script_scan_mM
from enzyextract.pre.scans.scan_to_parquet import scan_papers
from enzyextract.pre.table.scan_tables import process_pdfs
if __name__ == '__main__':
    raise NotImplementedError("This script is only an example.")

    pdf_root = 'D:/papers' # PDFs to process
    enzy_root = 'D:/MyExtractionRun/.enzy' # where intermediate data for these PDFs is stored

    print("Starting mM...")
    script_scan_mM(
        pdf_root=pdf_root, 
        write_dir=f'{enzy_root}/pre/mM', 
        model_path='data/models/resnet18-remicro-iter3.pth',
    )

    print("Starting tables...")
    process_pdfs(
        pdf_root=pdf_root,
        write_dir=f"{enzy_root}/pre/tables",
        micros_path=f"{enzy_root}/pre/mM/mM.parquet"
    )

    print(f"Compressing PDFs to {enzy_root}/scans/pdf/pdf.parquet")
    df = scan_papers(
        pdfs_folder=pdf_root,
        recursive=False,
    )
    os.makedirs(f'{enzy_root}/scans/pdf', exist_ok=True)
    df.write_parquet(f'{enzy_root}/scans/pdf/pdf.parquet')