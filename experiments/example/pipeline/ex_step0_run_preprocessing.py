from enzyextract.pre.reocr.m_mu_reocr import script_scan_mM
from enzyextract.pre.table.scan_tables import process_pdfs
if __name__ == '__main__':
    raise NotImplementedError("This script is only an example.")
    # script_scan_mM()
    # script_federated_inference()

    pdf_root = 'path/to/some/folder'
    root = 'path/to/some/folder'
    script_scan_mM(
        pdf_root=pdf_root, 
        write_dir=f'{root}/.enzy/pre/mM', 
        model_path='data/models/resnet18-remicro-iter3.pth',
    )

    process_pdfs(
        pdf_root=pdf_root,
        write_dir=f"{root}/.enzy/pre/tables",
        micros_path=f"{root}/.enzy/pre/mM/mM.parquet"
    ) 