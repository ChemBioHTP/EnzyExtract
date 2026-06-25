import os

from enzyextract.pre.scans.scan_to_parquet import scan_xmls_by_folder
if __name__ == '__main__':
    raise NotImplementedError("This script is only an example.")

    xml_root = 'D:/papers' # XMLs to process
    enzy_root = 'D:/MyExtractionRun/.enzy' # where intermediate data for these XMLs is stored

    print(f"Compressing XMLs to {enzy_root}/scans/xml/xml.parquet")
    df = scan_xmls_by_folder(
        xmls_folder=xml_root,
        recursive=False,
    )
    os.makedirs(f'{enzy_root}/scans/xml', exist_ok=True)
    df.write_parquet(f'{enzy_root}/scans/xml/xml.parquet')