
from datetime import datetime
import hashlib
import os
import shutil
import polars as pl


def backup_dataframes(
    targets: list[str],
    backup_folder: str = "data/backup",
):
    """
    Back up specified Parquet files, and also save hashes (basic versioning)
    """
    # today's date
    today = datetime.now().strftime(r"%Y%m%d")

    backup_folder = f"{backup_folder}/{today}"
    # save
    os.makedirs(backup_folder, exist_ok=True)

    metadata = []

    for parquet in targets:
        # save as {basename}_{sha256}.parquet
        # df = pl.read_parquet(parquet)
        # hash the file binary
        
        with open(parquet, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()
        basename = os.path.basename(parquet).replace(".parquet", "")

        backup_file = f"{basename}_{file_hash[:8]}.parquet"
        # df.write_parquet(f"{backup_folder}/{backup_file}")


        shutil.copy2(parquet, f"{backup_folder}/{backup_file}")
        metadata.append({
            "original_file": parquet,
            "backup_file": backup_file,
            "hash": file_hash,
        })

    print("Backed up to:", backup_folder)
    pl.DataFrame(metadata).write_csv(f"{backup_folder}/metadata.csv")