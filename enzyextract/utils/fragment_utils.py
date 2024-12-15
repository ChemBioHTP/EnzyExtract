import os
def latest_parquet(dirpath) -> tuple[str, float]:
    """
    Find the latest parquet file in a directory.

    Args:
        dirpath (str): The directory to search.

    Returns:
        str: The path to the latest parquet file.
        float: The creation time of the latest parquet file.

    """
    latest = None # best file
    latest_at = None # the time at which the best file was created
    for filename in os.listdir(dirpath):
        if filename.endswith('.parquet'):
            path = os.path.join(dirpath, filename)
            created = os.path.getctime(path)
            if latest_at is None or created > latest_at:
                latest = path
                latest_at = created
    return latest, latest_at

def needs_rebuild(latest_at, fragments_path):
    """
    1. Looks for the latest parquet in latest_path
    2. Check if any parquet in fragments_path is newer than the latest parquet
    3. If so, we need rebuild. 
    4. If there is no latest_path, we need rebuild.
    """
    # _, latest_at = latest_parquet(latest_path)
    if latest_at is None:
        return True
    for filename in os.listdir(fragments_path):
        if filename.endswith('.parquet'):
            path = os.path.join(fragments_path, filename)
            created = os.path.getctime(path)
            if created > latest_at:
                return True
    return False