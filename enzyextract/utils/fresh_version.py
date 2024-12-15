import glob


def next_available_version(dest_folder, filename, file_extension='.jsonl', alert=True):
    version = 1
    old_attempts = glob.glob(f'{dest_folder}/{filename}_*{file_extension}')
    if old_attempts:
        version = max([int(f.rsplit('_', 1)[1].split('.')[0]) for f in old_attempts]) + 1
        if alert:
            print("Using version", version, "for", filename)
    return version

def latest_version(src_folder, filename, file_extension='.jsonl', alert=True):
    next_one = next_available_version(src_folder, filename, file_extension, alert=False)
    if next_one > 1:
        return next_one - 1
    return None