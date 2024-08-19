import os


def pmid_to_tables_from(md_folder) -> dict[str, list[str]]:
    all_tables = [f for f in os.listdir(md_folder) if f.endswith('.md')]
    pmid_to_tables = {}
    for table in all_tables:
        pmid = table.split('_')[0]
        if pmid not in pmid_to_tables:
            pmid_to_tables[pmid] = []
        pmid_to_tables[pmid].append(table)
    pmid_to_tables = dict(sorted(pmid_to_tables.items()))
    return pmid_to_tables