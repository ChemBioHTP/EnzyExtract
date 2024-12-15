def read_md_by_pmid(content) -> list[int, str]:
    # look for the pmid header, which is ## PMID: 
    result = []
    current_pmid = None
    current_block = None
    lines = content.split('\n')
    
    # current_pmid = None
    for line in lines:
        if line.startswith("## PMID: "):
            if current_pmid is not None:
                result.append((current_pmid, current_block.strip() + '\n'))
            current_pmid = line[len("## PMID: "):]
            current_block = ""
        elif current_pmid is not None:
            current_block += line + '\n'
    # last one
    if current_pmid is not None:
        result.append((current_pmid, current_block.strip() + '\n'))
    return result