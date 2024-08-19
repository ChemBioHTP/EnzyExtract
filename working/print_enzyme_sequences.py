



from kcatextract.utils.construct_batch import get_resultant_content, locate_correct_batch


if __name__ == "__main__":
    root = "completions/enzymes"
    namespace = "tableless-enzymes"
    # version = "1"
    at, version = locate_correct_batch(root, namespace)
    print("Found version", version)
    
    to_file = f"fetch_sequences/results/{namespace}_{version}.md"
    
    result = """\
## Description:

prompt: enzymes_v1
"""
    for custom_id, content, stop_reason in get_resultant_content(f'{root}/{at}'):
        pmid = custom_id.split('_', 2)[2]
        result += f"## PMID: {pmid}\n\n"
        result += content + "\n\n"
    with open(to_file, 'w', encoding='utf-8') as f:
        f.write(result)
    
    
    
    