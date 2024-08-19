



from utils.construct_batch import get_resultant_content, locate_correct_batch


if __name__ == "__main__":
    root = "completions/enzy_eval"
    namespace = "eval-brenda-gt"
    # version = "1"
    to_file = f"completions/enzy_eval/{namespace}"
    at, version = locate_correct_batch(root, namespace)
    print("Found version", version)
    
    result = """\
## Description:

prompt: supervisor_v3
"""
    for custom_id, content in get_resultant_content(f'{root}/{at}'):
        pmid = custom_id.split('_', 2)[2]
        result += f"## PMID: {pmid}\n\n"
        result += content + "\n\n"
    with open(f'{to_file}_{version}.md', 'w', encoding='utf-8') as f:
        f.write(result)
    
    
    
    