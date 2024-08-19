
import re
import yaml

from kcatextract.backform.backform_utils import fix_the_yaml, isolate_the_yaml


def quality_assure_ai_message(ai_msg: str):
    problems = []
    
    
    # Heuristic 1: check to see if the AI converted by detecting both s^-1 and min^-1
    if 's^-1' in ai_msg and 'min^-1' in ai_msg:
        problems.append("possible conversion between s^-1 and min^-1")
    
    objs = []
    yaml_blocks = []
    # pre_yaml, yaml_block, post_yaml = isolate_the_yaml(yaml_block)
    
    def my_loader(yaml_block):
        objs.append(yaml.safe_load(yaml_block))
        yaml_blocks.append(yaml_block)
        return yaml_block
    ai_msg = fix_the_yaml(ai_msg, my_loader)
    if not yaml_blocks:
        problems.append("no yaml block detected")
    # obj = yaml.safe_load(yaml_block)
    bad_units = ['mol', 'mg', 'U', '/g', 'l/', 'L']
    
    # Heuristic 2: check for bad kcat
    baddies = []
    for obj in objs:
        for item in (obj.get('extras', obj.get('data')) or []):
            kcat = item.get('kcat')
            if kcat:
                unit = ''.join(x for x in kcat if x.isalpha() or x in '^-1')
                if 'min^-1' not in unit and 's^-1' not in unit:
                    baddies.append(kcat)
                    # problems.append("unknown (bad) kcat unit", kcat)

                elif any(bad in kcat for bad in bad_units):
                    baddies.append(kcat)
                    # problems.append("bad kcat unit", kcat)
                else:
                    pass
    if baddies:
        pass
        # print("Warning: bad kcat units detected:", baddies)
            
    ### ONESHOT
    # Heuristic 3: remove any "synonyms: null" from substrate block
    def remove_null_synonyms(yaml_block):
        
        start_substrates = '    substrates:'
        if start_substrates in yaml_block:
            pre, post = yaml_block.split(start_substrates, 1)
            # remove synonyms: null is OK
            yaml_block = pre + start_substrates + re.sub(r'\n          synonyms: null', '', post)
        return yaml_block
    ai_msg = fix_the_yaml(ai_msg, remove_null_synonyms)
        
    
    # TODO maybe turn lists into comma-delineated strings?
    
    # TODO maybe coerce ", " into "; " when necessary?
    to_semicolon_re = re.compile(r'  (temperatures|pHs|solvents|solutions|organisms|mutants|synonyms): "[^;]*, [^;]*"')
    for yaml_block in yaml_blocks:
        if to_semicolon_re.search(yaml_block):
            problems.append("comma delimiter detected")

    
    # TODO maybe flatten conditions, if GPT provides it?
    
    # Heuristic 4: check for fields reported in a non-flattened format
    nonflat_re = re.compile(r'  (temperatures|pHs|solvents|solutions|organisms|mutants|synonyms): ?\n')
    for yaml_block in yaml_blocks:
        if nonflat_re.search(yaml_block):
            problems.append("non-flattened conditions detected")
    
    # Heuristic 5: check for non-whitelisted fields
    for obj in objs:
        for k, v in obj.get('context', {}).items():
            if k not in ['enzymes', 'substrates', 'temperatures', 'pHs', 'solvents', 'solutions', 'organisms', 'mutants', 'synonyms', 'other']:
                problems.append(f"non-whitelisted context field: {k}")
    
    # Heuristic: commute the "data" field into the "extras" field
    # fixed_yaml = False
    # if obj.get('data'):
    #     # uh oh, we would actually want the "extras" field.
    #     # print("Expected data to actually be the 'extras' field", obj)
        
    #     if obj.get('extras'):
    #         # uh i think we delete here
    #         # print("Deleting data field")
            
    #         # print("Pre: \n", yaml_block)
    #         builder = ""
    #         inside_data = False
    #         for line in yaml_block.split('\n'):
    #             if inside_data:
    #                 if not line[0].isspace():
    #                     inside_data = False
                
    #             if not inside_data:
    #                 if line.rstrip() == 'data:':
    #                     inside_data = True
    #                     continue
    #                 builder += line + '\n'
    #         yaml_block = builder
    #         # print("Post: \n", yaml_block)
    #         # print()
                    
    #     else:
    #         # so 'data' but no 'extras' suggests we rename this to extras
    #         # print("Renaming data to extras")
            
    #         # print("Pre: \n", yaml_block)
    #         yaml_block = re.sub(r'^data:', 'extras:', yaml_block, flags=re.MULTILINE)
    #         # print("Post: \n", yaml_block)
    #     # now reserialize
    #     # yaml_block = yaml.safe_dump(obj, indent=4, sort_keys=False)
    #     # actually, we probably want to keep the yaml as is (because of the variety of yaml)
    #     fixed_yaml = True
        
    # now, get rid of problematic kcats
    problematic_kcat = False
    def fix_problematic_kcat(yaml_block):
        builder = ""
        for line in yaml_block.split('\n'):
            for bad in baddies: # if any
                if line.startswith(f'      kcat: "{bad}"'):
                    problematic_kcat = bad
                    builder += f'      kcat: null\n'
                    break
            else: # if none
                builder += line + '\n'
        return builder
    ai_msg = fix_the_yaml(ai_msg, fix_problematic_kcat)
    if problematic_kcat:
        # print("Rectified problematic kcat:", problematic_kcat)
        # fixed_yaml = True
        
        pre_yaml += "Reminder: I only include kcat with units of time^-1, like min^-1 or s^-1. I must leave out Vmax and specific activity."
        pre_yaml += "\n\n"
    
    # return problems, f"{pre_yaml}```yaml\n{yaml_block}```{post_yaml}"
    return problems, ai_msg


def quality_assure_for_enzyme_matching(req: dict, golden_idents: list[str] = []):
    """Does a few checks, to help improve the overall quality of the outputs
    :param golden_idents: idents that we know for sure are match"""
    
    golden_idents = [x.lower() for x in golden_idents]
    
    problems = []
        
    assert req['messages'][0]['role'] == 'system'
    
    assert req['messages'][1]['role'] == 'user'
    doc_msg = req['messages'][1]['content']
    
    # expect a yaml in doc_msg
    pre_in, yaml_in, post_in = isolate_the_yaml(doc_msg)
    if pre_in is None:
        problems.append("no yaml block detected")
        return problems
    
    # in_obj = yaml.safe_load(yaml_in)
    
    # now get all identifiers
    idents_in = set()
    for line in post_in.split('\n'):
        line = line.lower()
        if line.startswith('[fasta') or line.startswith('[pdb') or line.startswith('[genbank'):
            ident = line.split(' ')[1].strip(']')
            assert ident
            idents_in.add(ident.lower())
    
    assert req['messages'][-1]['role'] == 'assistant'    
    
    ai_msg = req['messages'][-1]['content']
    
    pre_out, yaml_out, post_out = isolate_the_yaml(ai_msg)
    if pre_out is None:
    # if '```yaml' not in ai_msg:
        problems.append("no yaml block detected")
        return problems
    
    yaml_out = yaml_out.replace("# for identifiers that are relevant but aren't matched", '')
    
    try:
        out_obj = yaml.safe_load(yaml_out)
    except yaml.YAMLError:
        problems.append("invalid yaml detected")
        return problems
    
    # make sure that gpt doesn't hallucinate any new identifiers
    # for key, 
    idents_out = set()
    for enzyme in out_obj.get('enzymes') or []:
        for key in ['fasta', 'pdb', 'uniprot']:
            idents = enzyme.get(key) or ''
            if '; ' in idents:
                idents = idents.split('; ')
            else:
                idents = idents.split(', ')
            for ident in idents:
                if ident:
                    idents_out.add(ident.lower())
    
    for ident in idents_out:
        if ident not in idents_in:
            # print("Hallucinated identifier:", ident)
            problems.append(f"hallucinated identifier: {ident}")
            return problems
    
    # require that certain idents are here
    for ident in idents_in:
        if ident in golden_idents:
            if ident not in idents_out:
                problems.append(f"GPT missed a highly likely identifier {ident}")
            else:
                pass
                print("GPT successfully captured a likely ident", ident)
        
    
    req['messages'][-1]['content'] = f"{pre_out}```yaml\n{yaml_out}```{post_out}"
    
    return []
    


def quality_assure_finetune(req: dict):
    """Does a few checks, to help improve the overall quality of the outputs"""
    
    # for req in finetune_request:
    
    problems = []
        
    assert req['messages'][0]['role'] == 'system'
    
    assert req['messages'][1]['role'] == 'user'
    doc_msg = req['messages'][1]['content']
    
    tableless = False
    if 'No yaml available. Construct from scratch!\n```yaml\ncontext:\n    null\ndata:\n    null\n' in doc_msg:
        # print("Fixed old [no doc] message")
        tableless = True
        req['messages'][1]['content'] = f"""\
No yaml available. Construct the output yaml directly.
```yaml
context:
null
data:
null
```                    
"""
    else:
        pass
        # print("Warning: this GPT was actually given a yaml")
    
    assert req['messages'][-1]['role'] == 'assistant'    
    
    ai_msg = req['messages'][-1]['content']
    problems, fixed_ai_msg = quality_assure_ai_message(ai_msg)
    req['messages'][-1]['content'] = fixed_ai_msg
    return problems