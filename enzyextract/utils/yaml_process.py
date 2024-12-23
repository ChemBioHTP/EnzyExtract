from typing import Optional

import yaml

import pandas as pd

import re


class YamlVersions:
    _semicolon = 1
    _improve = 2

    _enzymes_block = 4
    _substrates_block = 8

    ORIG = 64
    IMPROVE = 128 | _enzymes_block | _improve
    ONESHOT = 256 | _semicolon | _enzymes_block | _substrates_block


def explode_field(v: str | list | None, prefer_semicolons=True) -> list:
    """Turns a delimited string; ie. wild-type; R667W; R667A into a list.
    Works for string, list, null, None."""
    if not v:
        return []
    if isinstance(v, list):
        return v
    sep = ', '
    if prefer_semicolons:
        sep = '; '
        if sep not in v:
            sep = ', '
    return v.split(sep=sep)


def do_auto_context(descriptor_data, context, prefer_semicolons=True):
    # we are able to auto-fill enzyme, substrate, etc. fields from context
    # TODO: do the "(with)" parsing
    builder = []
    enzyme_specs = None
    # if isinstance(context.get('enzymes'), dict):
    #     enzymes = context['enzymes'].get('data')
    # if isinstance(context.get('substrates'), dict):
    #     substrates = context['substrates'].get('data')

    context = context.copy() # type: dict[str, list[str]]

    # flatten context, and extract the enzyme and substrate specs (thesaurus)
    enzyme_specs = None
    substrate_specs = None
    if 'enzymes' in context and context['enzymes']:
        # if list of dicts
        if isinstance(context['enzymes'], list) and isinstance(context['enzymes'][0], dict):
            # flatten
            enzyme_specs = context['enzymes'] # type: list[dict]
            context['enzymes'] = []
            context['mutants'] = []
            context['organisms'] = []
            for eors in enzyme_specs:
                if eors.get('fullname'):
                    context['enzymes'].append(eors['fullname'])
                context['enzymes'].extend(eors['synonyms'])
                context['mutants'].extend(eors['mutants'])
                context['organisms'].extend(eors['organisms'])
    if 'substrates' in context and context['substrates']:
        # if list of dicts
        if isinstance(context['substrates'], list) and isinstance(context['substrates'][0], dict):
            substrate_specs = context['substrates'] # type: list[dict]
            context['substrates'] = []
            for substrate in substrate_specs:
                if substrate.get('fullname'):
                    context['substrates'].append(substrate['fullname'])
                context['substrates'].extend(substrate['synonyms'])

    for entry in descriptor_data:
        tags = explode_field(entry.get('descriptor'), prefer_semicolons=prefer_semicolons)
        obj = {
            'enzyme': None,
            'enzyme_full': None,
            'substrate': entry.get('substrate', None), # change: allow substrate to be directly given
            'substrate_full': None,
            'mutant': None,
            'organism': None,
            'kcat': entry.get('kcat'),
            'km': entry.get('km', entry.get('Km')),
            'kcat_km': entry.get('kcat_km', entry.get('kcat/Km', entry.get('kcat_Km'))),
            'temperature': None,
            'pH': None,
            # 'solvent': None,
            'solution': None,
            'other': [],
            'descriptor': entry.get('descriptor')
        }
        # put to lower
        for key, values in context.items():
            # put to lower
            k = key[:-1] if key.endswith('s') else key
            if key == 'other' and not isinstance(values, list):
                continue # TODO
            if obj.get(k): # do not overwrite
                continue
            
            v_lower = [v.lower() for v in values]
            for tag in tags:
                if values and tag.lower() in v_lower: # if the tag is a member of a context-list
                    # remove "s" from key
                    if k == 'solvent': # forceful transfer
                        obj['solution'] = tag
                    elif k == 'other' or k not in obj: # guard against "inhibitors" or other unspecified types
                        obj['other'].append(tag)
                    else:
                        obj[k] = tag
                    break
            else:
                if values and len(values) == 1 and key not in ['mutants', 'other']: # if singleton, use it
                    if k in obj:
                        obj[k] = values[0]

        # singleton enzyme/substrate


        for key in ['enzyme', 'substrate', 'mutant', 'organism']:
            if obj[key] is None:
                # ugh oh, we really want the enzyme/substrate/mutant
                # go in reverse: check if an enzyme is a substring of a tag

                # for eors in context.get(k + 's') or []:
                #     query = r'\b' + re.escape(eors) + r'\b'
                #     # TODO check if this proceeds in tag order. No it doesn't.
                #     # TODO fix
                #     if any(re.search(query, tag, re.IGNORECASE) for tag in tags):
                #         obj[k] = eors
                #         break
                targets = context.get(key + 's') or []
                targets.sort(key=len, reverse=True) 
                # longest first (most specific to least specific)
                queries = []
                for eors in targets:
                    queries.append((r'\b' + re.escape(eors) + r'\b', eors))
                for tag in tags: # this will be in tag order
                    if "(with" in tag:
                        # this is a coenzyme, cannot classify it as enzyme or substrate
                        continue
                    for query, eors in queries:
                        if re.search(query, tag, re.IGNORECASE):
                            obj[key] = eors
                            break



        if obj['pH'] is None:
            for tag in tags:
                if 'pH' in tag:
                    obj['pH'] = tag.replace('pH', '').strip()
                    break
        if obj['temperature'] is None:
            for tag in tags:
                if '°C' in tag:
                    obj['temperature'] = tag.replace('°C', '').strip()
                    break
        if obj['organism'] is None:
            # gpt has the habit of abridging the organism; for instance, providing e. coli while
            # contextualizing with escherichia coli
            # TODO
            pass
        obj['other'] = '; '.join(obj['other'])
        if obj['other'] == '':
            obj['other'] = None # consistency


        # try to locate the full name
        # this is special to the "partB" and "improve" and "oneshot" series
        for key, specs in [('enzyme', enzyme_specs), ('substrate', substrate_specs)]:
            if not specs:
                continue

            proceedwith = None
            if obj[key] is not None:
                # orig case is fine, because we assign obj['enzyme'] to the orig case
                for eors in specs: # could also be substrate
                    if obj[key] in eors.get('synonyms', []) or obj[key] in eors.get('fullname', ''):
                        proceedwith = eors

            elif len(specs) == 1:
                # if there's only 1, we can infer by singleton
                eors = specs[0] # enzyme or substrate
                proceedwith = eors

            elif key == 'enzyme' and obj['mutant'] is not None:
                # enzyme is None, but mutant is not None, suggests that it matched with some sort of mutant
                for eors in specs:
                    if obj['mutant'] in eors['mutants']:
                        if proceedwith is None:
                            proceedwith = eors
                        else:
                            # if there are 2, then it's ambiguous
                            proceedwith = None
                            break


            if proceedwith is not None:
                obj[f'{key}_full'] = proceedwith['fullname']
                if not obj[key]:
                    obj[key] = proceedwith['fullname']
                if key == 'enzyme' and len(proceedwith['organisms']) == 1 and not obj['organism']:
                    obj['organism'] = proceedwith['organisms'][0]


        builder.append(obj)
    return builder


def fix_df_for_yaml(df):
    # possible renames: Km --> km
    # possible renames: kcat/Km --> kcat_km
    df = df.rename(columns={
        'Km': 'km',
        'kcat/Km': 'kcat_km'
    })
    if 'km' not in df.columns:
        df['km'] = None
    if 'kcat' not in df.columns:
        df['kcat'] = None
    if 'kcat_km' not in df.columns:
        df['kcat_km'] = None
    if 'descriptor' not in df.columns:
        df['descriptor'] = None
    return df

string_acceptors = None
# string_acceptors = ['descriptor', 'kcat', 'Km', 'kcat/Km', 'substrate', 
#             'fullname', 'synonyms', 'mutants', 'organisms', 'temperatures', 'pHs', 'solvents', 
#             'solutions', 'other']

def force_escape_str(yaml_block: str) -> str:
    builder = ""
    for line in yaml_block.split('\n'):
        if ': ' in line:
            key, value = line.split(': ', 1)
            if value == 'null' or value == '[]':
                continue # exception
            if string_acceptors is None or key.strip('- ') in string_acceptors:
            # allow any
                if value and not (value[0] in '"\'' and value.strip()[-1] in '"\''):
                    escaped_str = value.replace('"', '\\"')
                    line = f'{key}: "{escaped_str}"'
        elif line.strip().startswith('- '):
            space, value = line.split('- ', 1)
            if value and not (value[0] in '"\'' and value.strip()[-1] in '"\''):
                escaped_str = value.replace('"', '\\"')
                line = f'{space}- "{escaped_str}"'
            
        builder += line + '\n'
    return builder
def parse_yaml(content: str, debugpmid=None):
    content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', content)
    
    # we want to read everything as strings, including numbers
    # this is a hack that does that
    
    content = force_escape_str(content)
        

    try:
        obj = yaml.safe_load(content)
        obj = {} if obj is None else obj
    except yaml.YAMLError as e:
        if debugpmid: print(f"[{debugpmid}] WARNING: Invalid YAML", e)
        obj = {}
    return obj


def validate_data(obj: list, fix=True, debugpmid=None):
    # expect km is str
    all_correct = True
    for datum in obj:
        delete_keys = []
        if not isinstance(datum, dict):
            if debugpmid: print(f"[{debugpmid}] Data is not a dict but a", type(datum), ":", datum)
            all_correct = False
            continue
        for k, v in datum.items():
            if k in ['descriptor', 'kcat', 'Km', 'km', 'kcat/Km', 'substrate']:
                if v and not isinstance(v, str):
                    if debugpmid: print(f"[{debugpmid}] Key {k} is not a string")
                    delete_keys.append(k)
                    all_correct = False
                elif v and k in ['Km', 'km', 'kcat'] and '; ' in k:
                    # do NOT allow lists to be provided into km
                    if debugpmid: print(f'[{debugpmid}] Key "{k}" is semicolon-delimited list')
            else:
                # if debugpmid: print(f'[{debugpmid}] Data has unexpected key "{k}"')
                # allow this for now
                delete_keys.append(k)
                # all_correct = False
        if fix and delete_keys:
            for k in delete_keys:
                del datum[k]

    return all_correct


def validate_context(obj: dict, fix=True, debugpmid=None, version: int=YamlVersions.ORIG):
    # expect km is str
    all_correct = True

    parent_deletable_keys = []
    
    if not isinstance(obj, dict):
        if debugpmid: print(f"[{debugpmid}] Context is not a dict but a", type(obj), ":", obj)
        return False

    for k, v in obj.items():
        # you know what, drop "other". pretty much useless
        if k == 'other':
            parent_deletable_keys.append(k)
            continue
        allowable = ['enzymes', 'substrates', 'temperatures', 'pHs', 'solvents', 'solutions', 'other'] # , 'inhibitors']
        substrates_like = ['substrates', 'coenzymes', 'cosubstrates', 'products', 'inhibitors', 'cofactors']
        if not (version & YamlVersions._enzymes_block):
            # if enzymes_block, these fields should be nested
            allowable.extend(['mutants', 'organisms'])

        if k not in allowable:
            # Scary: allow it and assume that auto_context should slot it into the "other" category
            if debugpmid and k not in substrates_like:
                print(f'[{debugpmid}] Context has unexpected key "{k}"')
            parent_deletable_keys.append(k)
            # all_correct = False
            continue

        # now, verify types
        # begrudgingly allow either string or list of strings everywhere
        if k == 'enzymes':
            if (version & YamlVersions._enzymes_block):
                if not v:
                    # set to empty list
                    obj['enzymes'] = []
                    continue
                if isinstance(v, list):
                    for enzyme in v:
                        if not isinstance(enzyme, dict):
                            if debugpmid: print(f"In {debugpmid}: context key enzymes has a non-dict item:", enzyme)
                            all_correct = False
                else:
                    if debugpmid: print(f"In {debugpmid}: context key enzymes is not a list:", v)
                    all_correct = False
            else:
                if v and not isinstance(v, (str, list)):
                    if debugpmid: print(f"In {debugpmid}: context key enzymes is not a string or list:", v)
                    all_correct = False
        elif k == 'substrates':
            if (version & YamlVersions._substrates_block):
                if not v:
                    # set to empty list
                    obj['substrates'] = []
                    continue
                if isinstance(v, list):
                    for substrate in v:
                        if not isinstance(substrate, dict):
                            if debugpmid: print(f"In {debugpmid}: context key substrates has a non-dict item:", substrate)
                            all_correct = False
                        elif 'fullname' not in substrate:
                            if debugpmid: print(f"In {debugpmid}: context key substrates has a dict item without 'fullname':", substrate)
                            all_correct = False
                else:
                    if debugpmid: print(f"In {debugpmid}: context key substrates is not a list:", v)
                    all_correct = False
            else:
                if v and not isinstance(v, (str, list)):
                    if debugpmid: print(f"In {debugpmid}: context key substrates is not a string or list:", v)
                    all_correct = False
        else:

            if isinstance(v, list):
                # again, begrudgingly allow lists of only strings
                # if k == 'other':
                    # begrudgingly allow list of dicts I guess
                    # for i, item in enumerate(v):
                    #     if isinstance(item, dict):
                    #         obj['other'][i] = '; '.join([k + ": " + v for k, v in item.items()])
                # else:
                for item in v:
                    if not isinstance(item, str):
                        if debugpmid: print(f'In {debugpmid}: context key "{k}" has a non-string item:', item)
                        all_correct = False
            elif isinstance(v, dict):
                # solely allow dict for the "other" field
                if k == 'other':
                    # flatten
                    obj['other'] = '; '.join([k + ": " + v for k, v in v.items()])
                else:
                    if debugpmid: print("Warning: unexpected dict in context. Got a dict in", k, "for pmid", debugpmid)
                    all_correct = False
            elif v and not isinstance(v, str):
                if debugpmid: print(f"In {debugpmid}: context key {k} is not a string or list, but a {type(v)}:", v)
                all_correct = False

    if fix and parent_deletable_keys:
        for k in parent_deletable_keys:
            del obj[k]
    return all_correct


def explode_context(obj: dict, debugpmid=None, yaml_version: int=YamlVersions.ORIG) -> dict:
    context = obj.get('context', {})
    # process context
    # To determine separator
    # luckily, we can split by ', ' and this will even permit substrates with commas like '2,3-dihydroxybenzoate'
    # rule:
    # if YamlVersions._semicolon is set, 
    # prefer to split by semicolon. But occasionally, GPT still prefers to use comma
    # thus if no semicolon is found, revert to comma

    for k, v in context.items():
        if v is None:
            context[k] = [] # pass # TODO should i turn into []?
        elif isinstance(v, str):
            # rule: 
            sep = ', '
            if yaml_version & YamlVersions._semicolon:
                sep = '; '
                if sep not in v and k != 'descriptor': # always split descriptor with ; 
                    sep = ', '
            context[k] = v.split(sep=sep)
        # if it's already a list: we're golden
        elif isinstance(v, list):
            # uhhh
            if k == "enzymes":
                # true for "improve" and "oneshot" series
                if v and isinstance(v[0], dict):
                    # need to explode v['synonyms'], v['mutants'], and v['organisms']
                    for enzyme in v:
                        enzyme['synonyms'] = explode_field(enzyme.get('synonyms'), prefer_semicolons=True)
                        enzyme['mutants'] = explode_field(enzyme.get('mutants'), prefer_semicolons=True)
                        enzyme['organisms'] = explode_field(enzyme.get('organisms'), prefer_semicolons=True)
                        if isinstance(enzyme.get('fullname', []), list):
                            enzyme['fullname'] = '; '.join(enzyme.get('fullname', []))
            elif k == "substrates":
                # true for "oneshot" series
                if v and isinstance(v[0], dict):
                    for substrate in v:
                        substrate['synonyms'] = explode_field(substrate.get('synonyms'), prefer_semicolons=True)
            else:

                # let's not coerce for now
                # as this will just cause problems down the road
                # context[k] = [str(x) for x in v]
                pass
        elif isinstance(v, dict):
            # unexpected
            if debugpmid: print(f'Warning: unexpected dict in context. Got "{k}". For pmid {debugpmid}')
            # del context[k]
            context[k] = []
    obj['context'] = context
    return obj


def yaml_to_df(content: str | dict, auto_context=False, version: int=YamlVersions.ONESHOT, debugpmid=None, verbose=True) -> tuple[pd.DataFrame, dict]:

    if isinstance(content, str):
        obj = parse_yaml(content, debugpmid=debugpmid)
        obj['data'] = obj.get('data') or []
        valid = validate_data(obj['data'], debugpmid=debugpmid)
        obj['context'] = obj.get('context') or {}
        valid = valid and validate_context(obj['context'], debugpmid=debugpmid, version=version)
        if not valid:
            return pd.DataFrame(), {}
        explode_context(obj, debugpmid=debugpmid, yaml_version=version)
    else:
        obj = content

    data = obj.get('data') or []
    context = obj['context']

    # improved_yaml = 'enzymes' in obj
    # then our yaml is an "improved" yaml

    if auto_context:
        data = do_auto_context(data, context)
    df = pd.DataFrame(data)
    df = fix_df_for_yaml(df)
    return df, context


def validate_yaml(obj: dict, debugpmid=None):
    # prevent bad yaml from blowing up the app
    result = True
    search = obj.get('data')
    search = search if search else []
    search2 = obj.get('extras')
    search2 = search2 if search2 else []

    return validate_data(search + search2, fix=False)


def merge_2_yamls(base: str | dict, extras: str | dict, debugpmid=None, base_ver: int=YamlVersions.ORIG, extra_ver: int=YamlVersions.IMPROVE) -> dict:

    if isinstance(base, str):
        base = parse_yaml(base)
        explode_context(base, debugpmid=debugpmid, yaml_version=base_ver)
    if isinstance(extras, str):
        extras = parse_yaml(extras)
        explode_context(extras, debugpmid=debugpmid, yaml_version=extra_ver)
        if not validate_yaml(extras, debugpmid):
            if debugpmid: print(f"Warning: pmid {debugpmid} broke instructions by having invalid YAML.")
            return {}

    context = base.get('context', {}).copy() #type: dict
    for k, v in extras.get('context', {}).items():
        if not v:
            continue
        setme = context.get(k) or []
        setme.extend(v)
        context[k] = setme

    # conditions where we might want to keep the "data" field from extras
    data = base.get('data') or []
    extra_data = extras.get('data') or []
    if data and extra_data and len(extra_data) != len(data):
        if debugpmid: print(f"Warning: pmid {debugpmid} broke instructions by directly adding to the 'data' field.")
        # not even attempt to rectify
    else:
        extra_extras = extras.get('extras', extra_data)
        extra_extras = extra_extras if extra_extras else []
        data.extend(extra_extras)
    return {"data": data, "context": context}


def extract_yaml_code_blocks(content, current_pmid=None) -> list[tuple[str, str]]:
    """
    Reads markdown: either multiple GPT responses or a single response.
    
    Return list of tuple of (yaml_string, pmid)"""
    # extract all the content inside ```yaml code blocks
    # return list of strings
    result = []
    lines = content.split('\n')
    i = 0

    # current_pmid = None
    while i < len(lines):
        line = lines[i]

        if line.startswith("## PMID: "): # TODO make this slightly more robust
            current_pmid = line[len("## PMID: "):]

        elif "```yaml" in line:
            yaml = ""
            for j in range(i+1, len(lines)):
                if "```" in lines[j]:
                    break
                yaml += lines[j] + '\n'
            result.append((yaml, current_pmid))
            i = j
        i += 1
    return result


def fix_multiple_yamls(file_path: Optional[str]=None, yaml_blocks: Optional[list[tuple[str, int]]]=None):
    """Sometimes, a single PMID has multiple YAML blocks. (ie. a data yaml for each table, and
    context is given in its own block.) This tries to address that"""

    if yaml_blocks is None:
        if file_path is None:
            raise ValueError("Need to provide either an iterable of yaml blocks or a file path")
        with open(file_path, 'r', encoding='utf-8') as f:
            yaml_blocks = extract_yaml_code_blocks(f.read())

    pmid_to_yamls = {}
    for yaml, pmid in yaml_blocks:
        if pmid not in pmid_to_yamls:
            pmid_to_yamls[pmid] = []
        pmid_to_yamls[pmid].append(yaml)
    # now, we need to do this operation:
    # identify the yaml that contains the context (usually the end)
    # then, concat each data yaml with the context yaml
    # result = []
    for pmid, yamls in pmid_to_yamls.items():
        if len(yamls) == 1:
            yield pmid, yamls[0] # we are done
            continue
        elif not yamls:
            continue

        print(f"[{pmid}] Multiple YAMLs found")
        context = None
        data = []
        for yaml in yamls:
            if 'context' in yaml and 'data' not in yaml:
                if context is not None:
                    print(f"[{pmid}] Multiple CONTEXT YAMLs found")
                context = yaml
            else:
                if 'data' not in yaml:
                    print(f"[{pmid}] No data field found")
                    continue
                data.append(yaml)
        if not context:
            print(f"[{pmid}] No context field found")
            context = ''
        for datum in data:
            yield pmid, datum + '\n' + context


def get_pmid_to_yaml_dict(file_path, **kwargs) -> dict:
    """returns a pmid_to_yaml dict"""
    pmid2yaml = {}
    for pmid, yaml in fix_multiple_yamls(file_path=file_path, **kwargs):
        if pmid not in pmid2yaml:
            pmid2yaml[pmid] = ""
        pmid2yaml[pmid] += yaml + '\n'

    return pmid2yaml

def equivalent_from_json_schema(content: str) -> dict:
    """Converts a string which is represented by JSON (common for structured output) into YAML"""
    import json
    obj = json.loads(content)
    # replace the context.pHs to list[str] if it is float
    if 'context' in obj:
        if 'pHs' in obj['context']:
            old_list = obj['context']['pHs']
            new_list = []
            for item in old_list:
                if not isinstance(item, str):
                    new_list.append(str(item))
                else:
                    new_list.append(item)
            obj['context']['pHs'] = new_list
    return obj
    


if __name__ == '__main__':
    test = """data:
    - descriptor: "M. tuberculosis DXP Isomeroreductase, Mg2+, NADPH"
      kcat: "2.1 ± 0.1 s^-1"
      km: "5.0 ± 1.3 µM"
    - descriptor: "M. tuberculosis DXP Isomeroreductase, Mg2+, NADH"
      kcat: "2.8 ± 0.5 s^-1"
      km: "410 ± 140 µM"
    - descriptor: "M. tuberculosis DXP Isomeroreductase, Mn2+, NADPH"
      kcat: "5.0 ± 0.1 s^-1"
      km: "3.3 ± 0.2 µM"
    - descriptor: "M. tuberculosis DXP Isomeroreductase, Mn2+, NADH"
      kcat: "5.6 ± 0.2 s^-1"
      km: "260 ± 20 µM"
    - descriptor: "M. tuberculosis DXP Isomeroreductase, Co2+, NADPH"
      kcat: "1.6 ± 0.1 s^-1"
      km: "0.4 ± 0.1 µM"
    - descriptor: "M. tuberculosis DXP Isomeroreductase, Co2+, NADH"
      kcat: "2.5 ± 0.2 s^-1"
      km: "50 ± 10 µM"
    - descriptor: "M. tuberculosis DXP Isomeroreductase, Mg2+, NADP+"
      kcat: "1.3 ± 0.1 s^-1"
      km: "170 ± 30 µM"
context:
    enzymes: "Aa MDH, Ec MDH, Tf MDH"
    substrates: null
    mutants: null
    organisms: null
    temperatures: "4 °C, 10 °C, 37 °C"
    pHs: null
    solvents: null
    other: null"""
    df, context = yaml_to_df(test, False)
    print(df)
    print(context)