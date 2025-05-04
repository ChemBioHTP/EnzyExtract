"""
Normalizes a ton of python objects, unifying a bunch of diverse schema formats into a single schema.
The term "normalize" is akin to polars's json_normalize.

Accepts an object
"""

import polars as pl

from enzyextract.utils.yaml_process import explode_field


class Severity:
    OK = 0
    MINOR = 1
    SEVERE = 2
    FATAL = 3

    
def explode_strings_into_lists(obj: list[dict], schema, explode_unexpected=True) -> list[dict]:
    for row in obj:
        for k, v in row.items():
            if (
                (schema.get(k) == pl.List(pl.Utf8))
                or (explode_unexpected and k.endswith('s'))
            ):
                if isinstance(v, str):
                    # split on "; "
                    fixed = explode_field(v, prefer_semicolons=True)
                    if fixed:
                        row[k] = fixed

def _normalize_data(data: list[dict]) -> tuple[list[dict], bool]:
    """
    Fixes the data in-place.

    Returns a tuple of (errors).
    """
    errors = []
    if data and not isinstance(data, list):
        msg = f"Data should be a list of dicts but is {type(data)}"
        errors.append({'msg': msg, 'stacktrace': str(data), 'status': Severity.FATAL})
        return errors


    ### Rename bad keys
    rename_keys = {
        'Km': 'km',
        'kcat/Km': 'kcat_km',
        'kcat/km': 'kcat_km'
    }

    ### Fix bad
    for datum in data:
        delete_keys = [] # clean up keys that are not 
        if not isinstance(datum, dict):
            errors.append({
                'msg': f"In data, item should be dict but is {type(datum)}",
                'stacktrace': str(datum)
            })
            continue
        for k, v in datum.items():
            # if k in ['descriptor', 'kcat', 'Km', 'km', 'kcat/Km', 'substrate']:

            # prohibit multiple
            if v and k in ['Km', 'km', 'kcat'] and (
                isinstance(v, list) or
                '; ' in v
            ):
                errors.append({
                    'msg': f"In data, key {k} should be a single value but is a list",
                    'stacktrace': str(v),
                    'status': Severity.OK
                })
                # do NOT allow lists to be provided into km
                if isinstance(v, list) and v and isinstance(v[0], str):
                    # convert v into str
                    v = '; '.join(v)
                    datum[k] = v
                    # else:
                    # delete_keys.append(k)
                else:
                    delete_keys.append(k)
            elif v and not isinstance(v, str):
                errors.append({
                    'msg': f"In data, key {k} should be string but is {type(v)}",
                    'stacktrace': str(v),
                    'status': Severity.OK
                })
                delete_keys.append(k)
            
        # delete bad keys
        for k in delete_keys:
            del datum[k]
        # rename (ie. Km --> km)
        for k in rename_keys:
            if k in datum:
                datum[rename_keys[k]] = v
                del datum[k]
        # add fragments
        datum['fragments'] = datum.get('descriptor') or ''

    explode_strings_into_lists(data, {'fragments': pl.List(pl.Utf8)}, explode_unexpected=False)

    return errors

def homogenize_list(
    v: list, *, 
    errors: list=None, 
    self_name='unknown',
    default_key='fullname', 
    enforce_default_key=True, 
    max_nesting=1,
) -> list[dict]:
    """
    This homogenizes a list of stuff into a list of dicts.
    """
    assert isinstance(v, list), f"Expected list but got {type(v)}"

    for i, item in enumerate(v):
        if isinstance(item, dict):
            if enforce_default_key and default_key not in item:
                item[default_key] = None
                if errors: errors.append({
                    'msg': f"In {self_name}, item is missing {default_key}",
                    'stacktrace': str(item),
                    'status': Severity.OK
                })
        elif isinstance(item, str):
            # is a str, but it should be a list (quietly FIX)
            v[i] = {
                default_key: item
            }
        elif isinstance(item, list):
            if max_nesting > 1:
                # homogenize the nested list
                v[i] = homogenize_list(
                    item, 
                    errors=errors, 
                    self_name=f"{self_name}[{i}]", 
                    default_key=default_key, 
                    enforce_default_key=enforce_default_key, 
                    max_nesting=max_nesting-1
                )
            else:
                # we are at the max nesting level
                if errors: errors.append({
                    'msg': f"In {self_name}, item is a nested list (reached max nesting)",
                    'stacktrace': str(item),
                    'status': Severity.SEVERE
                })
            
        else:
            # good, we are the right type
            if errors: errors.append({
                'msg': f"In {self_name}, item should be dict but is {type(substrate)}",
                'stacktrace': str(item),
                'status': Severity.SEVERE
            })

def _normalize_context(obj: dict):
    """
    This function does a lot of heavy lifting in turning the context into a single consistent schema.
    """
    # expect km is str
    errors = []

    remove_keys = []
    
    if not isinstance(obj, dict):
        errors.append({
            'msg': f"Context should be dict but is {type(obj)}",
            'stacktrace': str(obj),
            'status': Severity.FATAL
        })
        return errors
    
    rename_keys = {
        'pH': 'pHs',
        'temperature': 'temperatures',
        'enzyme': 'enzymes',
        'substrate': 'substrates',
        'solvents': 'solution',
        'solvent': 'solution',
        'solutions': 'solution',
    }
    for k, to in rename_keys.items():
        if k in obj:
            obj[to] = obj[k]
            del obj[k]

    dynamic_renames = {}
    for k, v in obj.items():
        # you know what, drop "other". pretty much useless
        if k == 'other':
            remove_keys.append(k)
            continue

        # now, verify types
        # begrudgingly allow either string or list of strings everywhere
        if k == 'enzymes':
            if not v: # replace None with empty list
                obj['enzymes'] = []
                continue
            if isinstance(v, list):
                homogenize_list(v, errors=errors, self_name='enzymes', default_key='fullname')
                # for enzyme in v:
                #     if isinstance(enzyme, str):
                #         # is a str, but it should be a list (quietly FIX)
                #         v[i] = {
                #             'fullname': enzyme
                #         }
                #     elif not isinstance(enzyme, dict):
                #         errors.append({
                #             'msg': f"In enzymes, item should be dict but is {type(enzyme)}",
                #             'stacktrace': str(enzyme),
                #             'status': Severity.SEVERE
                #         })
            else:
                errors.append({
                    'msg': f"In enzymes, should be list but is {type(v)}",
                    'stacktrace': str(v),
                    'status': Severity.SEVERE
                })
        elif k == 'substrates':
            if not v: # replace None with empty list
                obj['substrates'] = []
                continue
            if isinstance(v, list):
                # for i, substrate in enumerate(v):
                homogenize_list(v, errors=errors, self_name='substrates', default_key='fullname')
            else:
                errors.append({
                    'msg': f"In substrates, should be list but is {type(v)}",
                    'stacktrace': str(v),
                    'status': Severity.SEVERE
                })
        else:
            # this is called a 'GENERIC CONTEXT' field

            # permit 

            if isinstance(v, list):
                # again, begrudgingly allow lists of only strings
                # if k == 'other':
                    # begrudgingly allow list of dicts I guess
                    # for i, item in enumerate(v):
                    #     if isinstance(item, dict):
                    #         obj['other'][i] = '; '.join([k + ": " + v for k, v in item.items()])
                # else:
                if any(isinstance(item, dict) for item in v):
                    # we need to make it a list of dicts
                    homogenize_list(v, errors=errors, self_name=k, default_key='value')
                    dynamic_renames[k] = f'{k}_struct'
                else:
                    # this better be a list of strings
                    obj[k] = '; '.join(v)
            elif isinstance(v, dict):
                # solely allow dict for the "other" field
                if k == 'other':
                    # flatten
                    obj['other'] = '; '.join([k + ": " + v for k, v in v.items()])
                else:
                    # for sub_k, sub_v in v.items():
                    #     if isinstance(sub_v, str):
                    #         pass
                    #     else:
                    #         errors.append({
                    #             'msg': f"In {k} context, item is a deeply nested dict",
                    #             'stacktrace': str(sub_v),
                    #             'status': Severity.SEVERE
                    #         })
                    obj[k] = [v] # convert to list of dicts
                    dynamic_renames[k] = f'{k}_struct'
            elif v and not isinstance(v, str):
                errors.append({
                    'msg': f"In {k} context, item should be string but is {type(v)}",
                    'stacktrace': str(v),
                    'status': Severity.MINOR
                })
                remove_keys.append(k)

    for k in remove_keys:
        del obj[k]
    
    for k, new_k in dynamic_renames.items():
        if k in obj:
            obj[new_k] = obj[k]
            del obj[k]
    return errors
