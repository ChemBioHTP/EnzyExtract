"""
Exactly splits a dict into two parts:
1) The "good" part, which is the part that conforms to the schema
2) The "bad" part, which is the part that does not conform to the schema

Separate the wheat from the chaff
"""

import copy
from typing import Tuple
import polars as pl

from enzyextract.post.yaml.normalize import homogenize_list
from enzyextract.post.yaml.schemas import _complete_ctx_schema, _enzyme_ctx_schema, _substrate_ctx_schema
from enzyextract.utils.yaml_process import explode_field


def thresh_str_list(
    v: list | str
) -> Tuple[list[str], list[str]]:
    """
    Enforce (NOT in-place) we have a list of strings
    """
    if isinstance(v, str):
        return explode_field(v, prefer_semicolons=True), []

    ok = []
    bad = []
    for i, item in enumerate(v):
        if item is None or isinstance(item, str):
            # okay
            ok.append(item)
        else:
            # this is a bad key
            bad.append(item)
            continue
    return ok, bad

def thresh_homogenize_list(
    v: list, *, 
    default_key='fullname', 
    enforce_default_key=True,
    max_nesting=1,
    item_schema=None,
) -> list[dict]:
    """
    This homogenizes a list of stuff into a list of dicts. Modifies stuff IN-PLACE.

    enforce_default_key: if True, will make sure every dict has the default key.
    So if a primitive is passed, will convert it to a dict with the default key
    and if a dict does not already have the default key, will add it with a value of None.

    max_nesting: if negative, allow unlimited nesting.
    """
    assert isinstance(v, list), f"Expected list but got {type(v)}"

    bad = []
    delete_keys = []
    for i, item in enumerate(v):
        if isinstance(item, dict):
            if enforce_default_key and default_key not in item:
                item[default_key] = None
        elif isinstance(item, (str, bool, int, float)):
            # is a str, but it should be a list (quietly FIX)
            if enforce_default_key:
                v[i] = {
                    default_key: item
                }
            else:
                delete_keys.append(i)
                bad.append(item)
                continue
        elif isinstance(item, list):
            if max_nesting < 0 or max_nesting > 1:
                # homogenize the nested list
                fixed, removed = thresh_homogenize_list(
                    item, 
                    default_key=default_key, 
                    enforce_default_key=enforce_default_key,
                    max_nesting=max_nesting-1
                )
                v[i] = fixed
                bad.append(removed)
                continue
            else:
                # we are at the max nesting level
                delete_keys.append(i)
                bad.append(item)
                continue
        else:
            delete_keys.append(i)
            bad.append(item)
            continue
        # check the item's schema
        if item_schema is not None:
            removed = thresh_dict(v[i], item_schema)
            if removed:
                bad.append(removed)
    
    for i in sorted(delete_keys, reverse=True):
        v.pop(i)
    
    return bad

def thresh_dict(
    obj: dict,
    schema: dict[str, pl.DataType],
    default_key=None,
):
    """
    Expects a polars schema
    Modifies in place
    """
    assert isinstance(obj, dict), f"Expected dict but got {type(obj)}"
    bad = {}
    remove_keys = []
    for k, v in obj.items():
        if k not in schema:
            # this is a bad key
            bad[k] = v
            remove_keys.append(k)
            continue

        # check the type
        # if isinstance(v, list):
        #     if allow_nesting:

        schema_type = schema[k]
            
        # List of structs
        if schema_type == pl.List(pl.Utf8):
            fixed, removed = thresh_str_list(v)
            obj[k] = fixed
            if removed:
                bad[k] = removed
        elif schema_type == pl.List(pl.Struct):
            # raise NotImplementedError(f"Nesting of {schema_type} in thresh_dict not yet supported")
            item_schema: pl.Struct = schema_type.inner
            if default_key is None:
                if len(item_schema.fields):
                    default_key = item_schema.fields[0].name
                else:
                    default_key = 'fullname'
            
            if not v: # replace None with empty list
                obj[k] = []
                continue
            if isinstance(v, list):
                badv = thresh_homogenize_list(v, default_key='fullname', item_schema=dict(item_schema))
                if badv:
                    bad[k] = badv
            elif isinstance(v, dict):
                # convert to list of dicts and hope for the best
                badv = thresh_dict(v, dict(item_schema))
                if badv:
                    # NOTE: technically, this should be the singleton [badv]
                    bad[k] = badv 
                obj[k] = [v]
            else:
                # unknown type
                bad[k] = v
                remove_keys.append(k)
        # With strings, we may do some coercion
        elif schema_type == pl.Utf8:
            if isinstance(v, str):
                pass
            elif isinstance(v, list) and all(isinstance(i, str) for i in v):
                # list of strings -> str
                obj[k] = '; '.join(v)
            elif isinstance(v, (int, float, bool)):
                # int, float -> str
                obj[k] = str(v)
            elif isinstance(v, dict):
                # ugghhh
                _ok = False
                if len(v) == 1:
                    one_key = list(v.keys())[0]
                    if one_key in ['fullname', 'value']:
                        obj[k] = v[one_key]
                        _ok = True
                
                # in most cases, this is a bad key
                if not _ok:
                    bad[k] = v
                    remove_keys.append(k)
            else:
                # unknown type
                bad[k] = v
                remove_keys.append(k)
            # preserve: None, True, False, strings

        elif schema_type.is_nested():
            raise NotImplementedError(f"Nesting of {schema_type} in thresh_dict not yet supported")
        #     if isinstance(schema_type, pl.List):
        #         pass
    
    for k in remove_keys:
        del obj[k]
    return bad




def thresh_context(obj: dict) -> tuple[dict, dict]:
    """
    This function does a lot of heavy lifting in turning the context into a single consistent schema.

    Separates into two parts:
    1) The "good" part, which is the part that conforms to the schema
    2) The "bad" part, which is the part that does not conform to the schema

    Also, does NOT modify the original object.
    """
    # expect km is str
    obj = copy.deepcopy(obj)
    bad = {}

    remove_keys = []

    if obj is None or obj == '{}' or obj == '':
        # empty
        return {}, {}
    
    if not isinstance(obj, dict):
        return {}, {'context_value': obj}
    
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
    
    # first, try to move "conditions" into the main object
    if 'conditions' in obj:
        conditions = obj['conditions']
        if isinstance(conditions, dict):
            for condition_k, condition_v in conditions.items():
                if condition_k in obj:
                    # this is a collision
                    obj['conditions_' + condition_k] = condition_v
                else:
                    obj[condition_k] = condition_v
            del obj['conditions']
        else: # skip if list or str 
            pass # skip

    # for k, v in obj.items():
    #     # you know what, drop "other". pretty much useless
    #     # if k == 'other':
    #     #     bad[k] = v
    #     #     remove_keys.append(k)
    #     #     continue

    #     # now, verify types
    #     # begrudgingly allow either string or list of strings everywhere
    #     # if schema_type == pl.List(pl.Struct)
    #     schema_type = None
    #     if k in _complete_ctx_schema:
    #         # we have a record that adheres to the schema
    #         # (generic context field)
    #         schema_type = _complete_ctx_schema[k]
    #         # TODO: generalize enzymes and substrates for arbitrary schemas
    #     else:
    #         # this is not in the schema
    #         bad[k] = v
    #         remove_keys.append(k)
    #         continue

    # orig = copy.deepcopy(obj)
    broken = thresh_dict(obj, _complete_ctx_schema)
    bad.update(broken)
    #     for k, schema in [('enzymes', _enzyme_ctx_schema), ('substrates', _substrate_ctx_schema)]:
    #     # if schema_type == pl.List(pl.Struct):
    #         # item_schema = 
    #         if not v: # replace None with empty list
    #             obj[k] = []
    #             continue
    #         if isinstance(v, list):
    #             badv = thresh_homogenize_list(v, default_key='fullname', item_schema=schema)
    #             if badv:
    #                 bad[k] = badv
    #         elif isinstance(v, dict):
    #             # convert to list of dicts and hope for the best
    #             badv = thresh_dict(v, schema)
    #             if badv:
    #                 # NOTE: technically, this should be the singleton [badv]
    #                 bad[k] = badv 
    #             obj[k] = [v]
    #         else:
    #             # unknown type
    #             bad[k] = v
    #             remove_keys.append(k)
        
    #     # now do some coercion
    #     if schema_type == pl.Utf8:
    #         if isinstance(v, list) and all(isinstance(i, str) for i in v):
    #             # list of strings -> str
    #             obj[k] = '; '.join(v)
    #         elif isinstance(v, (int, float, bool)):
    #             # int, float -> str
    #             obj[k] = str(v)
    #         elif isinstance(v, dict):
    #             # ugghhh
    #             _ok = False
    #             if len(v) == 1:
    #                 one_key = list(v.keys())[0]
    #                 if one_key in ['fullname', 'value']:
    #                     obj[k] = v[one_key]
    #                     _ok = True
                
    #             # in most cases, this is a bad key
    #             if not _ok:
    #                 bad[k] = v
    #                 remove_keys.append(k)
    #         else:
    #             # unknown type
    #             bad[k] = v
    #             remove_keys.append(k)
    #         # preserve: None, True, False, strings
    #     elif schema_type == pl.List(pl.Utf8):
    #         if isinstance(v, str):
    #             # str -> list of strings
    #             obj[k] = explode_field(v, prefer_semicolons=True)
    #     else:
    #         raise ValueError(f"Schema type {schema_type} for key {k} not yet supported")
        

    # for k in remove_keys:
    #     del obj[k]

    return obj, bad
