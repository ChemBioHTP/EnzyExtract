from enum import Enum
from typing import List, Tuple, Union
import polars as pl
import re
import copy

from tqdm import tqdm
import yaml
import ryaml

from enzyextract.post.yaml.normalize import Severity, _normalize_context, _normalize_data, explode_strings_into_lists
from enzyextract.utils.yaml_process import explode_field, extract_yaml_code_blocks, fix_multiple_yamls, force_escape_str

def clean_yaml_str_convert_to_dict(content: str) -> dict:
    content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', content)
    
    # we want to read everything as strings, including numbers
    # this is a hack that does that
    
    content = force_escape_str(content)
        


    # obj = yaml.safe_load(content)
    # obj = yaml.load(content, Loader=yaml.CSafeLoader)
    obj = ryaml.loads(content)
    obj = {} if obj is None else obj

    return obj

_data_schema = {
    'descriptor': pl.Utf8,
    'substrate': pl.Utf8,
    'kcat': pl.Utf8,
    'km': pl.Utf8,
    'kcat_km': pl.Utf8,
    'fragments': pl.List(pl.Utf8),
}
_enzyme_ctx_schema = {
    'fullname': pl.Utf8,
    'synonyms': pl.List(pl.Utf8),
    'organisms': pl.List(pl.Utf8),
    'mutants': pl.List(pl.Utf8),
}
_substrate_ctx_schema = {
    'fullname': pl.Utf8,
    'synonyms': pl.List(pl.Utf8),
}
_general_ctx_schema = {
    'temperatures': pl.List(pl.Utf8),
    'pHs': pl.List(pl.Utf8),
    # 'solutions': pl.List(pl.Utf8),
    'other': pl.List(pl.Utf8),
}
_errors_schema = {
    'pmid': pl.Utf8,
    'msg': pl.Utf8,
    'stacktrace': pl.Utf8,
}

def old_yaml_to_pl_dfs():
    return False
    if True:
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


def data_to_df(
    data: list[dict], 
    *, 
    schema_cols_only=True) -> Union[pl.DataFrame, None]:
    """
    Expect data:
    list of dicts with keys:
    - descriptor: str
    - kcat: str
    - km: str
    - kcat/Km: str
    - substrate: str
    """
    # expect km is str

    errors = _normalize_data(data)

    is_severe = any([e['status'] >= Severity.SEVERE for e in errors])
    
    empty_df = pl.DataFrame(schema=_data_schema)
    if is_severe:
        return empty_df, errors
    try:
        df = pl.DataFrame(data, schema_overrides=_data_schema)
        df = pl.concat([empty_df, df], how='diagonal_relaxed')
    except Exception as e:
        errors.append({
            'msg': f"Unable to create dataframe",
            'stacktrace': str(e)
        })
        return None, errors
    if schema_cols_only:
        df = df.select(_data_schema.keys())
    return df, errors


def generic_construct_ctx(records: list, schema_overrides, errors, name) -> pl.DataFrame:
    empty_df = pl.DataFrame(schema=schema_overrides)

    try:
        explode_strings_into_lists(records, schema_overrides)
        generic_ctx = pl.DataFrame(records, schema_overrides=schema_overrides)
        generic_ctx = pl.concat([empty_df, generic_ctx], how='diagonal_relaxed')
        return generic_ctx
    except (pl.exceptions.SchemaError, 
            pl.exceptions.SchemaFieldNotFoundError, 
            pl.exceptions.ShapeError) as e:
        errors.append({
            'msg': f"Unable to create enzymes context dataframe",
            'stacktrace': str(e)
        })
        return empty_df

def context_to_dfs(
    context: dict, 
    *, 
    fix=True) -> Tuple[dict[str, pl.DataFrame], dict]:
    """
    Expect context:
    - enzymes: list of dicts with keys:
        - fullname: str
        - synonyms: str
        - organisms: str
        - mutants: str
    - substrates: list of dicts with keys:
        - fullname: str
        - synonyms: str
    """
    # expect km is str

    errors = _normalize_context(context)

    is_severe = any([e['status'] >= Severity.SEVERE for e in errors])

    if is_severe:
        return {
            'enzyme_ctx': pl.DataFrame(schema=_enzyme_ctx_schema),
            'substrate_ctx': pl.DataFrame(schema=_substrate_ctx_schema),
            'general_ctx': pl.DataFrame(schema=_general_ctx_schema),
        }, errors

    
    enzyme_ctx = generic_construct_ctx(context.get('enzymes', []), _enzyme_ctx_schema, errors, 'enzyme')
    substrate_ctx = generic_construct_ctx(context.get('substrates', []), _substrate_ctx_schema, errors, 'substrate')

    _general = copy.deepcopy(context)
    if 'enzymes' in _general:
        del _general['enzymes']
    if 'substrates' in _general:
        del _general['substrates']
    general_ctx = generic_construct_ctx([context], _general_ctx_schema, errors, 'general')

    return {
        'enzyme_ctx': enzyme_ctx,
        'substrate_ctx': substrate_ctx,
        'general_ctx': general_ctx,
    }, errors



def yaml_to_pl_dfs(content: str, pmid) -> dict[str, pl.DataFrame]:
    """
    `content` is either a string or a dict, containing only valid yaml.
    Does minimal dataframe expanding, and no validation.

    Returns a dict with the keys:
    
    'data': a polars DataFrame with columns: 
        - descriptor, kcat, km, fragments
    'enzyme_ctx': a polars DataFrame with columns: 
        - fullname, synonyms, organisms, mutants,  
        - synonyms, organisms, and mutants are presented in lists. 
            - (calling .explode() will flatten them)
    'substrate_ctx': a polars DataFrame with columns:
        - fullname, synonyms
        - synonyms is presented in a list.
    'general_ctx': a polars DataFrame with columns:
        - temperatures, pHs, other
        - temperatures and pHs are presented in lists.
    'errors': a polars DataFrame with columns:
        - pmid: str, msg: str, stacktrace: str
        - this DataFrame is flattened (one pmid can have multiple errors/rows)
    """
    errors = []
    if isinstance(content, dict):
        obj = content
    elif isinstance(content, str):
        try:

            obj = clean_yaml_str_convert_to_dict(content)
        except (yaml.YAMLError, ryaml.InvalidYamlError) as e:
            errors.append({'pmid': pmid, 'msg': f"Invalid YAML", 'stacktrace': str(e)})
            return {
                'data': pl.DataFrame(schema=_data_schema),
                'enzyme_ctx': pl.DataFrame(schema=_enzyme_ctx_schema),
                'substrate_ctx': pl.DataFrame(schema=_substrate_ctx_schema),
                'general_ctx': pl.DataFrame(schema=_general_ctx_schema),
                'errors': pl.DataFrame(errors, schema=_errors_schema)
            }
    else:
        raise TypeError(f"Expected str or dict, got {type(content)}")

    data_list = obj.get('data') or []
    data_df, data_errors = data_to_df(data_list)
    errors.extend(data_errors)
    
    context_list = obj.get('context') or {}
    context_dfs, context_errors = context_to_dfs(
        context_list, 
        fix=True
    )
    errors.extend(context_errors)
    result = {
        'data': data_df,
        'enzyme_ctx': context_dfs['enzyme_ctx'],
        'substrate_ctx': context_dfs['substrate_ctx'],
        'general_ctx': context_dfs['general_ctx'],
        'errors': pl.DataFrame(errors, schema=_errors_schema)
    }
    for k, df in result.items():
        if df is not None and 'pmid' not in df.columns:
            result[k] = df.insert_column(
                0, 
                pl.lit(pmid).alias('pmid')
            )
    return result
    


def str_completions_to_dfs(
    contents: Union[str, List[str]], 
    pmids: Union[str, List[str]],
):
    """
    Looks for yaml code blocks in the content and converts them to dataframes.
    """

    if isinstance(contents, str):
        contents = [contents]
    if isinstance(pmids, str):
        pmids = [pmids]
    assert len(contents) == len(pmids), "content and pmid must have the same length"

    
    # else:
    #     # assume json content
    #     _generator = [(0, equivalent_from_json_schema(content))]
    extraction_per_yaml = []
    for c, pmid in tqdm(zip(contents, pmids)):
        # pmid = str(pmid_from_usual_cid(custom_id))
        # pmid = custom_id.rsplit('_', 1)[-1]
        
        c = c.replace('\nextras:\n', '\ndata:\n') # blunder
        _generator = fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(c, current_pmid=pmid))
        for _, yaml in _generator: # 
            new_stuff = yaml_to_pl_dfs(yaml, pmid)
            extraction_per_yaml.append(new_stuff)
    
    # concat all the dataframes
    # generals = [x['general_ctx'] for x in extraction_per_yaml]
    # general_agg = generals[0]
    # for g in generals[1:]:
    #     general_agg = pl.concat([general_agg, g], how='diagonal_relaxed')
    #     pass

    result = {
        'data': pl.concat([x['data'] for x in extraction_per_yaml], how='diagonal_relaxed'),
        'enzyme_ctx': pl.concat([x['enzyme_ctx'] for x in extraction_per_yaml], how='diagonal_relaxed'),
        'substrate_ctx': pl.concat([x['substrate_ctx'] for x in extraction_per_yaml], how='diagonal_relaxed'),
        'general_ctx': pl.concat([x['general_ctx'] for x in extraction_per_yaml], how='diagonal_relaxed'),
        'errors': pl.concat([x['errors'] for x in extraction_per_yaml], how='diagonal_relaxed'),
    }
    return result


if __name__ == '__main__':
    test = """data:
    - descriptor: wild-type cat-1
      substrate: H2O2
      kcat: 1 min^-1
      Km: null
      kcat/Km: null
      range: 0.1 - 0.5 mM
    - descriptor: R190Q cat-1; 25°C
      substrate: H2O2
      kcat: 33 ± 0.3 s^-1
      Km: "2.3 mM"
      kcat/Km: null
    - descriptor: R203Q cat-1; (with NADPH); 25°C
      substrate: H2O2
      kcat: null
      Km: 9.9 ± 0.1 µM
      kcat/Km: 4.4 s^-1 mM^-1
context:
    enzymes:
        - fullname: catalase
          synonyms: cat-1
          mutants: wild-type; R190Q; R203Q
          organisms: Escherichia coli
    substrates: 
        - fullname: hydrogen peroxide
          synonyms: H2O2
        - fullname: water
    temperatures: 25°C; 30°C
    pHs: 7.4
    other: NADPH"""
    out = yaml_to_pl_dfs(test, False)
    data_df = out['data']
    print(data_df)