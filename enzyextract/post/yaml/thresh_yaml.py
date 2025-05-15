from enum import Enum
from typing import List, Tuple, Union
import polars as pl
import re
import copy

from tqdm import tqdm
import yaml
import ryaml

from enzyextract.post.yaml.collect_rulebreakers import records_wide_to_long_df
from enzyextract.post.yaml.pl_parse_yaml import clean_yaml_str_convert_to_dict, data_to_df
from enzyextract.post.yaml.schemas import _complete_ctx_schema, _data_schema, _enzyme_ctx_schema, _substrate_ctx_schema, _general_ctx_schema, _errors_schema, rulebreakers_schema
from enzyextract.post.yaml.normalize import Severity, normalize_context, normalize_data, explode_strings_into_lists
from enzyextract.post.yaml.thresh_context import thresh_context
from enzyextract.utils.yaml_process import explode_field, extract_yaml_code_blocks, fix_multiple_yamls, force_escape_str



def str_completion_to_records(content: str) -> dict[str, pl.DataFrame]:
    """
    `content` is either a string or a dict, containing only valid yaml.
    Does minimal dataframe expanding, and no validation.

    Returns a dict with the keys:
    
    'data': pl.DataFrame
    'context_grain': dictionary records with schema adhering to:
        - _complete_ctx_schema
    'context_chaff': dictionary records with completely random schema.
    'errors': dictionary records with schema adhering to:
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
            errors.append({'msg': f"Invalid YAML", 'stacktrace': str(e), 'status': Severity.FATAL})
            return {
                'data': [],
                'context_grain': [],
                'context_chaff': [],
                'errors': errors,
            }
    else:
        raise TypeError(f"Expected str or dict, got {type(content)}")

    data_list = obj.get('data') or []
    # data_df, data_errors = data_to_df(data_list)
    data_errors = normalize_data(data_list)
    errors.extend(data_errors)
    
    context_list = obj.get('context') or {}
    if context_list == '{}':
        context_list = {}
    grain, context_chaff = thresh_context(context_list)
    result = {
        'data': data_list,
        'context_grain': [grain],
        'context_chaff': [context_chaff],
        'errors': errors
    }
    return result

def _smart_create_df(
    records: list[dict],
    schema
):


    # return pl.DataFrame(
    #     records,
    #     schema=schema
    # )
    empty_df = pl.DataFrame(
        schema=schema
    )
    df = pl.concat([
        empty_df,
        pl.DataFrame(
            records,
            schema_overrides=schema
        )
    # ], how='diagonal_relaxed')
    ], how='diagonal')
    return df

def str_completion_to_dfs(content: str, pmid: str) -> dict[str, pl.DataFrame]:
    records = str_completion_to_records(content)
    data_df = _smart_create_df(
        records['data'],
        _data_schema
    )
    context_df = _smart_create_df(
        records['context_grain'],
        _complete_ctx_schema
    )
    rulebreakers_df = records_wide_to_long_df(records['context_chaff'], pmid)
    errors_df = _smart_create_df(
        records['errors'],
        _errors_schema
    )
    result = {
        'data': data_df,
        'context': context_df,
        'errors': errors_df
    }
    for k in result:
        result[k] = result[k].insert_column(
            0,
            pl.lit(pmid).alias('pmid')
        )
    result['rulebreakers'] = rulebreakers_df
    return result



def threshed_str_completions_to_dfs(
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
    extraction_per_yaml = {
        'data': [],
        'context': [],
        'rulebreakers': [],
        'errors': [],
    }
    for c, pmid in tqdm(zip(contents, pmids), total=len(contents)):
        # pmid = str(pmid_from_usual_cid(custom_id))
        # pmid = custom_id.rsplit('_', 1)[-1]
        
        c = c.replace('\nextras:\n', '\ndata:\n') # blunder
        _generator = fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(c, current_pmid=pmid))
        for _, yaml in _generator: # 
            new_stuff = str_completion_to_dfs(yaml, pmid)
            for k, vlist in new_stuff.items():
                extraction_per_yaml[k].append(vlist)
    
    # concat all the dataframes
    # generals = [x['general_ctx'] for x in extraction_per_yaml]
    # general_agg = generals[0]
    # for g in generals[1:]:
    #     general_agg = pl.concat([general_agg, g], how='diagonal_relaxed')
    #     pass

    result = {
        k: pl.concat(v, how='vertical') for k, v in extraction_per_yaml.items()
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