from enum import Enum
from typing import List, Tuple, Union
import polars as pl
import re
import copy

from tqdm import tqdm
import yaml
import ryaml

from enzyextract.post.metadata.regurgitation import is_content_regurgitated
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
    data_df, data_errors = data_to_df(data_list)
    # data_errors = normalize_data(data_list)
    errors.extend(data_errors)
    
    context_list = obj.get('context') or {}
    if context_list == '{}':
        context_list = {}
    grain, context_chaff = thresh_context(context_list)
    if not context_chaff:
        context_chaff_out = []
    else:
        context_chaff_out = [context_chaff]
    result = {
        'data': data_df,
        'context_grain': [grain],
        'context_chaff': context_chaff_out,
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
    ], how='diagonal_relaxed')
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
    chaff = records['context_chaff']
    if chaff:
        rulebreakers_df = records_wide_to_long_df(records['context_chaff'], pmid)
    else:
        rulebreakers_df = None
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
    custom_ids: Union[str, List[str]],
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
        'context_grain': [],
        'rulebreakers': [],
        'errors': [],
    }

    yaml_idx = 0
    for c, pmid, custom_id in tqdm(zip(contents, pmids, custom_ids), total=len(contents)):
        is_regurgitation = is_content_regurgitated(c) or None

        c = c.replace('\nextras:\n', '\ndata:\n') # blunder
        _generator = fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(c, current_pmid=pmid))
        for _, yaml in _generator: 

            new_stuff = str_completion_to_records(yaml)
            for k, vlist in new_stuff.items():
                if isinstance(vlist, list):
                    # put the 'pmid' column in there
                    if k == 'context_chaff':
                        # special case
                        if not vlist or vlist == [{}]:
                            pass
                        else:
                            extraction_per_yaml['rulebreakers'].append(
                                records_wide_to_long_df(vlist, custom_id)
                            )
                    else:
                        # in general
                        for i, v in enumerate(vlist):
                            if isinstance(v, dict):
                                vlist[i] = {
                                    'pmid': pmid, 
                                    'custom_id': custom_id, 
                                    **v, 
                                    'flag.regurgitation': is_regurgitation
                                }
                        extraction_per_yaml[k].extend(vlist)
                elif isinstance(vlist, pl.DataFrame):
                    extraction_per_yaml[k].append(
                        vlist.insert_column(
                            0,
                            pl.lit(pmid).alias('pmid')
                        ).insert_column(
                            1,
                            pl.lit(custom_id).alias('custom_id')
                        ).with_columns([
                            pl.lit(is_regurgitation, dtype=pl.Boolean).alias('flag.regurgitation'),
                        ])
                    )
                elif vlist is None:
                    pass
                else:
                    raise TypeError(f"Expected list, got {type(vlist)}")
            yaml_idx += 1   
        pass
    # concat all the dataframes

    data_df = pl.concat(
        extraction_per_yaml['data'],
        how='vertical',
    )
    del extraction_per_yaml['data']
    context_df = pl.DataFrame(
        extraction_per_yaml['context_grain'],
        schema={
            'pmid': pl.Utf8,
            'custom_id': pl.Utf8,
            **_complete_ctx_schema,
        }
    )
    del extraction_per_yaml['context_grain']

    rulebreakers_df = pl.concat(
        extraction_per_yaml['rulebreakers'],
        how='vertical'
    )
    del extraction_per_yaml['rulebreakers']
    errors_df = pl.DataFrame(
        extraction_per_yaml['errors'],
        schema=_errors_schema
    )
    del extraction_per_yaml['errors']
    result = {
        'data': data_df,
        'context': context_df,
        'rulebreakers': rulebreakers_df,
        'errors': errors_df
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