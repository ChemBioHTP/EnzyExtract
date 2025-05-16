
import os
from typing import Union
import polars as pl
from tqdm import tqdm

from enzyextract.post.yaml.pl_parse_yaml import str_completions_to_dfs
from enzyextract.post.yaml.join_context import join_substrate_ctx, join_enzyme_ctx
from enzyextract.post.decode import jsonl_to_decoded_df
from enzyextract.post.yaml.thresh_yaml import threshed_str_completions_to_dfs



def scan_completions(
    compl_folder: Union[str, list[str]],
    top_n: int = 400,
    exclude_partial: bool = True,
):

    if isinstance(compl_folder, (str, os.PathLike)):
        compl_folder = [compl_folder]
    
    if isinstance(compl_folder, (list, tuple)):
        _possible_files = [
            (basename, f"{f}/{basename}")
            for f in compl_folder
            for basename in os.listdir(f)
        ]
    else:
        raise ValueError("compl_folder should be a path or a list of paths.")
    
    cumul = []
    for fname, fpath in tqdm(_possible_files):
        if fname.endswith('.jsonl'):
            if exclude_partial and fname.endswith('0.jsonl'):
                continue
            compl = jsonl_to_decoded_df(fpath, "openai", None)
            cumul.append(compl)
    
    df = pl.concat(cumul, how='vertical')

    fnames = []
    contents = []
    custom_ids = []
    _selected = df.select('custom_id', 'content')
    if top_n is not None:
        _selected = _selected.head(top_n)
    for custom_id, content in _selected.iter_rows():
        fname = custom_id.split('_', 2)[2]
        fnames.append(fname)
        contents.append(content)
        custom_ids.append(custom_id)
    
    # result = str_completions_to_dfs(contents, fnames)
    result = threshed_str_completions_to_dfs(contents, fnames, custom_ids)

    # skip the joining
    return result
    # data = result['data'].with_row_index('data.pkey')

    # step1, sub_pkey = join_substrate_ctx(data, result['substrate_ctx'])
    # step2, enz_pkey = join_enzyme_ctx(step1, result['enzyme_ctx'])
    # # print(step2)
    # return {
    #     'data': step2,
    #     'substrate_ctx': sub_pkey,
    #     'enzyme_ctx': enz_pkey,
    #     'general_ctx': result['general_ctx'],
    # }
