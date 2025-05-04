
import os
import polars as pl
from tqdm import tqdm

from enzyextract.post.yaml.pl_parse_yaml import str_completions_to_dfs
from enzyextract.post.yaml.join_context import join_substrate_ctx, join_enzyme_ctx
from enzyextract.submit.batch_decode import jsonl_to_decoded_df



def scan_completions(
    compl_folder: str
):

    cumul = []
    for jsonl in tqdm(os.listdir(compl_folder)):
        if jsonl.endswith('.jsonl'):
            readfrom = f"{compl_folder}/{jsonl}"
            compl = jsonl_to_decoded_df(readfrom, "openai", None)
            cumul.append(compl)
    
    df = pl.concat(cumul, how='vertical')

    fnames = []
    contents = []
    for custom_id, content in df.select('custom_id', 'content').head(400).iter_rows(): # .head(40)
        fname = custom_id.split('_', 2)[2]
        fnames.append(fname)
        contents.append(content)
    
    result = str_completions_to_dfs(contents, fnames)
    data = result['data'].with_row_index('data.pkey')

    step1, sub_pkey = join_substrate_ctx(data, result['substrate_ctx'])
    step2, enz_pkey = join_enzyme_ctx(step1, result['enzyme_ctx'])
    print(step2)