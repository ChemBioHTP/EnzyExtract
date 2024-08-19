# these pmids are perfect, but too small
# 10473402, 

# these pmids are perfect when looking at the subset with all of {enzyme, substrate, km or kcat}
# 10029307, 10190977, 10320327, 10427036, 10433689, 10438489, 10441376, 10446163, 10473548, 10480865, 
# 10480878, 10480915, 10514486, 10529247, 10531334, 10564758, 10684618, 10714990, 10748206, 10762259, 10801893, 10882170
# 8626758, 8807052, 9398292, 9495750, 9521731, 9556600, 9576908, 9628739, 9636048, 9733678, 9933602

import json
import random
import numpy as np
import pandas as pd

from backform.backform_utils import extract_supervalid_df, split_checkpoint, to_openai_finetune_request
from utils import prompt_collections





def correct_match_info(target_df):
    """Creates the MatchInfo for a correct df"""
    result = [i for i in range(len(target_df))]
    return result

# def random_permute(df_2, match_info, max_permutes=10):

#     assert len(df_2) == len(match_info), f"len(df_2) = {len(df_2)}, len(match_info) = {len(match_info)}"
#     # permutation = np.random.permutation(len(df_2))
#     # read up to max_permutes pairs from permutation, then swap those rows
#     for i in range(0, max_permutes):
#         # if i >= len(permutation) - 1:
#             # break
#         # a = permutation[i]
#         # b = permutation[i+1]
#         a = random.randint(0, len(df_2) - 1)
#         b = random.randint(0, len(df_2) - 1)
#         df_2.iloc[a], df_2.iloc[b] = df_2.iloc[b], df_2.iloc[a]
#         match_info[a], match_info[b] = match_info[b], match_info[a]
#     return df_2, match_info

def random_permute(match_info, max_permutes=10):
    match_info = match_info.copy()
    for i in range(0, max_permutes):
        # if i >= len(permutation) - 1:
            # break
        # a = permutation[i]
        # b = permutation[i+1]
        a = random.randint(0, len(match_info) - 1)
        b = random.randint(0, len(match_info) - 1)
        match_info[a], match_info[b] = match_info[b], match_info[a]
    return match_info


def int_to_letter(index):
    result = ""
    while index >= 0:
        result = chr(index % 26 + ord('A')) + result
        index = index // 26 - 1
    return result

def letter_to_int(letter):
    result = 0
    for char in letter:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1

def construct_ideal_completion(df_brendalike, df_descriptorlike, match_info, deletes_from_brenda=[], deletes_from_descriptor=[]):
    # construct input
    result = ""
    for i in range(max(len(df_brendalike), len(df_descriptorlike))):

        # (e.g. 0 = "A", 26 = "AA", 27 = "AB")
        letter = int_to_letter(i)
        
        if i < len(df_descriptorlike):
            if i in deletes_from_descriptor:
                # replace descriptor with something nondescript like NONE
                descriptor = "not found"
            else:
                desc_row = df_descriptorlike.iloc[i]
                descriptor = desc_row['descriptor']
            
            result += f"{i+1}. {descriptor}\n"
        
        if i < len(df_brendalike):
            if i in deletes_from_brenda:
                result += f"{letter}. not found\n"
            else:
                br_row = df_brendalike.iloc[i]
                enzyme_br = br_row['enzyme']
                substrate_br = br_row['substrate']
                comments_br = br_row.get('comments', br_row.get('descriptor', ''))
                result += f"{letter}. {enzyme_br} | {substrate_br} | {comments_br}\n"
        result += "\n"
    
    
    
    # construct solution
    # assume numbers = descriptor and letters = brenda
    # assumes that match_info describes brendalike: that is, brenda has been shuffled
    # but actually, if match_info is [3, 0, 1, 2] that indicates that at position 3 (so 4.) is what used to be A 
    solution = ""
    index_of_i = [0] * len(match_info)
    for i, j in enumerate(match_info):
        index_of_i[j] = i # [3, 0, 1, 2] -> [1, 2, 3, 0]
    for idx, letter_idx in enumerate(index_of_i):
        letter = int_to_letter(letter_idx)
        if idx in deletes_from_descriptor or letter_idx in deletes_from_brenda:
            letter = "not found"
        solution += f"{idx+1}. {letter}\n"
    
    return result, solution
    
    
def construct_permuted(subset, max_permutes=10, shuffle=True):
    if shuffle:
        subset = subset.sample(frac=1)
    df_br, df_desc = split_checkpoint(subset)
    match_info = correct_match_info(df_br)
    match_info = random_permute(match_info, max_permutes=max_permutes)
    df_br = df_br.iloc[match_info]
    inp, sol = construct_ideal_completion(df_br, df_desc, match_info)
    return inp, sol

def to_openai_messages(checkpoint_df, **kwargs):
    result = []
    pmids = sorted(list(checkpoint_df['pmid'].unique()))
    for pmid in pmids:
        subset = checkpoint_df[checkpoint_df['pmid'] == pmid]
        for max_permutes in [0.6, 3, 10]:
            max_permutes = int(max_permutes * random.uniform(0.5, 2))
            inp, sol = construct_permuted(subset, max_permutes=max_permutes, **kwargs)
            # TODO: try again, but with chain of thought
            sol = f"""```answer
{sol}
```"""
            result.append(to_openai_finetune_request(prompt_collections.backform_eval_v1, [inp], sol))
    return result
    

def trial_run():
    
# test out random_permute
    random.seed(42)
    df_sample_br = pd.DataFrame({'enzyme': ['catalase', 'green enzyme', 'red enzyme', 'blue catalase'], 
                                 'substrate': ['H2O2', 'chlorophyll', 'tomato', 'water'], 
                                 'comments': ['pH: 7.0', 'pH: 8.0', 'pH: 9.0', 'pH: 10.0'],
                                 'km': [1, 2, 3, 4], 
                                 'kcat': [5, 6, 7, 8]})
    df_sample_desc = pd.DataFrame({'descriptor': ['catalase, H2O2', 'green catalase, chlorophyll', 
                                                  'red enzyme, tomato', 'blue catalase, water']})
    # print(sample_df)
    match_info = correct_match_info(df_sample_br)
    
    match_info = random_permute(match_info)
    df_sample_br = df_sample_br.iloc[match_info]
    
    inp, sol = construct_ideal_completion(df_sample_br, df_sample_desc, match_info, deletes_from_descriptor=[3])
    print(inp)
    print(sol)
    exit(0)


if __name__ == "__main__":
    
    # print(df_2)
    # print(match_info)
    
    trial_run()
    
    checkpoint_df = pd.read_csv("backform/checkpoints/rekcat_checkpoint_3 - LATEST_rekcat-vs-brenda_5.csv")
    target_pmids = [10029307, 10190977, 10320327, 10427036, 10433689, 10438489, 10446163, 10473548, 10480865, 
        10480878, 10480915, 10514486, 10529247, 10531334, 10564758, 10684618, 10748206, 10762259, 10801893, 10882170, 
        8626758, 9398292, 9495750, 9521731, 9556600, 9576908, 9628739, 9636048, 9733678, 9933602]
    # target_pmids = ['9696781']
    # suspicious pmids: 10441376, 10714990, 8807052
    target_pmids = [str(x) for x in target_pmids]

    # split into train and test
    random.seed(42)
    random.shuffle(target_pmids)
    train_pmids = target_pmids[:int(0.8 * len(target_pmids))]
    test_pmids = target_pmids[int(0.8 * len(target_pmids)):]

    train_df = extract_supervalid_df(checkpoint_df, train_pmids)
    test_df = extract_supervalid_df(checkpoint_df, test_pmids)
    
    # namespace = '4o_match_eval'
    namespace = '4o_match_eval_v2'
    
    with open(f'backform/finetunes/{namespace}.train.jsonl', 'w') as f:
        for item in to_openai_messages(train_df):
            f.write(json.dumps(item) + '\n')
    with open(f'backform/finetunes/{namespace}.val.jsonl', 'w') as f:
        for item in to_openai_messages(test_df):
            f.write(json.dumps(item) + '\n')
    
    # sanity check: print result for pmid 10190977
    # subset = checkpoint_df[checkpoint_df['pmid'] == '10190977']
    # inp, sol = construct_permuted(subset)
    # print(inp)
    # print(sol)
    
        
    
    # 2, 4, 7 max 

