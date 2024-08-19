
from typing import Callable


def to_openai_dict_message(role: str, content: str) -> dict:
    """bit of duplicate code, see also construct_batch.py"""
    if role not in ["system", "user", "assistant"]:
        raise ValueError(f"Unknown role: {role}")
    return {
        "role": role,
        "content": content
    }

def to_openai_finetune_request(system_prompt: str, docs: list[str], model_answer: str): 
    user_messages = [to_openai_dict_message("user", doc) for doc in docs]
    return {
        "messages": [
            to_openai_dict_message("system", system_prompt),
            *user_messages,
            to_openai_dict_message("assistant", model_answer)
        ]
    }

def openai_batch_to_finetune(batch_input: dict, batch_response: dict, system_prompt=None):
    """Converts a model (presumably perfect) batch input and batch output and turns it into the finetune format"""
        
    
    # sample batch response: {"id": "batch_req_2YAn1dmdzPuKil0ylqAZxffF", "custom_id": "brenda-rekcat-md-v1-2_1_10026218", "response": {"status_code": 200, "request_id": "6a701c65512bc41b5717de0a3fb16fe1", "body": {"id": "chatcmpl-9nvJrspxr1zMo4hP1Phu0v0cBI0Uw", "object": "chat.completion", "created": 1721685435, "model": "gpt-4o-2024-05-13", "choices": [{"index": 0, "message": {"role": "assistant", "content": "Thoughts and comments:\n- The table contains data for two enzymes: Acylated GPI-PLC and De-acylated GPI-PLC.\n- The Km values are given in mM, and the kcat values are given in min^-1.\n- There are two experimental conditions (Exp. 1 and Exp. 2) for each enzyme.\n- The table does not specify additional conditions like temperature, pH, or organism.\n\nFinal answer:\n```yaml\ndata:\n    - descriptor: \"Acylated GPI-PLC, Exp. 1\"\n      kcat: \"1188 min^-1\"\n      Km: \"2.7 mM\"\n      kcat/Km: null\n    - descriptor: \"Acylated GPI-PLC, Exp. 2\"\n      kcat: \"1636 min^-1\"\n      Km: \"2.6 mM\"\n      kcat/Km: null\n    - descriptor: \"De-acylated GPI-PLC, Exp. 1\"\n      kcat: \"39 min^-1\"\n      Km: \"1.7 mM\"\n      kcat/Km: null\n    - descriptor: \"De-acylated GPI-PLC, Exp. 2\"\n      kcat: \"89 min^-1\"\n      Km: \"2.0 mM\"\n      kcat/Km: null\ncontext:\n    enzymes: \"Acylated GPI-PLC, De-acylated GPI-PLC\"\n    substrates: null\n    mutants: null\n    organisms: null\n    temperatures: null\n    pHs: null\n    solvents: null\n    other: \"Exp. 1, Exp. 2\"\n```"}, "logprobs": null, "finish_reason": "stop"}], "usage": {"prompt_tokens": 771, "completion_tokens": 356, "total_tokens": 1127}, "system_fingerprint": "fp_18cc0f1fa0"}}, "error": null}
    
    model_answer = batch_response['response']['body']['choices'][0]['message']['content']
    if batch_response['response']['body']['choices'][0]['finish_reason'] != 'stop':
        raise ValueError("Expected completion to finish")
    
    return openai_crafted_batch_to_finetune(batch_input, model_answer, system_prompt)

def openai_crafted_batch_to_finetune(batch_input: dict, model_answer: str, system_prompt=None):
    """Given an input batch and a perfect response, turns it into the finetune format"""
    messages = batch_input['body']['messages']
    if messages[0]['role'] != 'system':
        raise ValueError("Expected first message to be system prompt")
    if system_prompt is None:
        system_prompt = messages[0]['content']
    input_messages = messages[1:]
    
    return to_openai_finetune_request(system_prompt, [msg['content'] for msg in input_messages], model_answer)

def extract_supervalid_df(larger_df, pmids):
    df = larger_df[larger_df['pmid'].isin(pmids)]
    # make sure that all of these are present: enzyme, substrate, enzyme_2, substrate_2
    df = df.dropna(subset=['enzyme', 'substrate', 'enzyme_2', 'substrate_2'])
    # also make sure that either km or kcat is present
    df = df.dropna(subset=['km', 'kcat'], how='all')
    df = df.dropna(subset=['km_2', 'kcat_2'], how='all')
    return df


def split_checkpoint(checkpoint_df):
    # randomly permutes df_2, then returns what was done to df_2
    df_br = checkpoint_df.copy()
    df_desc = df_br[['enzyme', 'substrate', 'descriptor', 'km', 'kcat']].copy()
    

    # rename descriptor_2 and variant_2 to comments_2, if they exist    
    df_br.rename(columns={'descriptor_2': 'comments_2', 'variant_2': 'comments_2'}, inplace=True, errors='ignore')
    df_br = df_br[['enzyme_2', 'substrate_2', 'comments_2', 'km_2', 'kcat_2']].copy()
    df_br.columns = ['enzyme', 'substrate', 'comments', 'km', 'kcat']
    return df_br, df_desc


def isolate_the_yaml(ai_msg: str):
    if '```yaml\n' not in ai_msg:
        return None, None, ai_msg
    pre_yaml, yaml_block = ai_msg.split('```yaml\n', 1)
    if '```' not in yaml_block:
        return None, None, ai_msg
    yaml_block, post_yaml = yaml_block.split('```', 1)
    return pre_yaml, yaml_block, post_yaml

def fix_the_yaml(ai_msg: str, lambda_for_yaml: Callable[[str], str]) -> str:
    result = ""
    
    post_yaml = ai_msg
    while True:
        pre_yaml, yaml_block, post_yaml = isolate_the_yaml(post_yaml)
        
        if pre_yaml is None:  # No more YAML blocks found
            result += post_yaml
            break
        
        result += pre_yaml
        fixed_yaml = lambda_for_yaml(yaml_block)
        result += f"```yaml\n{fixed_yaml}```"
    
    return result
