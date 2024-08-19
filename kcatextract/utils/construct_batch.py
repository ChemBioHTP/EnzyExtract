import json


def to_openai_dict_message(role: str, content: str) -> dict:
    # if isinstance(lc_msg, SystemMessage):
        # role = "system"
    # elif isinstance(lc_msg, HumanMessage):
        # role = "user"
    # elif isinstance(lc_msg, AIMessage):
        # role = "assistant"
    # else:
        # raise ValueError(f"Unknown message type: {type(lc_msg)}")
    if role not in ["system", "user", "assistant"]:
        raise ValueError(f"Unknown role: {role}")
    return {
        "role": role,
        "content": content
    }


def to_openai_batch_request(uuid: str, system_prompt: str, docs: list[str], model_name='gpt-4o-mini'):  # gpt-4-turbo-2024-04-09
    if isinstance(docs, str):
        docs = [docs]
    messages = [to_openai_dict_message("system", system_prompt)] + [to_openai_dict_message("user", doc) for doc in docs]
    return {
        "custom_id": uuid,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            # This is what you would have in your Chat Completions API call
            "model": model_name,
            "temperature": 0,
            "messages": messages,
        }
    }

def pmid_from_usual_cid(uuid: str) -> int:
    """Assumes f'{namespace}_{version}_{pmid}'"""
    return int(uuid.split('_', 2)[2])
    

def write_to_jsonl(batch, filename):
    with open(filename, 'w') as f:
        for item in batch:
            f.write(json.dumps(item) + '\n')
            



import json
import os


def locate_correct_batch(src_folder, namespace, version=None):
    # filename gets scrambled. luckily, just search for 
    # {"id": "batch_req_cOTEwOobeyeib0QWIhpo7PWQ", "custom_id": 
    prefix = len("""{"id": "batch_req_cOTEwOobeyeib0QWIhpo7PWQ", """) # "custom_id": 
    
    # search by date created, to prefer the newest versions
    # for filename in os.listdir(src_folder):
    for filename in sorted(os.listdir(src_folder), 
                           key=lambda x: os.path.getctime(os.path.join(src_folder, x)),
                           reverse=True):
        if not filename.endswith('.jsonl'):
            continue
        with open(f'{src_folder}/{filename}', 'r') as f:
            line = f.readline()
            target = f'"custom_id": "{namespace}_'
            if version is not None:
                target += str(version)
                
            if line[prefix:].startswith(target):
                if version is None:
                    start_from = prefix + len(target)
                    version = line[start_from:line.index('_', start_from)]
                return filename, version
            else:
                pass
                # print(line[prefix:], target)
    raise FileNotFoundError(f"Could not find {namespace} in {src_folder}")

def get_resultant_content(filename) -> list[tuple[str, str]]:
    # return list of (custom_id, content, finish_reason)
    result = []
    with open(filename, 'r') as f:
        for line in f:
            obj = json.loads(line)
            finish_reason = obj['response']['body']['choices'][0]['finish_reason']
            result.append((obj['custom_id'], obj['response']['body']['choices'][0]['message']['content'], finish_reason))
    return result


