"""
Formerly located in enzyextract.utils.construct_batch

"""


import json

import PIL
from PIL import Image

import base64
from io import BytesIO

def image_to_base64(image: PIL.Image.Image) -> str:
    buffered = BytesIO()
    image.save(buffered, format="JPEG")
    return base64.b64encode(buffered.getvalue()).decode('utf-8')

def to_openai_dict_message(role: str, content: str | PIL.Image.Image, detail='auto') -> dict:
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
    if isinstance(content, Image.Image):
        base64_image = image_to_base64(content)
        return { 
            "role": role,
            "content": [ # TODO: with images, the openai API supports a list of content (e.g. text and image)
                {
                    "type": "image_url", 
                    "image_url": {
                        "url": f"data:image/jpeg;base64,{base64_image}",
                        "detail": detail
                    }
                }
            ]
        }
    return {
        "role": role,
        "content": content
    }


def to_openai_batch_request(uuid: str, system_prompt: str, docs: list[str | PIL.Image.Image], model_name='gpt-4o-mini', detail='auto'):  # gpt-4-turbo-2024-04-09
    """
    To get content, do: obj['body']['messages'][x]['content']
    """
    if isinstance(docs, str):
        docs = [docs]
    messages = [to_openai_dict_message("system", system_prompt)] + [to_openai_dict_message("user", doc, detail=detail) for doc in docs]
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

def pmid_from_usual_cid(cid: str) -> str:
    """Assumes f'{namespace}_{version}_{pmid}'"""
    return cid.split('_', 2)[2] # formerly int


def namespace_from_usual_cid(cid: str) -> int:
    """Assumes f'{namespace}_{version}_{pmid}'"""
    return cid.split('_', 2)[0]

def versioned_namespace_from_usual_cid(cid: str) -> tuple[str, int]:
    """Assumes f'{namespace}_{version}_{pmid}'"""
    uscores = cid.count('_')
    if uscores < 2:
        return cid
    namespace, version = cid.split('_', 2)[:2]
    return f'{namespace}_{version}' # allow dois with underscores as pmid
    

def write_to_jsonl(batch, filename):
    with open(filename, 'w') as f:
        for item in batch:
            f.write(json.dumps(item) + '\n')

def chunked_write_to_jsonl(batch, filepath, chunk_size=1000):
    """
    Need to enforce chunk size, since OpenAI has data size limit

    filepath: complete path to a file, INCLUDING {dest_folder}/{namespace}.jsonl
    Returns list of filenames written to, so they can be opened and submitted in sequence
    """
    # chunk_size = 1000
    have_multiple = len(batch) > chunk_size # need to enforce chunk size, since OpenAI has data size limit

    write_dests = []
    for i in range(0, len(batch), chunk_size):
        chunk = batch[i:i+chunk_size]
        if have_multiple:
            if filepath.endswith('.jsonl'):
                will_write_to = f'{filepath[:-6]}.{i}.jsonl'
            else:
                will_write_to = f'{filepath}.{i}.jsonl'
            # will_write_to = f'{dest_folder}/{namespace}_{version}.{i}.jsonl'
        else:
            will_write_to = filepath
        write_to_jsonl(chunk, will_write_to)
        write_dests.append(will_write_to)
    return write_dests



import json
import os


def locate_correct_batch(src_folder, namespace, version=None):
    # filename gets scrambled. luckily, just search for 
    # {"id": "batch_req_cOTEwOobeyeib0QWIhpo7PWQ", "custom_id": 
    
    # prefix = len("""{"id": "batch_req_cOTEwOobeyeib0QWIhpo7PWQ", """) # "custom_id": 
    # they changed the length of the id
    # now, search for ", "custom_id": "
    
    # search by date created, to prefer the newest versions
    # for filename in os.listdir(src_folder):
    candidates = sorted(os.listdir(src_folder), 
                           key=lambda x: os.path.getctime(os.path.join(src_folder, x)),
                           reverse=True)
    for filename in candidates:
        if not filename.endswith('.jsonl'):
            continue
        with open(f'{src_folder}/{filename}', 'r') as f:
            line = f.readline()
            target = f"{namespace}_" # f'"custom_id": "{namespace}_'
            if version is not None:
                target += str(version)
            
            query = '", "custom_id": "'
            if query not in line:
                continue
            prefix = line.index(query) + len('", "custom_id": "')
            
            if line[prefix:].startswith(target):
                if version is None:
                    start_from = prefix + len(target)
                    version = line[start_from:line.index('_', start_from)]
                return filename, version
            else:
                pass
                # print(line[prefix:], target)
    raise FileNotFoundError(f"Could not find {namespace} in {src_folder}")

def decode_custom_id(output_filepath):
    """decode custom_id from output batch file
    assumes there are none of these characters: >"< """
    with open(output_filepath, 'r') as f:
        line = f.readline()
        if line.startswith('{"id": "batch_req_'):
            # output
            prefix = len('''{"id": "batch_req_cOTEwOobeyeib0QWIhpo7PWQ", "custom_id": "''') # "custom_id": 
            return line[prefix:line.index('"', prefix)]
        elif line.startswith('{"custom_id": "'):
            # input
            prefix = len('{"custom_id": "')
            return line[prefix:line.index('"', prefix)]

def preview_batches_in_folder(src_folder, output_folder, undownloaded_only=True, printme=True):
    """preview the batches in a folder
    if undownloaded_only, only print the ones that have not been downloaded"""
    src_to_output = {}
    for filename in os.listdir(src_folder):
        if not filename.endswith('.jsonl'):
            continue
        cid = decode_custom_id(f'{src_folder}/{filename}')
        ns = versioned_namespace_from_usual_cid(cid)
        src_to_output[ns] = None
    
    for filename in os.listdir(output_folder):
        if not filename.endswith('.jsonl'):
            continue
        cid = decode_custom_id(f'{output_folder}/{filename}')
        ns = versioned_namespace_from_usual_cid(cid)
        if ns in src_to_output:
            src_to_output[ns] = filename
        else:
            print(f"Warning: {ns} not found in src_folder")
    
    if undownloaded_only:
        src_to_output = {k: v for k, v in src_to_output.items() if v is None}
    
    if printme:
        for k, v in src_to_output.items():
            print(k, ":", v)
    
    return src_to_output
    
    

def get_batch_output(filename, allow_unfinished=True) -> list[tuple[str, str, str]]:
    # return list of (custom_id, content, finish_reason)
    result = []
    with open(filename, 'r') as f:
        for line in f:
            obj = json.loads(line)
            finish_reason = obj['response']['body']['choices'][0]['finish_reason']
            if not allow_unfinished and finish_reason == 'length':
                continue # skip too long
            result.append((obj['custom_id'], obj['response']['body']['choices'][0]['message']['content'], finish_reason))
    return result


def get_batch_input(filename) -> list[tuple[str, list[str]]]:
    # return list of (custom_id, docs)
    result = []
    with open(filename, 'r') as f:
        for line in f:
            obj = json.loads(line)
            docs = [msg['content'] for msg in obj['body']['messages'][1:]]
            result.append((obj['custom_id'], docs))
    return result

if __name__ == '__main__':
    preview_batches_in_folder('batches/enzy', 'completions/enzy', undownloaded_only=False)