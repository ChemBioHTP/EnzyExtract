import litellm
import os
from enzyextract.submit.base import SubmitPreference, do_presubmit
from dotenv import load_dotenv
from typing import Union, Optional, Tuple, List, Dict
from io import BytesIO
import json

def process_env(filepath):
    load_dotenv(filepath)

async def submit_litellm_batch_file(
    filepath: Union[str, bytes, List[Dict]],
    pending_file: Optional[str] = None,
    custom_llm_provider: str = 'openai'
) -> Tuple[str, str]:
    """
    Submit a batch file using LiteLLM.
    https://docs.litellm.ai/docs/batches

    Args:
        filepath: Either a filepath (str), bytes-like object, or list of dictionaries containing the file content
        pending_file: Optional file to track pending submissions
        custom_llm_provider: LLM provider to use (default: 'openai')

    Returns a tuple: (
        file_uuid: str,
        batch_uuid: str,
    )
    """
    # Handle different input types
    if isinstance(filepath, (str, bytes)):
        if isinstance(filepath, str):
            # If it's a filepath, open and read the file
            with open(filepath, 'rb') as f:
                file_content = f
        else:
            # If it's bytes-like, create a BytesIO object
            file_content = BytesIO(filepath)
    elif isinstance(filepath, list) and all(isinstance(item, dict) for item in filepath):
        # If it's a list of dicts, JSON encode it to bytes
        json_bytes = json.dumps(filepath).encode('utf-8')
        file_content = BytesIO(json_bytes)
    else:
        raise ValueError("filepath must be either a filepath (str), bytes-like object, or list of dictionaries")

    batch_input_file = await litellm.acreate_file(
        file=file_content,
        purpose="batch",
        custom_llm_provider=custom_llm_provider,
    )
    batch_input_file_id = batch_input_file.id

    batch_confirm = await litellm.acreate_batch(
        input_file_id=batch_input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        custom_llm_provider=custom_llm_provider,
        metadata={
            "filepath": filepath if isinstance(filepath, str) else "bytes_input"
        }
    )
    
    # get the id
    batch_id = batch_confirm.id
    print("Submitted as batch", batch_id)
    
    return batch_input_file_id, batch_id

    # if pending_file is not None:
    #     with open(pending_file, 'a') as f:
    #         f.write(json.dumps({'input': filepath, 'output': batch_id}) + '\n')
    
    # return batch_id
    
    # client.batches.list(limit=10)
    
    # check status: 
    # batch_status = client.batches.retrieve(batch_id)
