import base64
import json
import PIL
import requests
import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

from enzyextract.submit.batch_utils import image_to_base64
from enzyextract.submit.base import LLMCommonBatch, SubmitConsent, do_presubmit

_client = None
def get_client():
    global _client
    if _client is None:
        _client = anthropic.Anthropic()
    return _client

def to_anthropic_batch_request(
    custom_id: str, 
    system_prompt: str, 
    *, 
    docs: list[str | PIL.Image.Image] = [], 
    pdf_fpath: str = None,
    model_name='claude-3-7-sonnet-20250219', 
    # detail='auto',
    ):
    """With the anthropic API, we may directly pass the binary file."""

    user_messages = []

    if pdf_fpath is not None:
        with open(pdf_fpath, 'rb') as f:
            pdf_bytes = f.read()
            pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
        
        user_messages.append({
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": pdf_base64,
                # "detail": detail,
            },
        })
    if isinstance(docs, str):
        raise ValueError("Docs should be a list of strings or images.")
    for doc in docs:
        if isinstance(doc, PIL.Image.Image):
            base64_image = image_to_base64(doc)
            user_messages.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": base64_image,
                    # "detail": detail
                }
            })
        else:
            user_messages.append({
                "type": "text",
                "text": doc,
            })

    if system_prompt is not None:
        _system_prompt = {
            "type": "text",
            "text": system_prompt,
            "cache_control": {"type": "ephemeral"}
        }
    else:
        _system_prompt = None
    return Request(
        custom_id=custom_id,
        params=MessageCreateParamsNonStreaming(
            model=model_name,
            max_tokens=8192,
            system=[_system_prompt],
            messages=[{
                "role": "user",
                "content": user_messages,
            }]
        )
    )

def submit_anthropic_batch_file(reqs: list[Request]):
    """Anthropic's batch API works slightly differently."""


    inp = do_presubmit(
        count=len(reqs),
        submit_suffix="Submit to Anthropic?"
    )
    
    if inp.lower() != SubmitConsent.YES:
        print("Aborted.")
        return None

    client = get_client()
    message_batch = client.messages.batches.create(
        requests=reqs
    )
    batch_id = message_batch.id 
    # message_batch.processing_status # in_progress, canceling, ended
    return batch_id

def retrieve_anthropic_batch(batch_id: str):
    """Retrieve anthropic batch."""
    client = get_client()
    message_batch = client.messages.batches.retrieve(
        batch_id,
    )
    status = message_batch.processing_status
    # map
    if status == 'in_progress':
        pass
    elif status == 'canceling':
        status = 'cancelling'
    elif status == 'ended':
        status = 'completed'
    else:
        raise ValueError(f"Unknown status: {status}")
    return LLMCommonBatch(
        _underlying=message_batch,
        status=status,
        output_file_id=message_batch.results_url,
        error_file_id=None,
        endpoint='anthropic_custom'
    )

class ResponseLike:
    """A class that mimics the response object of the anthropic API."""
    def __init__(self, content: str):
        self.content = content


def retrieve_anthropic_results(batch_id: str, file_id: str, to_json_response: bool = False):
    """Retrieve anthropic batch results. Assumes that they are ready.
    
    Args:
        batch_id (str): The ID of the batch to retrieve.
        to_json_response (bool): If True, access through return_value.content
    """
    client = get_client()

    # collector = []
    # for result in client.messages.batches.results(
    #     batch_id,
    # ):
    #     collector.append(result)
    
    # if to_json_response:
    #     bdr = ''
    #     for result in collector:
    #         bdr += json.dumps(result) + '\n'
    #     return ResponseLike(bdr)
    # else:
    #     return collector

    headers = {
        "x-api-key": client.api_key,
        "anthropic-version": "2023-06-01"
    }

    response = requests.get(file_id, headers=headers)

    if response.status_code != 200:
        raise ValueError(f"Failed to retrieve results: {response.status_code}, {response.text}")

    if to_json_response:
        return response
    else:
        return response.json()
    
    


    