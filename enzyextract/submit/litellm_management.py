import litellm
import os
from enzyextract.submit.base import SubmitConsent, do_presubmit
from dotenv import load_dotenv

def process_env(filepath):
    load_dotenv(filepath)

async def submit_litellm_batch_file(filepath, pending_file=None, custom_llm_provider='openai'):
    """
    Submit a batch file using LiteLLM.
    https://docs.litellm.ai/docs/batches

    Returns a tuple: (
        file_uuid: str,
        batch_uuid: str,
    )
    """


    
    # print("BUCKET NAME:", os.environ['GCS_BUCKET_NAME'])

    with open(filepath, 'rb') as f:
        batch_input_file = await litellm.acreate_file(
            file=f,
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
            "filepath": filepath
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
