import json
import os
from pathlib import Path
import time
import polars as pl
from openai import BadRequestError, AuthenticationError
from enzyextract.submit.openai_management import get_openai_client, process_env
from enzyextract.pipeline.llm_log import read_log, write_log, llm_log_schema


def _chat_completion_create(client, body: dict):
    kwargs = {
        "model": body.get("model"),
        "messages": body.get("messages", []),
    }
    max_tokens = body.get("max_tokens")
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    try:
        return client.chat.completions.create(**kwargs)
    except BadRequestError as exc:
        if "max_tokens" not in str(exc):
            raise
        retry_kwargs = dict(kwargs)
        retry_kwargs.pop("max_tokens", None)
        if max_tokens is not None:
            retry_kwargs["max_completion_tokens"] = max_tokens
        return client.chat.completions.create(**retry_kwargs)


def _messages_to_responses_args(body: dict) -> dict:
    messages = body.get("messages", [])
    instructions = "\n\n".join(
        str(message.get("content", ""))
        for message in messages
        if message.get("role") == "system" and message.get("content")
    )
    input_messages = [
        {"role": message.get("role", "user"), "content": str(message.get("content", ""))}
        for message in messages
        if message.get("role") != "system"
    ]
    kwargs = {
        "model": body.get("model"),
        "input": input_messages or "",
    }
    if instructions:
        kwargs["instructions"] = instructions
    max_tokens = body.get("max_tokens")
    if max_tokens is not None:
        kwargs["max_output_tokens"] = max(16, int(max_tokens))
    return kwargs


def _responses_create_as_chat_completion(client, body: dict):
    response = client.responses.create(**_messages_to_responses_args(body))
    usage = getattr(response, "usage", None)
    input_tokens = getattr(usage, "input_tokens", 0) if usage else 0
    output_tokens = getattr(usage, "output_tokens", 0) if usage else 0
    return {
        "id": response.id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": response.model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": getattr(response, "output_text", ""),
                },
                "logprobs": None,
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
        "system_fingerprint": None,
    }


def _create_completion_body(client, body: dict) -> dict:
    try:
        response = _chat_completion_create(client, body)
    except AuthenticationError as exc:
        if "insufficient permissions" not in str(exc).lower():
            raise
        return _responses_create_as_chat_completion(client, body)
    return {
        "id": response.id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": response.model,
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": choice.message.role,
                    "content": choice.message.content
                },
                "logprobs": None,
                "finish_reason": choice.finish_reason
            }
            for choice in response.choices
        ],
        "usage": {
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens
        },
        "system_fingerprint": response.system_fingerprint
    }


def process_batch_synchronously(batch_fpath: str, enzy_root: str) -> str:
    """
    Process a batch of OpenAI API requests synchronously and write responses to completion file.
    
    Args:
        batch_fpath: Path to the batch file containing requests
        enzy_root: Path to the .enzy folder
        
    Returns:
        str: Path to the completion file where responses are written
    """
    client = get_openai_client()
    
    # Convert batch_fpath to completion_fpath
    batch_path = Path(batch_fpath)
    completion_fpath = str(Path(enzy_root) / "completions" / batch_path.name)
    
    # Read batch requests
    requests = []
    with open(batch_fpath, 'r') as f:
        for line in f:
            if line.strip():
                requests.append(json.loads(line))
    
    # Process requests synchronously
    responses = []
    for req in requests:
        try:
            # Extract request details
            custom_id = req.get('custom_id')
            body = req.get('body', {})
            
            response_body = _create_completion_body(client, body)
            
            # Format response
            formatted_response = {
                "id": f"batch_req_{int(time.time() * 1000)}",
                "custom_id": custom_id,
                "response": {
                    "status_code": 200,
                    "request_id": f"req_{int(time.time() * 1000)}",
                    "body": response_body,
                    "error": None
                }
            }
            responses.append(formatted_response)
            
        except Exception as e:
            # Handle errors
            error_response = {
                "id": f"batch_req_{int(time.time() * 1000)}",
                "custom_id": custom_id,
                "response": {
                    "status_code": int(getattr(e, "status_code", 500)),
                    "request_id": f"req_{int(time.time() * 1000)}",
                    "body": None,
                    "error": str(e)
                }
            }
            responses.append(error_response)
    
    # Write responses to completion file
    os.makedirs(os.path.dirname(completion_fpath), exist_ok=True)
    with open(completion_fpath, 'w') as f:
        for response in responses:
            f.write(json.dumps(response) + '\n')
    print("Wrote to", completion_fpath)
    
    # Update llm_log.tsv
    log_path = os.path.join(enzy_root, "llm_log.tsv")
    df = read_log(log_path)
    
    # Find the namespace and version from the batch_fpath
    correct_row = df.filter(
        pl.col('batch_fpath') == batch_fpath
    )
    if correct_row.height == 0:
        print("No matching row found in the log for the given batch_fpath.")
    else:
        namespace = correct_row.item(0, 'namespace')
        version = correct_row.item(0, 'version')
        shard = correct_row.item(0, 'shard')
        
        # Create update DataFrame
        updates_df = pl.DataFrame({
            'namespace': [namespace],
            'version': [version],
            'shard': [shard],
            'completion_fpath': [completion_fpath],
            'status': ['downloaded']
        }, schema_overrides=llm_log_schema)
        
        # Update the log file
        df = df.update(updates_df, on=['namespace', 'version'], how='left')
        write_log(df, log_path)
        print("Updated", log_path)
    
    return completion_fpath

if __name__ == "__main__":
    # Example usage
    process_env(".env")

    batch_fpath = "path/to/your/batch.jsonl"
    enzy_root = "path/to/.enzy"
    completion_fpath = process_batch_synchronously(batch_fpath, enzy_root)
    print(f"Responses written to: {completion_fpath}")
