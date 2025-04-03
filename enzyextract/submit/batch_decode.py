import json
import polars as pl

outputs_schema = {
    "custom_id": pl.Utf8,
    "content": pl.Utf8,
    "finish_reason": pl.Utf8,
    "input_tokens": pl.Int64,
    "output_tokens": pl.Int64,
}

def stream_jsonl(fpath: str):
    """stream a jsonl file"""
    with open(fpath, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue
            yield obj

def decode_openai_line(obj) -> dict:
    """decode openai batch output (a single line)"""
    custom_id = obj['custom_id']
    content = obj['response']['body']['choices'][0]['message']['content']
    finish_reason = obj['response']['body']['choices'][0]['finish_reason']
    input_tokens = obj['response']['body']['usage']['prompt_tokens']
    output_tokens = obj['response']['body']['usage']['completion_tokens']
    return {
        "custom_id": custom_id,
        "content": content,
        "finish_reason": finish_reason,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }

def decode_openai_batch(objs: list[dict]) -> pl.DataFrame:
    """decode openai batch outputs"""
    decoded = []
    for obj in objs:
        decoded.append(decode_openai_line(obj))
    return pl.DataFrame(decoded, schema_overrides=outputs_schema)

def decode_anthropic_line(obj) -> dict:
    """decode anthropic batch output (a single line)"""
    custom_id = obj['custom_id']
    content = obj['results']['message']['content'][0]['text']
    finish_reason = obj['results']['message']['stop_reason']
    # end turn
    input_tokens = obj['results']['usage']['input_tokens']
    output_tokens = obj['results']['usage']['output_tokens']
    return {
        "custom_id": custom_id,
        "content": content,
        "finish_reason": finish_reason,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }
def decode_anthropic_batch(objs: list[dict]) -> pl.DataFrame:
    """decode anthropic batch outputs (a list of lines)"""
    decoded = []
    for obj in objs:
        decoded.append(decode_anthropic_line(obj))
    return pl.DataFrame(decoded, schema_overrides=outputs_schema)

def decode_vertex_line(obj: dict, custom_id) -> dict:
    """decode vertexAI batch output (a single line)"""
    # TODO: note it appear that vertexai guarantees that the order is same as the input, 
    # so we can use the index to get the custom_id
    # custom_id = None 
    content = obj['response']['candidates'][0]['content']['parts'][0]['text']
    finish_reason = obj['response']['candidates'][0]['finishReason']
    input_tokens = obj['response']['usageMetadata']['promptTokenCount']
    output_tokens = obj['response']['usageMetadata']['candidatesTokenCount']
    return {
        "custom_id": custom_id,
        "content": content,
        "finish_reason": finish_reason,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }

def decode_vertex_batch(objs: list[dict], corresp_df: pl.DataFrame) -> pl.DataFrame:
    """decode vertexAI batch outputs (a list of lines)"""
    decoded = []
    # batch_id in the same order as in corresp_df
    custom_ids = corresp_df['custom_id'].to_list()
    for obj, custom_id in zip(objs, custom_ids):
        decoded.append(decode_vertex_line(obj, custom_id))
    return pl.DataFrame(decoded, schema_overrides=outputs_schema)