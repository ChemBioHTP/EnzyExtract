import base64
import hashlib
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
    content = obj['result']['message']['content'][0]['text']
    finish_reason = obj['result']['message']['stop_reason']
    # end turn
    input_tokens = obj['result']['message']['usage']['input_tokens']
    output_tokens = obj['result']['message']['usage']['output_tokens']
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

def decode_vertex_line(obj: dict) -> dict:
    """decode vertexAI batch output (a single line)"""
    # TODO: note it appear that vertexai guarantees that the order is same as the input, 
    # so we can use the index to get the custom_id
    # custom_id = None 
    content = obj['response']['candidates'][0]['content']['parts'][0]['text']
    finish_reason = obj['response']['candidates'][0]['finishReason']
    input_tokens = obj['response']['usageMetadata']['promptTokenCount']
    output_tokens = obj['response']['usageMetadata']['candidatesTokenCount']

    # translate finish reason https://cloud.google.com/vertex-ai/generative-ai/docs/reference/python/latest/vertexai.generative_models.FinishReason
    if finish_reason == 'STOP':
        finish_reason = 'stop'
    # elif finish_reason == 'END_OF_TEXT':
        # finish_reason = 'length'
    elif finish_reason == 'MAX_TOKENS':
        finish_reason = 'length'
    else:
        print("Unknown finish reason:", finish_reason)
    return {
        # "custom_id": custom_id,
        "content": content,
        "finish_reason": finish_reason,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }

def decode_vertex_batch(objs: list[dict]) -> pl.DataFrame:
    """decode vertexAI batch outputs (a list of lines).
    
    VertexAI is actually a pain because it doesn't guarantee the order of the output AND custom_id aren't respected either.
    So we need to use the corresp_df to map the output to the input using file hashes
    """
    decoded = []
    
    # custom_ids = corresp_df['custom_id'].to_list()
    # batch_id is NOT in the same order as in corresp_df
    # for obj, custom_id in zip(objs, custom_ids):
        # decoded.append(decode_vertex_line(obj, custom_id))
    for i, obj in enumerate(objs):
        all_txt = ''
        for msg in obj['request']['contents']:
            if msg.get("role") == "user":
                for part in msg['parts']:
                    all_txt += part['text'].strip()
        sha256_hash = base64.b64encode(hashlib.sha256(all_txt.encode()).digest()).decode()
        decoded.append({
            **decode_vertex_line(obj),
            'input_sha256': sha256_hash,
            'all_txt': all_txt,
        })
        
    return pl.DataFrame(decoded, schema_overrides=outputs_schema)

def jsonl_to_decoded_df(fpath: str, llm_provider: str, corresp_df: pl.DataFrame) -> pl.DataFrame:
    if fpath is None:
        raise ValueError("File path is None (batch is not ready?)")
    source = stream_jsonl(fpath)
    if llm_provider == 'openai':
        df = decode_openai_batch(source)
        if corresp_df is not None:
            df = df.join(corresp_df, on='custom_id', how='left')
        return df
    elif llm_provider == 'anthropic':
        df = decode_anthropic_batch(source)
        if corresp_df is not None:
            df = df.join(corresp_df, on='custom_id', how='left')
        return df
    elif llm_provider == 'vertex_ai':
        df = decode_vertex_batch(source, corresp_df)
        if corresp_df is None:
            raise ValueError("corresp_df is None, but vertexAI requires it")
        df = df.join(corresp_df, on='input_sha256', how='full')

        strange = df.filter(pl.col('input_sha256').is_null() | pl.col('input_sha256_right').is_null())
        # Unfortunately, hashes may be different due to vertex ai's replacement of
        # utf8 characters with U+FFFD >:(
        left = strange.filter(pl.col('all_txt').is_not_null()).item(0, 'all_txt') # from vertex ai
        right = strange.filter(pl.col('input_text').is_not_null()).item(0, 'input_text') # from original (corresp_df)
        if left != right:
            print("Strange: ", left, right)
        return df
    else:
        raise ValueError(f"Unknown LLM provider: {llm_provider}")