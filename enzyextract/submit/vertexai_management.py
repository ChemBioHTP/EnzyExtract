"""
For some reason, Vertex AI does not respect a custom_id and neither do they return the responses in order. 
Thus, the only way to retain the correspondence betweeninput and output is either to embed a custom_id in the prompt or
compare the input described in the output.
"""

import base64
import hashlib
import json
import polars as pl


def generate_corresp(batch_fpath):
    """
    Generate a correspondence file for the batch prediction results.
    """
    collector = []
    with open(batch_fpath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                print(f"Failed to decode JSON: {line}")
                continue
            
            # Extract the custom_id and pmid from the data
            custom_id = data.get("custom_id")
            pmid = custom_id.rsplit('_', 1)[-1]

            all_txt = ''
            for msg in data.get('body', {}).get("messages", []):
                if msg.get("role") == "user":
                    all_txt += msg.get("content", "").strip()
            
            # get sha256 hash of all_txt, to base64
            sha256_hash = base64.b64encode(hashlib.sha256(all_txt.encode()).digest()).decode()
            collector.append({
                "custom_id": custom_id,
                "pmid": pmid,
                "input_sha256": sha256_hash,
                "input_text": all_txt,
            })
    return collector

if __name__ == "__main__":
    pass
    # Example usage
    # batch_fpath = 'C:/conjunct/EnzyExtract/data/rumble/.enzy/batches/rumble_gemini_dev1_2.jsonl'
    # corresp = generate_corresp(batch_fpath)
    # corresp_df = pl.DataFrame(corresp)
    # corresp_df.write_parquet('C:/conjunct/EnzyExtract/data/rumble/.enzy/corresp/rumble_gemini_dev1_2.parquet')