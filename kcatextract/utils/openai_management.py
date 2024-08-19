import openai

openai_client = None
def read_env_for_openai(filepath):
    with open(filepath, 'r') as f:
        for line in f:
            key, value = line.strip().split('=', 1)
            if key == 'OPENAI_API_KEY':
                openai_client = openai.OpenAI(api_key=value)
                break
        else:
            raise ValueError("No OpenAI key found!")
                
                


def submit_batch_file(filename):
    if openai_client is None:
        # try to create client
        openai_client = openai.OpenAI()
        if openai_client.api_key is None:
            raise ValueError("No OpenAI key found!")
    # read to make sure
    assert filename.endswith('.jsonl')
    with open(filename, 'r') as f:
        count = sum(1 for _ in f)
    print(f"Batch of {count} items at {filename} ready for submission. Submit to OpenAI?")
    if input("Proceed? (y/n): ").lower() == 'y':
        with open(filename, 'rb') as f:
            batch_input_file = openai_client.files.create(
                file=f,
                purpose="batch"
            )
    else:
        print("Aborted.")