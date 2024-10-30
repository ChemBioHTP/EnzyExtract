import json
import os


compl_folder = 'completions/enzy/apogee'
model = '4os-tuned'

total_prompt_tokens = 0
total_completion_tokens = 0

# total_prompt_tokens = 602652868
# total_completion_tokens = 34420195

for filename in os.listdir(compl_folder):
    if not filename.endswith('_1.jsonl'):
        continue
    with open(f'{compl_folder}/{filename}', 'r') as f:
        for line in f:
            obj = json.loads(line)
            total_prompt_tokens += obj['response']['body']['usage']['prompt_tokens']
            total_completion_tokens += obj['response']['body']['usage']['completion_tokens']
            
print(total_prompt_tokens, total_completion_tokens)

# calculate cost
if model == '4o-mini':
    cost = 0.15/1E6 * total_prompt_tokens + 0.600/1E6 * total_completion_tokens
    print(cost)
elif model == '4os':
    cost = 1.25/1E6 * total_prompt_tokens + 5/1E6 * total_completion_tokens
    print(cost)
elif model == '4os-tuned':
    cost = 1.875/1E6 * total_prompt_tokens + 7.5/1E6 * total_completion_tokens
    print(cost)