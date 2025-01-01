
from enzyextract.submit.batch_utils import to_openai_batch_request
from enzyextract.submit.openai_management import process_env, submit_batch_file
from enzyextract.prompts import for_vision
from PIL import Image
import PIL
process_env('.env')


pmpt = for_vision
img = Image.open(r"C:\conjunct\tmp\eval\vision/10.1002_adsc.200505044_0 orig.png")
req = to_openai_batch_request("N/A", pmpt, [img, 
    ], 'gpt-4o-mini',
    detail='low')
from openai import OpenAI
client = OpenAI()

response = client.chat.completions.create(
  model="gpt-4o-2024-08-06",
  messages=req['body']['messages'],
  response_format={
    "type": "text"
  },
  temperature=0,
#   max_completion_tokens=2048,
#   top_p=1,
#   frequency_penalty=0,
#   presence_penalty=0,
  
)
print(response)

# 4o-mini: succeeds: copmletions: 168, prompt_tokens=343, total_tokens=511
# 'The units in the table are as follows:\n\n- \\( K_m \\) (NADPH) and \\( K_m \\) (NADH) are in micromolar (µM).\n- \\( k_{cat} \\) is in min\\(^{-1}\\).\n\nThe values along the diagonal are:\n\n- WT-reductase: \\( K_m(NADPH) = 2.5 \\, \\mu M \\)\n- W1046A: \\( k_{cat} = 524.5 \\, \\text{min}^{-1} \\)\n- W1046S: \\( K_m(NADH) = 4.4 \\, \\mu M \\)\n- R966D: \\( k_{cat} = 5410 \\, \\text{min}^{-1} \\)'

# gpt-4o-2024-08-06 completion_tokens=184, prompt_tokens=343, total_tokens=527
# 'The units in the table are micromolar (µM) for \\(K_M\\) and min\\(^{-1}\\) for \\(k_{cat}\\).\n\nThe values along the diagonal are:\n- \\(K_M(\\text{NADPH})\\) for W1046A: \\(0.82 \\pm 0.08\\) µM\n- \\(k_{cat}(\\text{NADPH})\\) for W1046S: \\(366 \\pm 41\\) min\\(^{-1}\\)\n- \\(K_M(\\text{NADH})\\) for R966D, W1046S: \\(11.6 \\pm 0.7\\) µM\n- \\(k_{cat}(\\text{NADH})\\) for WT-reductase: \\(2810 \\pm 100\\) min\\(^{-1}\\)'
# I guess it works alright

# vision: 
# with 4o: 0.000213 per image
# text: 168*(10/1E6) + 343*(2.5/1E6) = 0.0025375
# with mini: 0.000425 per image + 
# text: 168*(0.150/1E6) + 343*(0.6/1E6) = 0.000231

# You will be given a table and extracted data. Follow these steps:
# 1. Identify the units of the table
# 2. Identify any columns which use scientific notation.
# 3. Identify and correct all errors in the extracted data. 
# 4. If there are no mistakes, then respond "All correct."
