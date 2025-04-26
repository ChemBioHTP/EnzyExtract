# EnzyExtract

Extract kinetics data from PDFs using LLMs.

# Installation

```bash
git clone https://github.com/ChemBioHTP/EnzyExtract
cd EnzyExtract
pip install -e .
```

Furthermore, create a `.env` file in the project root. Add your OpenAI API key:
```bash
OPENAI_API_KEY=...
```

If you want to run anthropic or vertex AI models, you might need further API keys.
```bash
ANTHROPIC_API_KEY=...
GCS_BUCKET_NAME=...
GCS_PATH_SERVICE_ACCOUNT=...
GOOGLE_APPLICATION_CREDENTIALS=...
VERTEXAI_LOCATION=...
VERTEXAI_PROJECT=...
```

# Usage

See `experiments/example/pipeline/ex_step*.py` for example scripts. The scripts should be run sequentially, though file paths may need to be adjusted.

Steps:
1. step0_run_preprocessing.py:
    - Handles the preprocessing steps (ResNet, Table Extraction)
    - create a `.enzy` folder for simplified file management
2. step1_run_tableboth.py
    - Given PDFs and preprocessed data, feed to LLMs using Batch API.
    - File locations should be automatically saved to `.enzy/llm_log.tsv`.
3. step1b_run_pdf_binaries.py
    - **Alternative** to `step0` and `step1`: feed PDF binaries directly to Claude.
4. step2_download.py
    - Small script to retrieve batches from Batch APIs.
5. step3_llm_to_df.py
    - Convert the LLM output to parquet files.
6. step4_generate_identifiers.py
    - **Optional**: Attach sequence identifiers (EC number, UniProt ID, PDB ID, SMILES, PubChem ID) to the data from `step3`.
7. step5_compare_dfs.py
    - Evaluate and benchmark LLM data against a trusted dataset.
8. step6_plot_dfs.py
    - Plot the data from `step5`.
