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
1. ex_step0_run_preprocessing.py:
    - Handles the preprocessing steps (ResNet, Table Extraction)
    - create a `.enzy` folder for simplified file management
2. ex_step1_run_tableboth.py
    - Given PDFs and preprocessed data, feed to LLMs using Batch API.
    - File locations should be automatically saved to `.enzy/llm_log.tsv`.
3. ex_step1b_run_pdf_binaries.py
    - **Alternative** to `step0` and `step1`: feed PDF binaries directly to Claude.
4. ex_step2_download.py
    - Small script to retrieve batches from Batch APIs.
5. ex_step3_llm_to_df.py
    - Convert the LLM output to parquet files.
6. ex_step5_generate_identifiers.py
    - **Optional**: Attach sequence identifiers (EC number, UniProt ID, PDB ID, SMILES, PubChem ID) to the data from `step3`.

## Evaluation

See `experiments/example/evaluation/ex_step*.py`.

1. ex_eval1_compare_dfs.py
    - Evaluate and benchmark LLM data against a trusted dataset.
2. ex_eval2_plot_dfs.py
    - Plot the data from `ex_eval1`.

## Enzyme Accessions

Enzyme accession pipeline is a WIP. Please refer to the following files:

1. `enzyextract.pre.scans.scan_to_parquet` 
    - Scans PDFs, conveniently storing them in text form.
2. `enzyextract.pre.scans.scan_accessions`
    - Scans those text files for enzyme accessions.
3. `enzyextract.pipeline.accessions.acc1_regroup_accessions`
    - Determines which accessions have yet to be processed.
4. `enzyextract.pipeline.accessions.acc2_run_accessions`
    - Queries UniProt/PDB/NCBI databases for enzyme accessions.
5. `enzyextract.pipeline.accessions.acc3_run_uniprot_from_pmid`
    - **Optional**: Queries UniProt database for enzyme accessions, querying based on PMID.
5. `enzyextract.pipeline.accessions.acc4_run_uniprot_searched`
    - **Optional**: Queries UniProt database for enzyme accessions, querying based on enzyme and organism names.
