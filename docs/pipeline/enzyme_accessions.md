## Enzyme Accessions

Enzyme accession pipeline is a WIP. Please refer to the following scripts:

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

Conceptually, after this point, scripts (currently not yet organised into a pipeline) match downloaded accessions and the kinetic data using string similarity and the judgment of LLMs.

Example data for the enzyme accession pipeline has been released:
1. `data/enzymes/sequence_scans/latest_sequence_scans.parquet`
    - Regex search through documents for any possible PDB/UniProt/NCBI id
2. `data/enzymes/accessions/final/{ncbi,pdb,uniprot}.parquet`
    - Downloaded sequences
3. `data/thesaurus/confident/{ncbi,pdb,uniprot}.parquet`
    - Sequences, matched by enzyme name, plus string similarity