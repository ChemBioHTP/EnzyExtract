## Headers

This schema applies to:
- `EnzyExtractDB/EnzyExtractDB_176463.parquet`
- `data/export/TheData_kcat.parquet`
- `data/export/TheData_unpruned.parquet`

General notes:
1. Unstandardized fields come directly from LLMs.
2. **Almost all columns can contain null.** The only non-null columns are "pmid", "canonical", "descriptor", and "meta.doctype".
3. **Warning**: note the warnings on `pmid` and `substrate`.

### Primary Key

- **pmid**: str
    - Nonnull
    - Identifier for the PDF/XML document. 
    - **Warning**: if a PMID is not available, a doi-derived identifier may be used. 
    - **See `canonical`** for a more consistent identifier.


### Unstandardized

- **enzyme**: str
    - Enzyme short name, abbreviation, alias, etc
    - Unstandardized
- **enzyme_full**: str
    - Enzyme full, unabbreviated name
    - Unstandardized
- **substrate**: str
    - Short name, abbreviation, alias, etc for a chemical species.
    - **Warning**: Always corresponds to the Km value, *even if that chemical species is not strictly speaking the substrate*.
    - Unstandardized
- **substrate_full**: str
    - Chemical species full, unabbreviated name
    - Unstandardized
- **mutant**: str
    - Any modifier on the enzyme. 
    - Unstandardized
    - **See `clean_mutant`** for standardized point mutants.
- **organism**: str
    - Organism name
    - Unstandardized
- **kcat**: str
    - $k_{cat}$ value and unit
    - Unstandardized
    - **See `kcat_value`** for standardized kcat.
- **km**: str
    - $Km$ value and unit
    - Unstandardized
    - **See `km_value`** for standardized Km.
- **kcat_km**: str
    - $k_{cat} / K_m$ value and unit
    - Unstandardized
- **temperature**: str
    - Measured temperature
    - Unstandardized
- **pH**: str
    - Measured pH
    - Unstandardized
- **solution**: None
    - Currently unused
- **cofactors**: str
    - Any additional participating ligands or small molecules not directly measured by $K_m$.
    - Not always available.
    - Unstandardized
- **other**: None
    - Currently unused
- **descriptor**: str
    - The direct output from the LLM for that given row.

### Standardized

- **canonical**: str
    - Nonnull
    - Canonical identifier for the PDF/XML document. Should be PMID if available; otherwise DOI.
- **clean_mutant**: list[str]
    - Standardized list of point mutations for that row's enzyme.
    - Each string matches the regex `[A-Z]\d+[A-Z]`
- **cid**: list[int]
    - The PubChem Compound ID(s) corresponding to `substrate` if one (or multiple) exact name match(es) were found.
    - Matched using PubChem .
- **brenda_id**: list[int]
    - The BRENDA ligand ID(s) corresponding to `substrate` if one (or multiple) exact name match(es) were found.
    - Matched using BRENDA 2023.
- **cid_full**: list[int]
    - The PubChem Compound ID(s) corresponding to `substrate_full` if one (or multiple) exact name match(es) were found.
- **brenda_id_full**: list[int]
    - The BRENDA ligand ID(s) corresponding to `substrate` if one (or multiple) exact name match(es) were found.
    - Matched using BRENDA 2023.
- **smiles**: str
    - The SMILES string, converted from the substrate's chemical identifier (cid, brenda_id, cid_full, smiles_full).
    - If there are multiple identifiers, we assume they are synonymous and the first one is arbitrarily taken.
- **enzyme_ecs**: list[str]
    - The EC number, converted from `enzyme` if one (or multiple) exact name match(es) were found in BRENDA.
    - Matched using BRENDA 2023.
    - Each string matches the regex `\d+\.\d+\.\d+\.\d+`
- **enzyme_ecs_full**: list[str]
    - The EC number, converted from `enzyme_full` if one (or multiple) exact name match(es) were found in BRENDA.
    - Matched using BRENDA 2023.
    - Each string matches the regex `\d+\.\d+\.\d+\.\d+`
- **kcat_value**: float
    - The $k_{cat}$ value, in s^-1.
- **km_value**: float | None
    - The $K_m$ value, in mM.
- **sequence**: str
    - The enzyme sequence, corresponding to the best enzyme accession (UniProt, PDB, NCBI)
    - The best accession is determined through a combination of whether the accession was directly mentioned in the paper, and string similarity between LLM and database enzyme name.
- **sequence_source**: str
    - The origin of the sequence.
    - Valid values are: 
        - `uniprot searched`
        - `uniprot cited`
        - `uniprot cited picked`
        - `pdb cited`
        - `pdb cited picked`
        - `ncbi cited`
        - `ncbi cited picked`
    - Papers are screened for UniProt, PDB, and NCBI identifiers. Then, they are matched back to the enzyme using string similarity thresholds and LLMs.
    - `cited` means that the paper directly mentions a database's (UniProt, PDB, NCBI) identifier.
    - `picked` means that though the other DB's entry did not initially match the enzyme name exactly, their equivalence was affirmed by a LLM.
    - `searched` means that a sequence was searched from the enzyme name.
- **uniprot**: str
    - An associated UniProtKB accession
- **ncbi**: str
    - An associated NCBI (RefSeq or Genbank) accession
- **pdb**: str
    - An associated PDB accession

### Health checks

- **max_enzyme_similarity**: float
    - *String similarity* between enzyme name and the enzyme name reported with the UniProt/PDB/NCBI accession id
    - Between 0 and 100
- **max_organism_similarity**: float
    - *Organism similarity* between organism name and the organism name reported with the UniProt/PDB/NCBI accession id
    - Between 0 and 100
- **meta.doctype**: str
    - Origin of the data
    - Valid values:
        - `pdf`
        - `xml`
- **flag.repetitive**: bool
    - If the LLM repeated a value many times in its output
    - Repetition is associated with LLM failure: see "repetition penalty"
- **flag.hallucination**: bool
    - If the LLM reported a value that cannot be found in the text
- **flag.scientific**: bool
    - If the LLM's output contains scientific notation
    - Errors here can cause errors of many orders of magnitude
