# brenda_kcat_v3.parquet

BRENDA data is obtained from https://www.brenda-enzymes.org/ the `2023.1` json download. 

The approach is:
1. the json is walked for any non-null `turnover_number` or `km_value`, its `ec` and `ref` are recorded. This produces a list of pmids.
2. All `turnover_number`, `km_value`, and `kcat_km` values are then taken from those pmids.
3. DataFrame operations:
    - split BRENDA comments into distinct rows when they refer to separate entries.
    - parse BRENDA comments to obtain pH, temperature, mutant.
    - traverse relational data to obtain organism, uniprot, etc.
    - pivot kcat and Km to be adjacent columns.


`brenda_kcat_v3.parquet` comprehensively has all kcat values from the json dump. However, a there are a few outliers:

## suspect_mutant


Sometimes, BRENDA reports mutants in a way that's hard to tell if it is a double mutant or two individual point mutations. 

For instance, 

> PMID: 9100001 \
> km_value: `0.003` \
> comment: `#5# mutant Q15R and R117N enzymes, double-stranded substrate, containing a site-specific duplex cis-syn cyclobutane pyrimidine dimer <1>`

In the provided example, it is hard to immediately tell if the `0.003` km value refers to the Q15R/R117N double mutant, or if it refers the `Q15R` and `R117N` mutants individually.

For these rows, the `suspect_mutant` flag is set as true (<1% of rows)

## suspect_stranded

In BRENDA, the "reference" is always recorded in the comment as the number between the angle brackets: e.g. `<1>` in the example above.

The number almost always matches the reference id in the BRENDA json. However, rarely (2%), that entry is filed under a different "reference" id (for instance, `brenda['data'][ec]['kcat_value'][i]['references']` != 1), and the data point does not belong to the reported PMID.

For these rows, the `suspect_stranded` flag is set as true (2% of rows)

# brenda_kcat_cleanest.parquet

PMIDs with the `suspect_stranded` flag are excluded, as only the cleanest data is desired for validation.
