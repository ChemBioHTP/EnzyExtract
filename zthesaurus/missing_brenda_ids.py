import polars as pl

data = pl.read_parquet('data/export/TheData.parquet')

interesting_bids = data.filter(
    pl.col('smiles').is_null()
    & (
        pl.col('brenda_id').is_not_null()
        | pl.col('brenda_id_full').is_not_null()
    )
).select(
    'brenda_id',
    'brenda_id_full'
)
# concat brenda_id and brenda_id_full
interesting_bids = pl.concat([
    interesting_bids['brenda_id'],
    interesting_bids['brenda_id_full']
])
interesting_bids = interesting_bids.explode().unique().drop_nulls()
interesting_bids = interesting_bids.to_frame('brenda_id')

brenda = pl.read_parquet('data/substrates/brenda_inchi_all.parquet').filter(
    pl.col('inchi').is_not_null()
    & (pl.col('inchi') != '-')
).select('brenda_id', 'inchi').unique('brenda_id')
result = interesting_bids.join(brenda, on='brenda_id', how='inner')
print(result)



# write results to tmp file
# with open('_debug/tmp_inchi.txt', 'w') as f:
#     f.write('inchi\n')
#     for inchi in result['inchi'].to_list():
#         f.write(inchi + '\n')
# 5398 to 3454


inchi2smiles = pl.read_csv('data/substrates/brenda/pubchem_inchi_to_smiles.tsv', separator='\t')
inchi2smiles = inchi2smiles.unique('inchi')
result = result.join(inchi2smiles, on='inchi', how='inner')

# result.write_parquet('data/substrates/brenda/brenda_inchi_smiles.parquet')
print(result)

successful = result.filter(pl.col('smiles').is_not_null())

result = result.filter(pl.col('smiles').is_null())
print(result) # 1289

import polars as pl
from rdkit import Chem
from tqdm import tqdm
from enzyextract.utils.pl_utils import wrap_pbar

def convert_inchi_to_smiles(df: pl.DataFrame) -> pl.DataFrame:
    """
    Convert InChI strings to SMILES notation in a Polars DataFrame with progress tracking.
    
    Parameters:
    df (polars.DataFrame): DataFrame containing an 'inchi' column
    
    Returns:
    polars.DataFrame: Original DataFrame with an additional 'smiles' column
    """
    def _inchi_to_smiles(inchi: str) -> str | None:
        try:
            mol = Chem.MolFromInchi(inchi)
            if mol is None:
                return None
            return Chem.MolToSmiles(mol)
        except:
            return None
    
    
    # Create progress bar
    with tqdm(total=df.height) as pbar:
        # Add SMILES column to DataFrame with progress tracking
        result = df.with_columns([
            pl.col('inchi')
              .map_elements(wrap_pbar(pbar, _inchi_to_smiles), return_dtype=pl.Utf8)
              .alias('smiles')
        ])
    
    # Calculate and display conversion statistics
    total = df.height
    converted = result.filter(pl.col('smiles').is_not_null()).height
    print(f"Successfully converted {converted}/{total} structures ({(converted/total*100):.1f}%)")
    
    return result

converted = convert_inchi_to_smiles(result)

converted.write_parquet('data/substrates/brenda/rdkit_inchi_to_smiles.parquet')

# now, combine what we know with what we just converted
# select brenda_id, inchi, smiles

good = pl.concat([
    successful.select(['brenda_id', 'inchi', 'smiles']),
    converted.select(['brenda_id', 'inchi', 'smiles'])
])

good.write_parquet('data/substrates/brenda/brenda_inchi_smiles.parquet')
