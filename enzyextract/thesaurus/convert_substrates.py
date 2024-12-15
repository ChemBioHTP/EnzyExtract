import polars as pl
import os
from datetime import datetime
from enzyextract.utils.fragment_utils import needs_rebuild, latest_parquet

def rebuild_smiles_df(fragments_folder='fetch_sequences/results/smiles_fragments') -> pl.DataFrame:
    """
    Given many fragments, create a single dataframe with all the smiles
    
    Note: if a fragment is not found, but later it is found, 
    then unsuccessful findings will be removed
    for conveniene
    """
    # load every csv from data/smiles_fragments/*.tsv

    so = {'Name': pl.Utf8, 'Smiles': pl.Utf8}
    result = []
    for filename in os.listdir(fragments_folder):
        if filename.endswith('.tsv'):
            df = pl.read_csv(f'{fragments_folder}/{filename}', separator='\t', schema_overrides=so)
        elif filename.endswith('.csv'):
            df = pl.read_csv(f'{fragments_folder}/{filename}', schema_overrides=so)
        elif filename.endswith('.parquet'):
            df = pl.read_parquet(f'{fragments_folder}/{filename}')
        else:
            continue
            
        if 'src' in df.columns:
            pass
        elif filename.startswith('pubchem'):
            df = df.with_columns(
                pl.lit('pubchem').alias('src')
            )
        elif filename.startswith('rdkit'):
            df = df.with_columns(
                pl.lit('rdkit').alias('src')
            )
        else:
            df = df.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias('src')
            )
        # if 'InChI' not in df.columns:
            # df['InChI'] = None
        result.append(
            df.rename(
                {'Name': 'name', 'Smiles': 'smiles'},
                strict=False
            ).select(['name', 'smiles', 'src'])
        )
    # so2 = {'src': pl.Categorical}

    out = pl.concat(result) # type: pl.DataFrame
    # remove all where the Smiles is "not found"
    # unqueryable = out[out['Smiles'] == 'not found']['Name'].unique()
    # out = out[out['Smiles'] != 'not found'].drop_duplicates()
    # unqueryable = set(unqueryable) - set(out['Name'].unique())
    # out['lower_name'] = out['Name'].str.lower()
    # remove those in unqueryable which are also in out (AKA they were eventually found)
    not_found = out.filter(
        pl.col('smiles') == 'not found'
    )['name'].unique()
    found = out.filter(
        pl.col('smiles') != 'not found'
    )['name'].unique()
    # redundant: remove those in unqueryable which are also in out (AKA they were eventually found)

    unqueryable = set(not_found) - set(found)
    out = out.filter(
        (pl.col('smiles') != 'not found') # we want valid smiles
        | (pl.col('name').is_in(unqueryable)) # or we want those where we genuinely cannot find a smiles
    ) 
    # this excludes those that report Name=?? and Smiles=not found, but where later we find Name=?? and Smiles=valid

    # then the unqueryable set can be recovered by out.filter(pl.col('Smiles') == 'not found')['Name'].unique()
    return out

def latest_smiles_df(latest_folder='data/substrates/smiles', fragments_folder='fetch_sequences/results/smiles_fragments') -> pl.DataFrame:
    """
    Fetches the latest smiles with all known Name-to-SMILES mappings

    If any new fragments are found, rebuilds the smiles dataframe from the latest fragments, if necessary.
    Otherwise, the latest smiles dataframe is simply returned.

    Args:
        latest_folder: the folder where the latest parquet files are stored
        fragments_folder: the folder where the fragments are stored
    
    Returns:
        a polars.DataFrame with columns ['name', 'smiles']
    """
    latest, latest_at = latest_parquet(latest_folder)
    if needs_rebuild(latest_at, fragments_folder):
        print("Rebuilding smiles dataframe")
        df = rebuild_smiles_df(fragments_folder)
        # write to latest folder
        now = datetime.now().strftime("%Y%m%d-%H%M%S")
        df.write_parquet(f'{latest_folder}/latest_{now}.parquet')
    else:
        df = pl.read_parquet(latest)
    return df


def rebuild_inchi_df(fragments_folder='fetch_sequences/results/inchi_fragments') -> pl.DataFrame:
    
    result = []
    so = {'Name': pl.Utf8, 'InChI': pl.Utf8, 'Smiles': pl.Utf8}

    renames = {
        'Name': 'name',
        'InChI': 'inchi',
        'Smiles': 'smiles'
    }
    for filename in os.listdir(fragments_folder):
        if filename.endswith('.tsv'):
            df = pl.read_csv(f'{fragments_folder}/{filename}', separator='\t', schema_overrides=so)
        elif filename.endswith('.csv'):
            df = pl.read_csv(f'{fragments_folder}/{filename}', schema_overrides=so)
        else:
            continue
        assert 'InChI' in df.columns, "Creating InChI dataframe requires InChI column"
        result.append(df.rename(renames, strict=False)[['name', 'inchi', 'smiles']])
    return pl.concat(result) # assume all InChIs are valid, barring some unusual circumstance

def latest_inchi_df(latest_folder='data/substrates/inchi', fragments_folder='fetch_sequences/results/inchi_fragments') -> pl.DataFrame:
    """
    Fetches the latest InChIs with all known Name-to-InChI mappings

    If any new fragments are found, rebuilds the InChI dataframe from the latest fragments, if necessary.
    Otherwise, the latest InChI dataframe is simply returned.

    Args:
        latest_folder: the folder where the latest parquet files are stored
        fragments_folder: the folder where the fragments are stored
    
    Returns:
        a polars.DataFrame with columns ['name', 'inchi', 'smiles']
    """
    latest, latest_at = latest_parquet(latest_folder)
    if needs_rebuild(latest_at, fragments_folder):
        print("Rebuilding InChI dataframe")
        df = rebuild_inchi_df(fragments_folder)
        # write to latest folder
        now = datetime.now().strftime("%Y%m%d-%H%M%S")
        df.write_parquet(f'{latest_folder}/latest_{now}.parquet')
    else:
        df = pl.read_parquet(latest)
    return df