# some utils after matching dfs

import pandas as pd


def left_shift_pmid(df):
    # the dumb left shift pmid obligation
    df['pmid'] = df['pmid'].replace('', pd.NA)
    df['pmid'] = df['pmid'].fillna(df['pmid_2'])
    df.drop(columns=['pmid_2'], inplace=True)
    return df

def convenience_rearrange_cols(df):
    # rearrange columns for convenience:
    # the ideal order:
    # km_feedback, kcat_feedback, pmid
    # descriptor, km, kcat, kcat_km, enzyme, substrate, organism, temperature, pH, solvent, other, mutant, 
    # enzyme_2, ec_2, ref_2, pH_2, temperature_2, substrate_2, comments_2, km_2, kcat_2, kcat_km_2, mutant_2, 
    # ALSO: rename variant_2 and descriptor_2 to comments_2 if present.
    
    # in google sheets, these will be hidden: 
    # organism, temperature, pH, solvent, other
    # ec_2, ref_2, pH_2, temperature_2, 
    # therefore, try to sandwich them between relevant columns
    
    # also, try to be understanding if columns are missing. Also, rename variant_2 and descriptor_2 to comments_2 if present.
    
    df = df.copy()
    df.rename(columns={'variant_2': 'comments_2', 'descriptor_2': 'comments_2', 'solvent': 'solution'}, inplace=True)
    columns = ['km_feedback', 'kcat_feedback', 'pmid', 
               'descriptor', 'km', 'kcat', 'kcat_km', 'enzyme', 'substrate', 
               'temperature', 'pH', 'solution', 'other', 
               'mutant',
               'enzyme_2', 
               'ec_2', 'ref_2', 'temperature_2', 'pH_2', 
               'substrate_2', 'comments_2', 'km_2', 'kcat_2', 'kcat_km_2', 'mutant_2']
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    # then, retain all remaining columns
    df = df[columns + [col for col in df.columns if col not in columns]]
    return df
    
    
    
    