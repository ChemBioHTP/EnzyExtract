import pandas as pd    
from enzyextract.hungarian.csv_fix import clean_columns_for_valid, widen_df

def script_create_runeem_df():
    # create a dataframe of the runeem set
    # brenda_wiley_tbl = pd.read_csv(r'data/humaneval/runeem components/brenda_wiley_tbl .xlsx - clean_tbl.csv', dtype={'doi': str})
    brenda_wiley_tbl = pd.read_csv(r'data/humaneval/runeem components/brenda_wiley_tbl .xlsx - clean_tbl 20241125.csv', dtype={'doi': str})
    brenda_wiley_notbl = pd.read_csv(r'data/humaneval/runeem components/brenda_wiley_notbl_REFINED - Sheet1 20241125.csv', dtype={'doi': str})
    # rekcat_df = pd.read_csv(r'C:/conjunct/vandy/yang/corpora/eval/rekcat_checkpoint_v4_only_values_checked.csv', dtype={'pmid': str})
    # rekcat_df = pd.read_csv(r'data/humaneval/runeem components/rekcat export - export 20241219.tsv', dtype={'pmid': str}, sep='\t')
    rekcat_df = pd.read_csv(r'data/humaneval/runeem components/rekcat export - export 20250322.tsv', dtype={'pmid': str}, sep='\t')
    rekcat_bottom_df = pd.read_csv(r'data/humaneval/runeem components/rekcat export - export galen 20241219.tsv', dtype={'pmid': str}, sep='\t')
    # ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/rekcat/rekcat_checkpoint_5 - rekcat_ground_63.csv'


    apogee_df = pd.read_csv(r'data/humaneval/runeem components/apogee_runeem_20241125.csv', dtype={'pmid': str})

    for df in [brenda_wiley_tbl, brenda_wiley_notbl, rekcat_df, apogee_df]:
        print(df.columns)
    
    # Index(['missing row', 'doi', 'enzyme', 'variant', 'substrate', 'kcat', 'km',
    #    'kcat/km', 'temperature', 'pH', 'enzyme_full', 'organism', 'pdb_id',
    #    'uniprot', 'ncbi', 'seq_first_10', 'Check'],
    #   dtype='object')
    # Index(['doi', 'enzyme', 'variant', 'substrate', 'kcat', 'km', 'kcat/km',
    #     'temperature', 'pH', 'enzyme_full', 'organism', 'pdb_id', 'uniprot',
    #     'ncbi', 'seq_first_10'],
    #     dtype='object')
    # Index(['km_feedback', 'kcat_feedback', 'pmid', 'descriptor', 'km', 'kcat',
    #     'kcat_km', 'enzyme', 'substrate', 'organism', 'temperature', 'pH',
    #     'solvent', 'other', 'mutant', 'enzyme_2', 'ec_2', 'ref_2', 'pH_2',
    #     'temperature_2', 'substrate_2', 'comments_2', 'km_2', 'kcat_2',
    #     'kcat_km_2', 'mutant_2', 'Unnamed: 26'],
    #     dtype='object')
    # Index(['enzyme', 'enzyme_full', 'substrate', 'substrate_full', 'mutant',
    #     'organism', 'kcat', 'km', 'kcat_km', 'temperature', 'pH', 'solution',
    #     'other', 'descriptor', 'pmid'],
    #     dtype='object')

    # RENAMES:
    # from brenda_wiley_{notbl,tbl}:

    builder = []

    brenda_wiley_tbl['src'] = 'brenda_wiley_tbl'
    brenda_wiley_notbl['src'] = 'brenda_wiley_notbl'

    brenwi_renames = {
        'doi': 'pmid',
        'variant': 'mutant',
        'kcat/km': 'kcat_km',
    }
    brenwi_keep = ['pmid', 'enzyme', 'enzyme_full', 'organism', 'substrate', 'substrate_full', 
            'kcat', 'km', 'kcat_km', 'temperature', 'pH', 'mutant', 'src']

    for df in [brenda_wiley_tbl, brenda_wiley_notbl]:
        df = df.rename(columns=brenwi_renames)
        # add enzyme_full column
        df['enzyme_full'] = ''
        df['substrate_full'] = ''
        builder.append(df[brenwi_keep])

    

    # need to remove the half, to prevent conflicts
    rekcat_df = pd.concat([rekcat_df, rekcat_bottom_df], ignore_index=True)

    rekcat_keep = ['pmid', 'enzyme_2', 'organism', 'substrate_2', 'km_2', 'kcat_2', 'kcat_km_2', 'comments_2', 'mutant_2']
    rekcat_df = rekcat_df[rekcat_keep]

    rekcat_df = rekcat_df.dropna(how='all')

    rekcat_renames = {
        'enzyme_2': 'enzyme',
        'substrate_2': 'substrate',
        'km_2': 'km',
        'kcat_2': 'kcat',
        'kcat_km_2': 'kcat_km',
        'comments_2': 'comments',
        'mutant_2': 'mutant_original'
    }
    rekcat_df = rekcat_df.rename(columns=rekcat_renames)

    rekcat_df = widen_df(rekcat_df)
    # add mutant, pH, temperature
    
    # add substrate_full, a blank column
    rekcat_df['enzyme_full'] = ''
    rekcat_df['substrate_full'] = ''
    rekcat_df['src'] = 'rekcat'

    # populate the 'mutant_preferred' column with 'mutant_original', then 'mutant'
    rekcat_df['mutant_preferred'] = rekcat_df['mutant_original'].fillna(rekcat_df['mutant'])
    # replace the 'mutant' column with 'mutant_preferred'
    rekcat_df['mutant'] = rekcat_df['mutant_preferred']
    out = rekcat_df[['pmid', 'enzyme', 'enzyme_full', 'organism', 'substrate', 'substrate_full', 
            'kcat', 'km', 'kcat_km', 'temperature', 'pH', 'mutant', 'src']]
    builder.append(out)

    # apogee_renames = {
    #     'enzyme_full': 'enzyme',
    #     'substrate_full': 'substrate'
    # }
    # apogee_df = apogee_df.rename(columns=apogee_renames)

    apogee_df['src'] = 'apogee'
    out = apogee_df[['pmid', 'enzyme', 'enzyme_full', 'organism', 'substrate', 'substrate_full', 
            'kcat', 'km', 'kcat_km', 'temperature', 'pH', 'mutant', 'src']]
    builder.append(out)

    runeem_df = pd.concat(builder, ignore_index=True)

    # ignore these pmids:

    blacklist = [8900165, # rekcat, excluded (incomplete, me)
    8939970,
    9148919,
    9169443,
    9228062,
    9352635,
    9398217,
    9425109,
    9605319,
    9632644,
    9693127,
    9694855,
    9733680,
    9867836,

    1554373, # rekcat, excluded (pita to correct)
    
    
    15350148,
    15471872,

    11856361, # brenda_wiley_notbl, excluded (incomplete)
    
    1102299, # apogee, excluded (large table missing)
    ] # rekcat, excluded (confused)
    blacklist = [str(i) for i in blacklist]

    runeem_df = runeem_df[~runeem_df['pmid'].isin(blacklist)]

    runeem_df = clean_columns_for_valid(runeem_df)
    # runeem_df.to_csv('data/humaneval/runeem/runeem_20241219.csv', index=False)
    runeem_df.to_csv('data/humaneval/runeem/runeem_20250322.csv', index=False)


if __name__ == '__main__':
    script_create_runeem_df()