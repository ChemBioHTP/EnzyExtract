import os
import pandas as pd
import polars as pl

from enzyextract.hungarian.csv_fix import prep_for_hungarian, widen_df
from enzyextract.utils.pmid_management import pmids_from_cache, cache_pmids_to_disk, lift_pmids
from enzyextract.utils.doi_management import doi_to_filename
from relink.enzyextract.utils.pmid_management import pmids_from_directory


def script_expand_brenda():
    # produce a wide brenda to peruse 
    brenda_df = pd.read_csv(r'C:\conjunct\vandy\yang\corpora\brenda\brenda_km_kcat_key_v2.csv')
    brenda_df = prep_for_hungarian(brenda_df)
    brenda_df = widen_df(brenda_df)

    brenda_df.to_csv(r'C:\conjunct\vandy\yang\corpora\brenda\brenda_km_kcat_20241109_widened.csv', index=False)

def script_collect_runeem_set():
    """
    collect all pmids annotated by runeem so far
    """

    brenda_wiley_tbl = pd.read_csv(r'data/humaneval/runeem components/brenda_wiley_tbl .xlsx - clean_tbl.csv', dtype={'doi': str})

    brenda_wiley_notbl = """103713
107026
108102
110588
1082813
1084291
1091485
1100398
1100400
1104357
1149736
10074715
10091591
10095793
10215898
10225425
10231382
10447693
10469136
10469153
10491134
10491189
10504564
10518807
10518926
10526176
10542053
10544268
10567717
10571860
10601849
10601871
10664468
10672020
10691985
10712602
10734215
10849007
10931180
10951215
10996306
10998052
10998181
11078894
11094156
11172808
11179970
11185964
11231285
11231302
11240138
11248687
11274461
11298769
11358525
11389720
11389739
11453995
11457449
11470266""".split('\n')

    # 63 papers from rekcat
    # ground_truth_csv = r'C:/conjunct/vandy/yang/corpora/eval/brenwi_runeem.csv'
    rekcat_csv = 'C:/conjunct/vandy/yang/corpora/eval/rekcat_checkpoint_v4_only_values_checked.csv'
    rekcat_df = pd.read_csv(rekcat_csv, dtype={'pmid': str})
    # ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/rekcat/rekcat_checkpoint_5 - rekcat_ground_63.csv'


    df_apogee = pd.read_csv('data/humaneval/runeem components/apogee_runeem_20241025.csv', dtype={'pmid': str})

    pmids = set(rekcat_df['pmid'].tolist())
    pmids.update(set(df_apogee['pmid'].tolist()))
    pmids.update(set(brenda_wiley_tbl['doi'].tolist()))
    pmids.update(set(brenda_wiley_notbl))

    print(len(pmids))




    train_pmids = pmids_from_cache('finetunes/pmids-t2neboth.train')
    val_pmids = pmids_from_cache('finetunes/pmids-t2neboth.val')

    in_common = train_pmids.intersection(pmids)
    in_common.update(val_pmids.intersection(pmids))

    print(len(in_common))

    eval_set = pmids - in_common

    print(len(eval_set))

    # now, collect 500 - 290 = 210 pmids from BRENDA to add to our evaluation

    need = 500 - len(eval_set)

    print("Need", need)

    brenda_pmids = pmids_from_directory('D:/brenda', recursive=True)
    downloaded_pmids = pmids_from_cache('downloaded')

    available_brenda_pmids = brenda_pmids & downloaded_pmids
    available_brenda_pmids = available_brenda_pmids - pmids
    available_brenda_pmids = available_brenda_pmids - in_common

    # sample 210
    sampled_brenda_pmids = set(pd.Series(list(available_brenda_pmids)).sample(need))

    eval_set.update(sampled_brenda_pmids)

    cache_pmids_to_disk(eval_set, 'eval/arctic')

def script_lift_runeem_set():

    # pmids = pmids_from_cache('eval/arctic')

    df = pd.read_csv('data/humaneval/runeem/runeem_20241124.csv', dtype={'pmid': str})
    pmids = set(df['pmid'].dropna())

    # lift_pmids(pmids, 'D:/topoff', 'C:/conjunct/tmp/km_unit_apogee')
    # lift_pmids(pmids, 'D:/wos', 'C:/conjunct/tmp/km_unit_apogee')

    pmids = sorted(pmids)

    pmids = [doi_to_filename(pmid) for pmid in pmids]


    additional = """1510926
1551850
1554373
12741843
12741958
12748196
12760898
12766158
12788706
12813039
12878037
12888555
12922166
12954635
12962500
13678418
13678419
14500895
14556651
14572311
14580998
14690410
14717585
14750781
14871895
14960587
14976196
14985330
15044726
15065882
15123417
15123647
15123660
15136574
15152005
15155769
15158713
15171683
15180996
15182361
15190062
15210695
15238222
15247292
15279950
15288786
15342576
15342621
15350148
15371425
15455210
15471872
15471876""".split('\n')

    pmids.extend(additional)
    lift_pmids(pmids, 'D:/papers/brenda', 'C:/conjunct/tmp/eval/arctic')
    lift_pmids(pmids, 'D:/papers/scratch', 'C:/conjunct/tmp/eval/arctic')
    lift_pmids(pmids, 'D:/papers/topoff', 'C:/conjunct/tmp/eval/arctic')
    lift_pmids(pmids, 'D:/papers/wos', 'C:/conjunct/tmp/eval/arctic')

    lifted = []
    for filename in os.listdir('C:/conjunct/tmp/eval/arctic'):
        if filename.endswith('.pdf'):
            lifted.append(filename[:-4])
    
    unfound = set(pmids) - set(lifted)
    print("Unfound", unfound)
    
def script_create_runeem_df():
    # create a dataframe of the runeem set
    # brenda_wiley_tbl = pd.read_csv(r'data/humaneval/runeem components/brenda_wiley_tbl .xlsx - clean_tbl.csv', dtype={'doi': str})
    brenda_wiley_tbl = pd.read_csv(r'data/humaneval/runeem components/brenda_wiley_tbl .xlsx - clean_tbl 20241125.csv', dtype={'doi': str})
    brenda_wiley_notbl = pd.read_csv(r'data/humaneval/runeem components/brenda_wiley_notbl_REFINED - Sheet1 20241125.csv', dtype={'doi': str})
    # rekcat_df = pd.read_csv(r'C:/conjunct/vandy/yang/corpora/eval/rekcat_checkpoint_v4_only_values_checked.csv', dtype={'pmid': str})
    rekcat_df = pd.read_csv(r'data/humaneval/runeem components/rekcat export - export 20241125.csv', dtype={'pmid': str})
    rekcat_bottom_df = pd.read_csv(r'data/humaneval/runeem components/rekcat export - export galen 20241125.csv', dtype={'pmid': str})
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

    runeem_df = prep_for_hungarian(runeem_df)
    runeem_df.to_csv('data/humaneval/runeem/runeem_20241125.csv', index=False)

def script_count_pdfs():
    root_directory = "D:\\"
    # List of subdirectories to include
    target_dirs = {"wos", "brenda", "scratch", "topoff"}
    
    for dirpath, dirnames, filenames in os.walk(root_directory):
        # Check if the current directory is one of the target directories
        if any(target in os.path.normpath(dirpath).split(os.sep) for target in target_dirs):
            # Count PDF files in the current directory
            pdf_count = sum(1 for file in filenames if file.lower().endswith('.pdf'))
            
            # Print the count if it's nonzero
            if pdf_count > 0:
                print(f"{dirpath}: {pdf_count} PDFs")

# Run the function


def convert_parquet():
    # convert shit to parquet
    # df = pl.read_csv('fetch_sequences/substrates/brenda_inchi_all.tsv', separator='\t', schema_overrides={'brenda_id': pl.Utf8})
    so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}
    df = pl.read_csv('data/_compiled/apogee-all.tsv', separator='\t', schema_overrides=so)


    # df = df.with_columns([
    #     pl.col('brenda_id').replace('-', None).cast(pl.UInt32)
    # ])

    # df.write_parquet('data/substrates/brenda_inchi_all.parquet')
    df.write_parquet('data/_compiled/apogee_all.parquet')

def determine_mutants():
    # check to see how many rows have valid mutants
    from enzyextract.fetch_sequences.read_pdfs_for_idents import mutant_pattern, mutant_v3_pattern
    df = pl.read_parquet('data/_compiled/apogee_all.parquet')
    df = df.filter(
        pl.col("mutant").str.contains(mutant_pattern.pattern)
        | pl.col("mutant").str.contains(mutant_v3_pattern.pattern)

    )
    print(df.shape)
    exit(0)

if __name__ == "__main__":
    # script_count_pdfs()
    # script_create_runeem_df()
    # script_lift_runeem_set()
    # df = script_look_for_ecs()
    # exit(0)
    determine_mutants()
    convert_parquet()
    pass
    exit(0)

    df = pl.read_parquet('data/_compiled/apogee_all.parquet')
    print(df)
    # script_ec_success_rate()