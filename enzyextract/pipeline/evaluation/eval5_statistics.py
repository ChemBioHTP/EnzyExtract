from colorama import Fore, Style
import polars as pl

from enzyextract.pipeline.evaluation.utilities import printb, printred

def _unique_x_stats(df: pl.DataFrame, prefix='  ', print=print):
    # unique enzyme names
    _enzyme_count = df.select(
        pl.col('enzyme').unique().alias('unique_enzyme_names')
    ).height
    print(f"{prefix}Unique enzyme names", _enzyme_count)

    # _enzyme_all_count = pl.concat([
    #     df['enzyme'],
    #     df['enzyme_full']
    # ]).n_unique()
    # print(f"{prefix}Unique enzyme names (all)", _enzyme_all_count)
    
    _organism_count = df['organism'].n_unique()
    print(f"{prefix}Unique organisms", _organism_count)
    if 'clean_mutant' in df.columns:
        # unique mutants
        _mutant_count = df.select(
            pl.col('clean_mutant').list.sort().list.join(' ').unique().alias('unique_mutants')
        ).height
    else:
        _mutant_count = df.select(
            # pl.col('clean_mutant').list.sort().list.join(' ').unique().alias('unique_mutants')
            pl.col('mutant')
        ).n_unique()
    print(f"{prefix}Unique mutants", _mutant_count)

    _substrate_count = df['substrate'].n_unique()
    printred(f"{prefix}Unique substrates", _substrate_count)
    if 'smiles' in df.columns:
        _smiles_count = df['smiles'].n_unique()
        printred(f"{prefix}Unique SMILES", _smiles_count)
    

def _print_esk(df, prefix='  '):
    _has_enzyme_substrate_kinetic = df.filter(
        (pl.col('enzyme').is_not_null() | pl.col('enzyme_full').is_not_null()) |
        (pl.col('substrate').is_not_null() | pl.col('substrate_full').is_not_null()) |
        (pl.col('kcat').is_not_null() | pl.col('km').is_not_null() | pl.col('kcat_km').is_not_null())
    )
    printred(f"{prefix}Non-null e/s/k count", _has_enzyme_substrate_kinetic.height)
    
def _column_stats(df: pl.DataFrame, title='', prefix='  '):
    print(title)

    printred(f"{prefix}Height", df.height)

    _print_esk(df, prefix=prefix)

    _kcat_count = df.filter(pl.col('kcat').is_not_null()).height
    printred(f"{prefix}kcat count", _kcat_count)
    _km_count = df.filter(pl.col('km').is_not_null()).height
    printred(f"{prefix}km count", _km_count)



    # high/medium/low confidence
    availability = df.with_columns(
        (
            (pl.col('enzyme').is_not_null() | pl.col('enzyme_full').is_not_null()).cast(pl.Int64)
            + (pl.col('organism').is_not_null()).cast(pl.Int64)
            + (pl.col('uniprot').is_not_null()).cast(pl.Int64)
            + (pl.col('ncbi').is_not_null()).cast(pl.Int64)
            + (pl.col('pdb').replace('null', None).is_not_null()).cast(pl.Int64)
        ).alias('enzyme.availability')
    )
    availability = availability.with_columns(
        pl.when(pl.col('enzyme.availability') >= 3)
        .then(pl.lit('high'))
        .when(pl.col('enzyme.availability') == 2)
        .then(pl.lit('medium'))
        .otherwise(pl.lit('low'))
        .alias('enzyme.confidence')
    )
    print(f"{prefix}High confidence (≥3/5)", availability.filter(pl.col('enzyme.confidence') == 'high').height)
    print(f"{prefix}Medium confidence (2/5)", availability.filter(pl.col('enzyme.confidence') == 'medium').height)
    print(f"{prefix}Low confidence (1/5)", availability.filter(pl.col('enzyme.confidence') == 'low').height)

    _unique_x_stats(df, prefix=prefix)
    pass

def _statistics_legacy():
    old_df = pl.read_parquet('data/export/TheData.parquet')
    old_kcat_df = pl.read_parquet('data/export/TheData_kcat.parquet')

    print(f"{Fore.BLUE}### v0.1 (preprint){Style.RESET_ALL}")
    print("Height", old_df.shape)
    printred("Height (has kcat)", old_kcat_df.height)

    old_pruned_df = old_kcat_df.filter(
        ~pl.col('kcat').fill_null('').str.contains('10^', literal=True)
        & ~pl.col('km').fill_null('').str.contains('10^', literal=True)
    )
    print("Old height (pruned)", old_pruned_df.shape)
    print()
    # _column_stats(old_df, title='old', prefix='  ')

    _column_stats(old_pruned_df, title='pruned', prefix='  ')
    print()



def _statistics(unpruned: pl.DataFrame, pruned: pl.DataFrame):

    # exit(0)

    ### filter by pmid with kcat
    _kcat_pmids = pruned.filter(
        pl.col('kcat').is_not_null()
    ).select('pmid').unique()
    kcat_pmid_subset = pruned.join(
        _kcat_pmids,
        on='pmid',
        how='semi'
    )

    print(f"{Fore.BLUE}### v0.2 (revision){Style.RESET_ALL}")

    printred("Unpruned height", unpruned.height)
    printred("Pruned height", pruned.height)
    printred("Unpruned height, by pmid w/ kcat", kcat_pmid_subset.height)


    ### naive filter by kcat
    unpruned_kcat = unpruned.filter(
        pl.col('kcat').is_not_null()
    )
    printred("Unpruned #kcat", unpruned_kcat.height)
    _print_esk(unpruned_kcat)
    printred("Unpruned #km", unpruned_kcat.filter(
        pl.col('km').is_not_null()
    ).height)

    kcat_subset = pruned.filter(
        pl.col('kcat').is_not_null()
    )
    printred("Pruned #kcat", kcat_subset.height)


    # _column_stats(kcat_subset, "Pruned, from pmids with ≥1 kcat")
    printred("Pruned #kcat, by pmid w/ kcat", kcat_pmid_subset.filter(
        pl.col('kcat').is_not_null()
    ).height)
    printred("Pruned #km, by pmid w/ kcat",  kcat_pmid_subset.filter(
        pl.col('km').is_not_null()
    ).height)
    
    # print()
    # kcat_subset = pruned.join(
    #     kcat_pmids,
    #     on='pmid',
    #     how='semi'
    # )
    # print("Pruned height", pruned.shape)
    _column_stats(kcat_subset, "Pruned, kcat only")
    # _column_stats(kcat_pmid_subset, "Pruned, by pmid w/ kcat")

    _ec_count = pl.concat([
        kcat_subset['enzyme_ecs'],
        kcat_subset['enzyme_ecs_full'],
    ]).explode().n_unique()
    printred("Unique EC numbers", _ec_count)
    print()


def _brenda_statistics():
    brenda_df = pl.read_parquet('data/brenda/brenda_kcat_cleanest.parquet').rename({
        'turnover_number': 'kcat',
        'km_value': 'km',
        'organism_name': 'organism'
    }).with_columns(
        pl.lit(None).alias('enzyme_full'),
        pl.lit(None).alias('uniprot'),
        pl.lit(None).alias('ncbi'),
        pl.lit(None).alias('pdb'),
    )

    # join with brenda smiles
    brenda_smiles = pl.read_parquet('data/thesaurus/substrate/latest_substrate_thesaurus.parquet')

    brenda_df = brenda_df.join(
        brenda_smiles.select('name', 
                             pl.col('smiles_brenda').alias('smiles')),
        left_on='substrate',
        right_on='name',
        how='left'
    )

    print(f"{Fore.BLUE}### BRENDA{Style.RESET_ALL}")
    # print("Height", brenda_df.shape)

    _kcat_count = brenda_df.filter(pl.col('kcat').is_not_null()).height
    print("kcat count", _kcat_count)
    _km_count = brenda_df.filter(pl.col('km').is_not_null()).height
    print("km count", _km_count)

    # _column_stats(brenda_df, title='BRENDA', prefix='  ')
    _unique_x_stats(brenda_df, prefix='  ', print=printred)
    print()


def _delta_statistics(ours, brenda):
    """
    Measure "new" rows not found in the other dataset.

    Specifically, we consider two statistics:
    (1) numerical values not found in BRENDA
        - This is performed with a left ASOF join on kcat value.
        - Note: this counts an error in BRENDA or an error in our extraction as a "new" value too.
    (2) cardinality check: per PMID: (rows in ours) - (rows in BRENDA), when positive.
        - If one of BRENDA/ours has an error but both pipelines extract the same number of values,
        (cardinality is the same), this statistic is not affected (more robust).
    (3) simple cardinality check: (total rows in ours) - (total rows in BRENDA)
        - Most conservative estimate
        - Note that since not all BRENDA rows have PMIDs, 
    """

    print(f"{Fore.BLUE}### Delta statistics{Style.RESET_ALL}")

    # explode ranges
    brenda_values = brenda.filter(
        pl.col('turnover_number').is_not_null()
    ).with_columns(
        pl.col('turnover_number').str.split(' -- ').list.eval(pl.element().replace("", None).cast(pl.Float64)).alias('kcat_value'),
        pl.col('km_value').str.split(' -- ').list.eval(pl.element().replace("", None).cast(pl.Float64)).alias('km_value')
    )
    brenda_values = brenda_values.explode('kcat_value').explode('km_value')
    brenda_values = brenda_values.with_columns(
        pl.col('kcat_value').sort().over('pmid'),
    )
    # join on pmid and left asof join on kcat_value
    matches = ours.select('pmid', 'kcat_value').join_asof(
        brenda_values.select('pmid', 'kcat_value'),
        on='kcat_value',
        by='pmid',
        strategy='nearest',
        tolerance=0.1,
        suffix='_brenda',
        coalesce=False,
        check_sortedness=False
    )
    unique_to_ours = matches.filter(
        pl.col('kcat_value').is_not_null() &
        pl.col('kcat_value_brenda').is_null()
    )
    print(unique_to_ours.height, "rows unique to ours") # 142233

    cardinality_check = ours.group_by('pmid').agg(
        pl.col('kcat_value').drop_nulls().count().alias('kcat_count')
    )
    
    cardinality_check = cardinality_check.join(
        brenda_values.group_by('pmid').agg(
            pl.col('kcat_value').drop_nulls().count().alias('kcat_count_brenda')
        ),
        on='pmid',
        how='left'
    )
    cardinality_check = cardinality_check.with_columns(
        # max(..., 0)
        pl.when(pl.col('kcat_count_brenda').is_null())
        .then(pl.col('kcat_count').cast(pl.Int32))
        .otherwise((pl.col('kcat_count').cast(pl.Int32) - pl.col('kcat_count_brenda').cast(pl.Int32)).clip(lower_bound=0)).alias('kcat_delta')
    )
    # sum kcat_delta
    kcat_delta = cardinality_check['kcat_delta'].sum()
    print(kcat_delta, "cardinality check (ours - brenda)") # 121089

    brenda_count = 86919 # reported
    # brenda_count = 79897 # height of brenda_kcat_tallest
    simplest_delta = ours.filter(pl.col('kcat').is_not_null()).height - brenda_count
    print(simplest_delta, "simplest delta (ours - brenda)") # 89544

    # PMID statistics (how many new documents?)

    pmid_ours = ours.select('canonical').unique()
    pmid_brenda = brenda.select('pmid').unique()

    print(pmid_ours.height, "PMIDs in ours")
    print(pmid_brenda.height, "PMIDs in BRENDA")
    pmid_delta = pmid_ours.join(
        pmid_brenda,
        left_on='canonical',
        right_on='pmid',
        how='anti'
    ).height
    printred("PMIDs in ours not in BRENDA", pmid_delta) # 121089
    print()



def _prune_statistics(pruned, unpruned):
    print(f"{Fore.BLUE}### Pruned{Style.RESET_ALL}")

    unpruned = unpruned.filter(
        pl.col('kcat').is_not_null()
    )
    _hallucination = unpruned.filter(
        pl.col('flag.hallucination').is_not_null()
    )
    printred("Pruned #hallucination", _hallucination.height)
    printred("Pruned %hallucination", _hallucination.height / unpruned.height * 100)
    _repetitive = unpruned.filter(
        pl.col('flag.repetitive').is_not_null()
    )
    printred("Pruned #repetitive", _repetitive.height)
    printred("Pruned %repetitive", _repetitive.height / unpruned.height * 100)
    _scientific = unpruned.filter(
        pl.col('flag.scientific').is_not_null()
    )
    printred("Pruned #scientific", _scientific.height)
    printred("Pruned %scientific", _scientific.height / unpruned.height * 100)

    def _legacy_prune_statistics():
        old_df = pl.read_parquet('data/export/TheData_kcat.parquet')
        ours_unpruned = unpruned.filter(
            pl.col('kcat').is_not_null()
        )
        ours_pruned = pruned.filter(
            pl.col('kcat').is_not_null()
        )
        _num_desc_duplicates = old_df.height - old_df.n_unique()
        print("Legacy height", old_df.height)
        print("Intermediate height", ours_unpruned.height)
        print("Final height", ours_pruned.height)
        printred("# removed from legacy b/c duplicate descriptors", _num_desc_duplicates)
        printred("# removed from legacy b/c duplicate document", old_df.height - ours_unpruned.height - _num_desc_duplicates)
        printred("# removed from intermediate b/c {hallucination, repetitive, scientific}",
                ours_unpruned.height - ours_pruned.height)
        # isolate specific categories
        printred("  # hallucination", _hallucination.height)
        printred("  # repetitive", _repetitive.height)
        _hallucination_and_repetitive = _hallucination.filter(
            pl.col('flag.repetitive').is_not_null()
        )
        printred("  # hallucination and repetitive", _hallucination_and_repetitive.height)
        printred("  # scientific", _scientific.height)
    _legacy_prune_statistics()
    print()



    


def _paired_statistics():
    """
    Compared the pruned paired dataset
    """
    paired_df = pl.read_parquet('data/metrics/brenda/brenda_pruned.parquet')

    goodall = paired_df.filter(
        pl.col('same_enzyme') &
        pl.col('same_substrate')
    )

    print(f"{Fore.BLUE}### BRENDA+EnzyExtractDB Paired{Style.RESET_ALL}")
    printred("#kcat", goodall.filter(
        pl.col('kcat_2').is_not_null() & pl.col('kcat_1').is_not_null()
    ).height)
    printred("#km", goodall.filter(
        pl.col('km_2').is_not_null() & pl.col('km_1').is_not_null()
    ).height)


    print()




if __name__ == '__main__':

    pruned = pl.read_parquet('data/export/TheData_pruned.parquet')
    unpruned = pl.read_parquet('data/export/TheData_unpruned.parquet')

    brenda = pl.read_parquet('data/brenda/brenda_kcat_cleanest.parquet')
    # _statistics_legacy()
    _statistics(unpruned, pruned)
    _delta_statistics(pruned, brenda)
    _brenda_statistics()

    _prune_statistics(pruned, unpruned)

    _paired_statistics()