import polars as pl

from enzyextract.post.pl_validation import expect_columns, expect_not_columns, expect_schema

def create_generic_pkey(
    orig_df: pl.DataFrame,
    rename_to: str,
):
    """
    substrate_ctx should have columns:
    - pmid, fullname, synonyms

    Creates a dataframe with the columns:
    - ${rename_to}.pkey, pmid, ${rename_to}, is_synonym
    - pkey is a unique identifier for each substrate
    - a fullname and synonym share the same pkey

    NOTE: pkey may change between runs, so only use them to join but do not store them.
    """

    expect_columns(orig_df, ['pmid', 'fullname', 'synonyms'])
    assert orig_df['synonyms'].dtype == pl.List

    pkey = f'{rename_to}.pkey'
    orig_with_pkey = orig_df.with_row_index(pkey)

    full_df = orig_with_pkey.select(pkey, 'pmid', 'fullname')
    full_df = full_df.drop_nulls('fullname').rename({'fullname': rename_to})
    full_df = full_df.with_columns(
        pl.lit(False).alias('is_synonym')
    )

    syn_df = orig_with_pkey.select(pkey, 'pmid', 'synonyms')
    syn_df = syn_df.drop_nulls('synonyms').rename({'synonyms': rename_to})
    syn_df = syn_df.with_columns(
        pl.lit(True).alias('is_synonym')
    )
    syn_df = syn_df.explode(rename_to)

    name_to_pkey = full_df.merge_sorted(syn_df, key=pkey)
    print(name_to_pkey)
    return orig_with_pkey, name_to_pkey



def join_substrate_ctx(
    data: pl.DataFrame,
    substrate_ctx: pl.DataFrame
):
    """
    Added fields will have the prefix 'substrate.'

    LHS should have columns:
    - pmid, substrate

    RHS should have columns:
    - pmid, fullname, synonyms
    """

    expect_columns(data, ['pmid', 'substrate'])
    expect_columns(substrate_ctx, ['pmid', 'fullname', 'synonyms'])

    substrate_ctx_pkey, name_to_pkey = create_generic_pkey(substrate_ctx, rename_to='substrate')
    # sub_pure = sub_pkey.select('pkey', 'substrate').unique(keep='first') # prefers the fullname

    result = data.join(
        name_to_pkey.select(['pmid', 'substrate', 'substrate.pkey']),
        on=['pmid', 'substrate'],
        how='left',
        coalesce=True
    )
    _substrate_ctx_join = substrate_ctx_pkey.select(
        'substrate.pkey', 'fullname', 'synonyms'
    ).rename(lambda x: f'substrate.{x.removeprefix("substrate.")}')
    result = result.join(
        _substrate_ctx_join,
        on='substrate.pkey',
        how='left',
    )
    print(result)
    return result, substrate_ctx_pkey


def join_enzyme_ctx(
    data: pl.DataFrame,
    enzyme_ctx: pl.DataFrame
):
    """
    Added fields will have the prefix 'enzyme.'

    LHS should have columns:
    - pmid, fragments

    RHS should have columns:
    - pmid, fullname, synonyms
    """
    expect_schema(data, {
        'pmid': pl.Utf8,
        'fragments': pl.List(pl.Utf8),
    })
    expect_columns(enzyme_ctx, ['pmid', 'fullname', 'synonyms'])

    enzyme_ctx_pkey, name_to_pkey = create_generic_pkey(enzyme_ctx, rename_to='enzyme')

    data_wide = data.explode('fragments').rename({
        'fragments': 'fragment'
    })
    data_wide = data_wide.join(
        name_to_pkey.select(['pmid', 'enzyme', 'enzyme.pkey']),
        left_on=['pmid', 'fragment'],
        right_on=['pmid', 'enzyme'],
        how='left',
        coalesce=True
    )
    _enzyme_ctx_join = enzyme_ctx_pkey.select(
        'enzyme.pkey', 'fullname', 'synonyms'
    ).rename(lambda x: f'enzyme.{x.removeprefix("enzyme.")}')
    
    # TODO: case insensitive join, fuzzy join

    enzyme_matches = data_wide.drop_nulls('enzyme.pkey').group_by(
        'data.pkey'
    ).agg(
        'enzyme.pkey',
    )

    # enzyme_matches = enzyme_matches.join(
    #     _enzyme_ctx_join,
    #     on='enzyme.pkey',
    #     how='left',
    # )

    # match back into original
    result = data.join(
        enzyme_matches.select(
            'data.pkey', 'enzyme.pkey'
        ),
        on='data.pkey',
        how='left',
    )
    print(enzyme_matches)
    return result, enzyme_ctx_pkey


    
