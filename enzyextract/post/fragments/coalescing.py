
# def coalesce_collect(
#     # sources: List[Tuple[pl.DataFrame, Union[str, List[str]], Union[str, List[str]]]],
#     sources: List[Tuple[pl.DataFrame, Union[str, List[str]], str]],
#     final_column_name: str = 'coalesced',
#     additional_columns: List[str] = None,
#     common_column_name: str = 'fragment_id',
# ):
#     """
#     Takes 
#     sources: list of tuples of (DataFrame, col_name(s), save_as), in decreasing order of priority
#     common_columns: if provided, these columns are common to all DataFrames and will always be coalesced.

#     They will be coalesced into "final_column_name", which is a struct: 
#     {"value_1": ..., "value_2": ..., ...}
#     """

#     collector = None
#     column_names = []

#     for df, col_name, save_as in sources:
        
#         # collect_names = [col_name]
        
#         # assert len(col_names) == len(renames_to), "col_names and rename_to should be balanced"
        
#         # create a (variable-length) predicate to check for non-null values
#         plcol = pl.col(col_name)
#         non_null_predicate = (
#             plcol.is_not_null() & (plcol.str.len_chars() > 0)
#         )

#         assert common_column_name in df.columns, "DataFrame should contain 'fragment_id' column"

#         nonempty = (
#             df.select(common_column_name, plcol)
#             .filter(pl.col(common_column_name).is_not_null())
#             .filter(non_null_predicate)
#             # .with_columns(
#             #     pl.struct(collect_names).struct.rename_fields([f'value_{x}' for x in range(len(collect_names))]).alias(save_as)
#             # )
#             # .group_by('fragment_id', maintain_order=True) # when there are multiple values, keep all
#             # .agg(save_as)

#             .group_by(common_column_name, maintain_order=True) # when there are multiple values, keep all
#             .agg(plcol.unique())
#             .rename({
#                 col_name: save_as
#             })
#         )
#         if collector is None:
#             collector = nonempty
#         else:
#             collector = collector.join(
#                 nonempty,
#                 on=common_column_name,
#                 how='full',
#                 validate='m:1',
#                 coalesce=True, # important: fragment_id should be combined into one
#             )
#         column_names.append(save_as)
    
#     collector = collector.with_columns(
#         pl.coalesce(column_names).alias(final_column_name)
#     )

#     collector = collector.select(pl.selectors.all())

#     # Strange bug: 
#     # pyo3_runtime.PanicException: called `Result::unwrap()` on an `Err` value: 
#     # ComputeError(ErrString("RecordBatch requires an equal number of fields and arrays"))
#     # seems to be a result of repeatedly calling join() coalesce=False

#     return collector



# def coalesce_collect_struct(
#     # sources: List[Tuple[pl.DataFrame, Union[str, List[str]], Union[str, List[str]]]],
#     sources: List[Tuple[pl.DataFrame, Union[str, List[str]], str]],
#     final_column_name: str = 'coalesced',
#     additional_columns: List[str] = None,
# ):
#     """
#     Takes 
#     sources: list of tuples of (DataFrame, col_name(s), save_as), in decreasing order of priority
#     common_columns: if provided, these columns are common to all DataFrames and will always be coalesced.

#     They will be coalesced into "final_column_name", which is a struct: 
#     {"value_1": ..., "value_2": ..., ...}
#     """

#     collector = None
#     column_names = []

#     for df, col_names, save_as in sources:

#         if not isinstance(col_names, list):
#             col_names = [col_names]
        
#         collect_names = col_names
#         if additional_columns is not None:
#             collect_names = col_names + additional_columns
#         # if not isinstance(renames_to, list):
#         #     renames_to = [renames_to]
        
#         # assert len(col_names) == len(renames_to), "col_names and rename_to should be balanced"
        
#         # create a (variable-length) predicate to check for non-null values
#         non_null_predicate = None
#         for col_name in col_names:
#             curr_predicate = pl.col(col_name).is_not_null()
#             if df.schema[col_name] == pl.Utf8:
#                 curr_predicate = curr_predicate & (pl.col(col_name).str.len_chars() > 0)
#             if non_null_predicate is None:
#                 non_null_predicate = curr_predicate
#             else:
#                 non_null_predicate |= curr_predicate

#         nonempty = (
#             df.select('fragment_id', *collect_names)
#             .filter(non_null_predicate)
#             .with_columns(
#                 pl.struct(collect_names).struct.rename_fields([f'value_{x}' for x in range(len(collect_names))]).alias(save_as)
#             )
#             .group_by('fragment_id', maintain_order=True) # when there are multiple values, keep all
#             .agg(save_as)

#             # .group_by('fragment_id', maintain_order=True) # when there are multiple values, keep all
#             # .agg(col_names)
#             # .rename({
#             #     col_name: rename_to
#             #     for col_name, rename_to in zip(col_names, renames_to)
#             # })
#         )
#         if collector is None:
#             collector = nonempty
#         else:
#             collector = collector.join(
#                 nonempty,
#                 on='fragment_id',
#                 how='full',
#             )
#         column_names.append(save_as)
    
#     collector = collector.with_columns(
#         pl.coalesce(column_names).alias(final_column_name)
#     )

#     return collector

from typing import List, Sequence, Tuple, Union
import polars as pl

def coalesce_collect(
    # sources: List[Tuple[pl.DataFrame, Union[str, List[str]], Union[str, List[str]]]],
    sources: List[Tuple[pl.DataFrame, str, Union[str, Sequence[str], dict]]],
    column_renames: Union[str, Sequence[str]],

    join_key: str = 'fragment_id',

    final_join_strategy: str = 'best'
):
    """
    Takes 
    - sources: list of tuples of (DataFrame, df_name, df_columns), in decreasing order of priority
        - df_name: the name of the DataFrame, used as a prefix for the columns. Set to '' for no prefix.
        - df_columns can be a single column name or a list of column names, or a dictionary mapping column names to new names.
    - column_renames: df_columns will be renamed to these names, in the same order.
    - join_key: this is the column name that will be grouped-by and joined, and should be common to all dfs.
    - final_join_strategy: By default, rows from each df are only refered to by a "coalesce_id", which is a row index.
    If set to 'best', then the final "best" (coalesced) row will be joined with the columns in "df_columns", which contain
    the values from the best source.
    If set to 'all', then every intermediate step will also be joined with 

    If multiple df_columns are provided, the datatypes need to be consistent. This is enforced by a concat call.
    So each source should specify the same number of columns. This is enforced via concat (vertical_relaxed). 
    Exception: None can be used to indicate missing columns. 

    They will be coalesced into `best_coalesce_id`.
    """
    if final_join_strategy == "all":
        assert len(column_renames) == 1, "When final_join_strategy is 'all', only one column can be renamed (the coalesce_id)."

    coalesce_column_names = []

    all_selections = None
    # collect many selections from df, to be able to refer to rows
    # by a row_id. we will then vstack them.

    relational_df = None
    # instead of collecting values, collect the row_ids 
    # (makes it easier when there are many columns)
    if not isinstance(column_renames, list):
        column_renames = [column_renames]
    
    all_selections = pl.DataFrame(schema={
        'coalesce_id': pl.UInt32,
        join_key: pl.Null,
        **{k: pl.Null for k in column_renames}
    })

    for df, df_name, col_names in sources:
        fixed_col_names = [name if name is not None else pl.lit(None) for name in col_names]
        selection = df.lazy().select(
            join_key, 
            *fixed_col_names
        # remove rows that are all None
        ).filter(~pl.all_horizontal(pl.exclude(join_key).is_null())
                 
        # if join_key is not unique w.r.t. df, need to group by it.
        # example: descriptor (join_key: data_id) can contain multiple enzymes
        ).group_by(
            join_key
        ).agg(pl.all().unique())

        # if all_selections is None:
        #     # vertical stacking
        #     selection = selection.with_row_index('coalesce_id')
        #     all_selections = selection
        # else:
        # w = selection.collect()
        # rename columns
        renames = {}
        j = 0
        for col_name in selection.collect_schema().names():
            if col_name == join_key:
                continue
            else:
                renames[col_name] = column_renames[j]
                j += 1
        selection = selection.rename(renames)
        selection = selection.with_row_index('coalesce_id', offset=all_selections.height).collect()
        all_selections = pl.concat([
            all_selections,
            selection
        ], how='vertical_relaxed')
        # all_selections = all_selections.vstack(selection.collect())

        ids_only = selection.lazy().select(join_key, 'coalesce_id').rename({
            'coalesce_id': f'{df_name}.coalesce_id'
        })

        if relational_df is None:
            relational_df = ids_only
        else:
            relational_df = relational_df.join(
                ids_only,
                on=join_key,
                how='full',
                validate='m:1',
                coalesce=True, # important: fragment_id should be combined into one
            )
        coalesce_column_names.append(f'{df_name}.coalesce_id')

    # relational_df = relational_df.select(pl.selectors.all())

    # Strange bug: 
    # pyo3_runtime.PanicException: called `Result::unwrap()` on an `Err` value: 
    # ComputeError(ErrString("RecordBatch requires an equal number of fields and arrays"))
    # seems to be a result of repeatedly calling join() coalesce=False

    if final_join_strategy == 'best':
        relational_df = relational_df.with_columns(
            pl.coalesce(coalesce_column_names).alias('best_coalesce_id')
        ).collect()
        # join the best row with the columns in df_columns
        # this will be a single row per coalesce_id
        relational_df = relational_df.join(
            all_selections.drop(join_key, strict=False),
            left_on='best_coalesce_id',
            right_on='coalesce_id',
            how='left',
            validate='m:1',
            coalesce=True,
        )
    else:
        # keep all intermediate_steps
        relational_df = relational_df.with_columns(
            pl.concat_list(coalesce_column_names).list.drop_nulls().alias('all_coalesce_ids')
        )
        relational_df = relational_df.collect()

        # wow! join() works for values within lists
        # https://stackoverflow.com/questions/74017229/polars-join-on-list-items-without-explode-groupby
        # https://github.com/pola-rs/polars/pull/21687
        # relational_df = relational_df.join(
        #     all_selections.drop(join_key, strict=False),
        #     left_on='all_coalesce_ids',
        #     right_on='coalesce_id',
        #     how='left',
        #     # validate='m:1',
        #     coalesce=True,
        # )

        # pseudo-code:
        # relational_df = relational_df.with_columns(
        #     pl.col("all_coalesce_ids").list.eval(
        #         pl.element().join(
        #             # ...
        #         )
        #     )
        # )

        relational_df = relational_df.explode('all_coalesce_ids').join(
            all_selections.drop(join_key, strict=False),
            left_on='all_coalesce_ids',
            right_on='coalesce_id',
            how='left',
            # validate='m:1',
            coalesce=True,
        ).drop("all_coalesce_ids")
        relational_df = relational_df.group_by(join_key)


        value_column = column_renames[0]
        relational_df = relational_df.agg(
            pl.selectors.exclude(value_column).first(),  # restore the old
            pl.col(value_column).unique(),  # the new column
        )
        # value_column: list[list[Any]] -> list[Any]
        relational_df = relational_df.with_columns(
            pl.col(value_column).list.eval(
                pl.element().explode().drop_nulls()
            )
        )
        pass



    return relational_df, all_selections