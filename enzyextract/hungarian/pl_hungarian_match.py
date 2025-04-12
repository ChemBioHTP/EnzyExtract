import polars as pl
import polars.selectors as cs
import numpy as np
from scipy.optimize import linear_sum_assignment


def calculate_similarity_matrix(df1: pl.DataFrame, df2: pl.DataFrame, objective_fn):
    """
    Calculate the similarity matrix between two dataframes using the given objective function.
    Tries to maximize the objective.

    Parameters:
    df1 (pl.DataFrame): The first dataframe.
    df2 (pl.DataFrame): The second dataframe.
    objective_fn (function): The objective function that takes two rows and returns a similarity score.
    It shouuld take in 2 parametrs: row1 and row2. 
        row1 comes from df1, and is a tuple with each column in sequential order.
        row2 comes from df2.
    
    """
    n, m = len(df1), len(df2)
    similarity_matrix = np.zeros((n, m))
    
    for i, df1row in enumerate(df1.iter_rows(named=True)):
        for j, df2row in enumerate(df2.iter_rows(named=True)):
            similarity = objective_fn(df1row, df2row)
            similarity_matrix[i, j] = similarity
    
    return similarity_matrix

def assign_optimally(df1: pl.DataFrame, df2: pl.DataFrame, objective_fn, objective_column=None, maximize=True):
    """
    Optimally solves the assignment problem between two dataframes, given the objective function.
    Tries to maximize the objective.

    Returns a dataframe, with these columns: index_1 and index_2, which are the indices of the matched rows in df1 and df2 respectively.
    Hint: it may be useful to have a row index to make sense of the indices. You can use 2 joins afterwards to then combine df1 and df2 together.

    Parameters:
    df1 (pl.DataFrame): The first dataframe.
    df2 (pl.DataFrame): The second dataframe.
    objective_fn (function): The objective function that takes two rows and returns a similarity score.
    It should take in 2 parameters: row1 and row2.
        row1 comes from df1, and is a tuple with each column in sequential order.
        row2 comes from df2.
    objective_column (str): If specified, then the value of the objective function will be deposited into this column.
    """
    
    similarity_matrix = calculate_similarity_matrix(df1, df2, objective_fn)
    
    # Pad the similarity matrix if the dataframes have different sizes
    n, m = similarity_matrix.shape
    if n > m:
        padding = np.zeros((n, n - m))
        similarity_matrix = np.hstack((similarity_matrix, padding))
    elif m > n:
        padding = np.zeros((m - n, m))
        similarity_matrix = np.vstack((similarity_matrix, padding))
    
    row_ind, col_ind = linear_sum_assignment(similarity_matrix, maximize=maximize)

    # construct df
    larger_n = max(len(row_ind), len(col_ind))
    matches_dict = {
        'index_1': pl.Series(row_ind, dtype=pl.UInt32).extend_constant(None, larger_n - len(row_ind)),
        'index_2': pl.Series(col_ind, dtype=pl.UInt32).extend_constant(None, larger_n - len(col_ind))
    }

    # Add objective values if requested
    if objective_column is not None:
        objective_values = []
        for i, j in zip(row_ind, col_ind):
            # if i < n and j < m:  # Only get values from original similarity matrix
            objective_values.append(similarity_matrix[i, j])
            # else:
                # objective_values.append(None)
        
        # Pad objective values to match the larger size
        objective_values.extend([None] * (larger_n - len(objective_values)))
        matches_dict[objective_column] = pl.Series(objective_values)
    
    return pl.DataFrame(matches_dict)

def join_optimally(df1: pl.DataFrame, df2: pl.DataFrame, objective_fn, 
                   partition_by=None, how='inner', progress_bar=False, maximize=True, **kwargs):
    """
    Join in a SQL-like fashion, but by maximizing an objective function to find assignments. 

    The result: a dataframe;
    df1 will be suffixed with _1
    df2 will be suffixed with _2

    Parameters:
    df1 (pl.DataFrame): The first dataframe.
    df2 (pl.DataFrame): The second dataframe.
    objective_fn (function): The objective function that takes two rows and returns a similarity score.
    It shouuld take in 2 parametrs: row1 and row2. 
        row1 comes from df1, and is a tuple with each column in sequential order.
        row2 comes from df2.
    partition_by (str or list(str)): The column(s) to horizontally partition by. If None, then no partitioning is done.
        This helps keep n down, which is important for the Hungarian algorithm.
    how (str): Only significant if partition_by is specified. 
        Given a unique value of tuple, as specified by partition_by:
        Inner: only proceed if that tuple is present in both dataframes.
        Left: keep all rows from the left dataframe, and matching in rows from the right dataframe
        Right: keep all rows from the right dataframe, and matching in rows from the left dataframe.
        Outer: keep all rows from both dataframes, matching in rows where possible.
    objective_column (str): If specified, then the value of the objective function will be deposited into this column.
    """

    if partition_by is None:
        matches = assign_optimally(df1, df2, objective_fn, **kwargs)
        
        
        df1 = df1.rename({col: f"{col}_1" for col in df1.columns})
        df1 = df1.with_row_index('_index_1')
        
        df2 = df2.rename({col: f"{col}_2" for col in df2.columns})
        df2 = df2.with_row_index('_index_2')
        
        matches = matches.join(df1, left_on='index_1', right_on='_index_1', how='full')
        matches = matches.join(df2, left_on='index_2', right_on='_index_2', how='full').select(cs.exclude('_index_1', '_index_2'))
        return matches
    else:
        if isinstance(partition_by, str):
            partition_by = [partition_by]
        
        # we need to retain the dtypes
        for pname in partition_by:
            if pname not in df1.columns:
                raise ValueError(f"Partition column {pname} not found in df1")
            if pname not in df2.columns:
                raise ValueError(f"Partition column {pname} not found in df2")
        
        # combine the dataframes into one by concat diagonal
        
        # add _1 suffix to all columns in df1
        df1_columns = [x for x in df1.columns if x not in partition_by]
        df2_columns = [x for x in df2.columns if x not in partition_by]
        df1 = (
            df1.rename({col: f"{col}_1" for col in df1_columns})
            .with_columns([
                pl.lit(True).alias('from_df1'),
            ])
        )
        # add _2 suffix to all columns in df2
        df2 = (
            df2.rename({col: f"{col}_2" for col in df2_columns})
            .with_columns([
                pl.lit(False).alias('from_df1'),
            ])
        )

        # join the dataframes
        df = pl.concat([df1, df2], how='diagonal')

        builder = []
        _gen = df.partition_by(partition_by, as_dict=True).items()
        if progress_bar:
            from tqdm import tqdm
            _gen = tqdm(_gen)
        for partition_value, subset in _gen:
            
            # get the filter, and also remove the suffix
            df1 = (
                subset.filter(pl.col('from_df1'))
                .select([f'{col}_1' for col in df1_columns])
            )
            if df1.is_empty():
                if how in ['right', 'outer']:
                    builder.append(subset.select(cs.exclude('from_df1')).select(*partition_by, cs.exclude(partition_by)))
                continue
            df1orig = df1.rename({f'{col}_1': col for col in df1_columns})
            df1 = df1.with_row_index('_hungarian_index')

            df2 = (
                subset.filter(~pl.col('from_df1'))
                .select([f'{col}_2' for col in df2_columns])
            )
            if df2.is_empty():
                if how in ['outer', 'left']:
                    builder.append(subset.select(cs.exclude('from_df1')).select(*partition_by, cs.exclude(partition_by)))
                continue
            df2orig = df2.rename({f'{col}_2': col for col in df2_columns})
            df2 = df2.with_row_index('_hungarian_index')
            

            matches = assign_optimally(df1orig, df2orig, objective_fn, maximize=maximize, **kwargs)
            matches = matches.join(df1, left_on='index_1', right_on='_hungarian_index', how='full')

            matches = (
                matches.join(df2, left_on='index_2', right_on='_hungarian_index', how='full')
                .select(cs.exclude('index_1', 'index_2', '_hungarian_index', '_hungarian_index_right'))

                # add back in the partition columns
                .with_columns([
                    pl.lit(pvalue, dtype=df.schema[pname]).alias(pname)
                    for pname, pvalue in zip(partition_by, partition_value)
                ])
                .select(*partition_by, cs.exclude(partition_by))
            )
            builder.append(matches)
        return pl.concat(builder)



def test_join_optimally_basic():
    """Test basic functionality of join_optimally"""
    df1 = pl.DataFrame({
        'id': [1, 2, 3],
        'value': ['A', 'B', 'C']
    })
    df2 = pl.DataFrame({
        'id': [2, 1, 4],
        'score': [90, 85, 95]
    })
    
    def obj_fn(row1, row2):
        return -abs(row1['id'] - row2['id'])
    
    result = join_optimally(df1, df2, obj_fn)
    
    # Check that the result has the expected structure
    print(result)

def test_join_optimally_with_partition():
    """Test partitioned joining"""
    df1 = pl.DataFrame({
        'group': ['A', 'A', 'B'],
        'value': [70, 100, 3]
    })
    df2 = pl.DataFrame({
        'group': ['A', 'A', 'B'],
        'score': [90, 85, 95]
    })
    
    def obj_fn(row1, row2):
        return -abs(row1['value'] - row2['score'])  # Compare value and score
    
    result = join_optimally(df1, df2, obj_fn, partition_by='group')
    
    print(result)
    assert 'group' in result.columns
    # Check that we maintain the partition integrity
    groups = result.select('group').unique()
    assert len(groups) == 2  # We should have both A and B groups

def test_join_uneven_inner():
    """Test inner join behavior with uneven groups"""
    df1 = pl.DataFrame({
        'group': ['A', 'A', 'B', 'C'],
        'value': [1, 2, 3, 4]
    })
    df2 = pl.DataFrame({
        'group': ['A', 'B', 'B', 'D'],
        'score': [10, 20, 30, 40]
    })
    
    def obj_fn(row1, row2):
        return -abs(row1['value'] - row2['score'])
    
    expected = {
        'outer': {'A', 'B', 'C', 'D'},
        'inner': {'A', 'B'},
        'left': {'A', 'B', 'C'},
        'right': {'A', 'B', 'D'},
    }
    for how, groups in expected.items():
        result = join_optimally(df1, df2, obj_fn, partition_by='group', how=how)
        print(f"\n[UNEVEN] {how}:")
        print(result)

def test_join_optimally_left():
    """Test left join behavior with uneven groups"""
    df1 = pl.DataFrame({
        'group': ['A', 'A', 'B', 'C'],
        'value': [1, 2, 3, 4]
    })
    df2 = pl.DataFrame({
        'group': ['A', 'B', 'D'],
        'score': [10, 20, 40]
    })
    
    def obj_fn(row1, row2):
        return -abs(row1['value'] - row2['score'])
    
    result = join_optimally(df1, df2, obj_fn, partition_by='group', how='left')
    print("\nLeft join result:")
    print(result)
    
    # Should include all groups from df1
    assert set(result['group'].unique().to_list()) == {'A', 'B', 'C'}
    
    # Check that we have all rows from df1
    assert len(result) >= len(df1)

def test_join_optimally_right():
    """Test right join behavior with uneven groups"""
    df1 = pl.DataFrame({
        'group': ['A', 'B', 'C'],
        'value': [1, 2, 3]
    })
    df2 = pl.DataFrame({
        'group': ['A', 'A', 'B', 'D'],
        'score': [10, 20, 30, 40]
    })
    
    def obj_fn(row1, row2):
        return -abs(row1['value'] - row2['score'])
    
    result = join_optimally(df1, df2, obj_fn, partition_by='group', how='right')
    print("\nRight join result:")
    print(result)
    
    # Should include all groups from df2
    assert set(result['group'].unique().to_list()) == {'A', 'B', 'D'}
    
    # Check that we have all rows from df2
    assert len(result) >= len(df2)

def test_join_optimally_how():
    """Test outer join behavior with uneven groups"""
    df1 = pl.DataFrame({
        'group': ['A', 'B', 'C'],
        'value': [1, 2, 3]
    })
    df2 = pl.DataFrame({
        'group': ['A', 'B', 'D'],
        'score': [10, 20, 40]
    })
    
    def obj_fn(row1, row2):
        return -abs(row1['value'] - row2['score'])
    
    expected = {
        'outer': {'A', 'B', 'C', 'D'},
        'inner': {'A', 'B'},
        'left': {'A', 'B', 'C'},
        'right': {'A', 'B', 'D'},
    }
    for how, groups in expected.items():
        result = join_optimally(df1, df2, obj_fn, partition_by='group', how=how)
        print(f"\n[HOW] {how}:")
        print(result)
    
        # Should include all groups from both dataframes
        assert set(result['group'].unique().to_list()) == groups

def test_join_optimally_uneven_rows():
    """Test joining with uneven number of rows within groups"""
    df1 = pl.DataFrame({
        'group': ['A', 'A', 'A', 'B'],
        'value': [1, 2, 3, 4]
    })
    df2 = pl.DataFrame({
        'group': ['A', 'A', 'B', 'B'],
        'score': [10, 20, 30, 40]
    })
    
    def obj_fn(row1, row2):
        return -abs(row1['value'] - row2['score'])
    
    # Test all join types with uneven rows
    joins = ['inner', 'left', 'right', 'outer']
    
    for join_type in joins:
        result = join_optimally(df1, df2, obj_fn, partition_by='group', how=join_type)
        print(f"\nUneven rows - {join_type} join result:")
        print(result)
        
        if join_type == 'inner':
            assert set(result['group'].unique().to_list()) == {'A', 'B'}
        elif join_type == 'left':
            assert len(result.filter(pl.col('group') == 'A')) >= 3  # All rows from df1 for group A
        elif join_type == 'right':
            assert len(result.filter(pl.col('group') == 'B')) >= 2  # All rows from df2 for group B
        elif join_type == 'outer':
            assert set(result['group'].unique().to_list()) == {'A', 'B'}

def test_join_optimally_empty_group():
    """Test joining with an empty group in one dataframe"""
    df1 = pl.DataFrame({
        'group': ['A', 'B'],
        'value': [1, 2]
    })
    df2 = pl.DataFrame({
        'group': ['A'],
        'score': [10]
    })
    
    def obj_fn(row1, row2):
        return -abs(row1['value'] - row2['score'])
    
    # Test all join types with empty group
    joins = ['inner', 'left', 'right', 'outer']
    
    for join_type in joins:
        result = join_optimally(df1, df2, obj_fn, partition_by='group', how=join_type)
        print(f"\nEmpty group - {join_type} join result:")
        print(result)
        
        if join_type == 'inner':
            assert set(result['group'].unique().to_list()) == {'A'}
        elif join_type == 'left':
            assert set(result['group'].unique().to_list()) == {'A', 'B'}
        elif join_type == 'right':
            assert set(result['group'].unique().to_list()) == {'A'}
        elif join_type == 'outer':
            assert set(result['group'].unique().to_list()) == {'A', 'B'}
# Additional test cases that would be useful to add:
# - Test with empty dataframes
# - Test with different join types (inner, left, right, outer)
# - Test with multiple partition columns
# - Test with various objective functions (cosine similarity, custom metrics)
# - Test edge cases in similarity calculations
# - Test with different data types (strings, dates, etc.)
if __name__ == '__main__':
    test_join_optimally_basic()
    test_join_optimally_with_partition()

    test_join_optimally_how()
    test_join_uneven_inner()
    test_join_optimally_left()
    test_join_optimally_right()
    
    test_join_optimally_uneven_rows()
    test_join_optimally_empty_group()
#     test_join_optimally_empty_df()
    print("All tests passed!")