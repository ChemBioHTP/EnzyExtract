import polars as pl

SKIP_VALIDATION = False

def expect_schema(df, schema):
    """
    Check if the DataFrame matches the expected schema.
    """
    if SKIP_VALIDATION:
        return True
    for col, dtype in schema.items():
        if col not in df.columns:
            raise pl.exceptions.ColumnNotFoundError(f"Missing column: {col}")
        if df[col].dtype != dtype:
            raise pl.exceptions.SchemaError(f"Column '{col}' has type {df[col].dtype}, expected {dtype}")
    return True

def expect_columns(df, cols):
    """
    Check if the DataFrame contains the expected columns.
    """
    if SKIP_VALIDATION:
        return True
    missing_cols = [col for col in cols if col not in df.columns]
    if missing_cols:
        raise pl.exceptions.ColumnNotFoundError(f"""Missing columns: {', '.join(missing_cols)}
Expected columns: {', '.join(cols)}
Actual columns: {', '.join(df.columns)}""")
    return True

def expect_not_columns(df, cols):
    """
    Check if the DataFrame does not contain the specified columns.
    """
    if SKIP_VALIDATION:
        return True
    unexpected_cols = [col for col in cols if col in df.columns]
    if unexpected_cols:
        raise pl.exceptions.SchemaError(f"""Unexpected columns: {', '.join(unexpected_cols)}""")
    return True

