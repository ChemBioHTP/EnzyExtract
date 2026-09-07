import polars as pl


def wrap_pbar(pbar, func):
    def foo(*args, **kwargs):
        pbar.update(1)
        return func(*args, **kwargs)

    return foo


def str_dynamic_replace(haystack_col: str, needle_col: str, replacement_value: str) -> pl.Expr:
    """
    Replace values in a column based on corresponding values in another column.
    """
    def replace_needle_with_replacement(struct: dict):
        haystack = struct[haystack_col]
        needle = struct[needle_col]
        return haystack.replace(needle, replacement_value)

    return pl.struct(
        haystack_col,
        needle_col,
    ).map_elements(
        replace_needle_with_replacement,
        return_dtype=pl.Utf8
    )