from typing import overload
import polars as pl

@overload
def _load_asset(dest: str, eager: bool = True) -> pl.DataFrame: ...

@overload
def _load_asset(dest: str, eager: bool = False) -> pl.LazyFrame: ...
def _load_asset(dest: str, eager=True):
    if dest.endswith('.parquet'):
        if eager:
            return pl.read_parquet(dest)
        else:
            # Lazy loading
            return pl.scan_parquet(dest)
    raise ValueError(f"Unsupported file format for {dest}. Only .parquet files are supported.")