import os
from typing import overload
import polars as pl

class DependencyNotFoundError(FileNotFoundError):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

@overload
def _load_asset(dest: str, eager: bool = True) -> pl.DataFrame: ...

@overload
def _load_asset(dest: str, eager: bool = False) -> pl.LazyFrame: ...
def _load_asset(dest: str, eager=True, optional=False):
    if not os.path.exists(dest):
        if optional:
            return None
        raise DependencyNotFoundError(
            f"EnzyExtract cannot find the specified dependency: {dest}. "
            "Please locate the script that produces it. \n"
            "(Hint: Ctrl+Shift+F @export. Future versions will offer automatic dependency resolution.)")

    if dest.endswith('.parquet'):
        if eager:
            return pl.read_parquet(dest)
        else:
            # Lazy loading
            return pl.scan_parquet(dest)
    raise ValueError(f"Unsupported file format for {dest}. Only .parquet files are supported.")