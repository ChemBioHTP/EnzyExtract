import functools
import os
from typing import Optional
import polars as pl
from typing import Union, overload

from enzyextract.dependency.base import _load_asset

# Global registry for demo purposes
# TODO: implement graph (also consider: dagster)
DATA_REGISTRY = {
    "requires": [],
    "exports": []
}



def require(dest: str, to: Optional[str] = None, eager=True, instructions=None):
    """
    Dependency injection. Specify an explicit dependency.
    
    If "to" is provided, then a polars DataFrame (loaded from "dest") will be passed to the function
    as a keyword argument with the name "to". If "to" is not provided, then require() acts as
    a marker but no DataFrame will be passed on.
    """


    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if to and to not in kwargs:
                df = _load_asset(dest, eager=eager)
                kwargs[to] = df
            return func(*args, **kwargs)

        if not hasattr(wrapper, "_requires"):
            wrapper._requires = []
        wrapper._requires.append({"dest": dest, "name": to})
        DATA_REGISTRY["requires"].append({"function": func.__name__, "dest": dest, "name": to})
        return wrapper
    return decorator

def export(dest: str, autosave=False, cached=False):
    """
    Marks that a function should export data to a specific destination.
    Helps track dependencies.

    - autosave: If True, the dataframe is automatically saved after the function is executed.
    - cached: If True, and the destination already exists, then the dataframe is simply loaded.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if cached and os.path.exists(dest):
                print(f"Loading cached asset from {dest}")
                df = _load_asset(dest, eager=True)
                return df
            result = func(*args, **kwargs)
            if autosave:
                if isinstance(result, pl.DataFrame):
                    print(f"Autosaving to {dest}")
                    result.write_parquet(dest)
                else:
                    raise TypeError("Function decorated with @export must return a polars DataFrame")
            return result
        if not hasattr(wrapper, "_exports"):
            wrapper._exports = []
        wrapper._exports.append({"dest": dest, "autosave": autosave, "cached": cached})
        DATA_REGISTRY["exports"].append({"function": func.__name__, "dest": dest})
        return wrapper
    return decorator


if __name__ == "__main__":
    # example usage

    @require("data/brenda/brenda_to_ec.parquet", to="input_df")
    def my_func(hello, input_df=None):
        print(input_df)
        print(hello)
    
    my_func("Hello, World!") # input_df is injected


    # example where cannot be found
    @require("data/non_existent.parquet", to="input_df")
    def my_func(hello, input_df=None):
        print(input_df)
        print(hello)

    my_func("Hello, World!") # input_df is injected
