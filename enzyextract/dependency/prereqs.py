import functools
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



def require(dest: str, to: Optional[str] = None, eager=True):
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

def export(dest: str):
    """
    Marks that a function should export data to a specific destination.
    Helps track dependencies.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        if not hasattr(wrapper, "_exports"):
            wrapper._exports = []
        wrapper._exports.append({"dest": dest})
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
