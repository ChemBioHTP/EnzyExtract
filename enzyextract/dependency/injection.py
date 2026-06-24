import functools
from typing import Literal, Optional, overload
import polars as pl
import inspect

from enzyextract.dependency.base import DependencyNotFoundError, _load_asset

class InjectedData:
    def __init__(self, path: str, eager=True, optional=False):
        self.path = path
        self.eager = eager
        self.optional = optional
    def load(self):
        return _load_asset(self.path, eager=self.eager, optional=self.optional)


@overload
def REQUIRE(fpath: str) -> pl.DataFrame: ...
@overload
def REQUIRE(fpath: str, *, eager: Literal[True]) -> pl.DataFrame: ...
@overload
def REQUIRE(fpath: str, *, eager: Literal[False]) -> pl.LazyFrame: ...
def REQUIRE(fpath: str, *, eager=True):
    """
    Use `REQUIRE("some/path.parquet")` to specify a resource
    to be injected. When combined with @resolve, 
    you can treat this as magically producing a polars DataFrame.
    
    Technically, this will return an `InjectedData` object (or similar
    marker) that the main `@resolve` decorator will later resolve to a polars DataFrame.

    Bit of magic: actually, this function is "incorrectly" type-hinted to return a pl.DataFrame,
    when really it returns an InjectedData. However, the @inject decorator is intended to 
    materialize those InjectedData instances into polars DataFrames.

    Usage example:
    ```python
    @resolve
    def my_func(
        hello, 
        input_df=REQUIRE("data/brenda/brenda_to_ec.parquet")
    ):
        print(input_df)
        print(hello)
    
    my_func("Hello, World!") # input_df is injected
    """
    if isinstance(fpath, str):
        # If the function is actually a string, return an InjectedData instance
        return InjectedData(fpath, eager=eager, optional=False)


def OPTIONAL(fpath: str, *, eager=True) -> Optional[pl.DataFrame]:
    """
    See the REQUIRE macro, but this one is optional. Will inject None if the file does not exist.
    """
    if isinstance(fpath, str):
        return InjectedData(fpath, eager=eager, optional=True)

def resolve(func):
    """
    Dynamically determine dependencies, based on the REQUIRE macro.
    """
    sig = inspect.signature(func)
    defaults = {
        k: v.default
        for k, v in sig.parameters.items()
        if isinstance(v.default, InjectedData)
    }

    not_found = []
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        bound = sig.bind_partial(*args, **kwargs)
        for name, dep in defaults.items():
            if name not in bound.arguments:
                try:
                    kwargs[name] = dep.load()
                except DependencyNotFoundError:
                    not_found.append(dep.path)
        if not_found:
            raise DependencyNotFoundError(
                f"EnzyExtract cannot find the specified dependencies: {{{', '.join(not_found)}}}. "
                "Please locate the script(s) that produces them. "
                "(Hint: Ctrl+Shift+F @export. Future versions will offer automatic dependency resolution.)"
            )
        return func(*args, **kwargs)
    
    # Attach introspection metadata
    wrapper._has_injected_deps = True
    return wrapper

def introspect_dependencies(fn):
    """
    Detects dependencies of a function (of the )
    """
    sig = inspect.signature(fn)
    deps = {
        k: v.default.path
        for k, v in sig.parameters.items()
        if isinstance(v.default, InjectedData)
    }
    return deps

if __name__ == "__main__":
    # Example usage

    @resolve
    def my_func(
        hello, 
        input_df=REQUIRE("data/brenda/brenda_to_ec.parquet")
    ):
        print(input_df)
        print(hello)
    
    my_func("Hello, World!") # input_df is injected

    print(introspect_dependencies(my_func)) # {'input_df': 'data/brenda/brenda_to_ec.parquet'}


    @resolve
    def another_func(
        hello, 
        input_df=REQUIRE("data/non_existent.parquet"),
        another_df=REQUIRE("data/another_non_existent.parquet", eager=False)
    ):
        pass

    # another_func("Hello, World!")  # This will raise DependencyNotFoundError for both dependencies