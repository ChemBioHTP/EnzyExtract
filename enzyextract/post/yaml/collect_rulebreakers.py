from typing import Callable, Optional
import polars as pl

from enzyextract.post.yaml.schemas import rulebreakers_schema

# def df_wide_to_long(df: pl.DataFrame) -> pl.DataFrame:
#     """
#     Convert a wide DataFrame to a long format.
#     Which uses unpivot.
#     """

#     of_strs = df.unpivot(
#         cs.string(),
#         variable_name="key",
#         value_name="value_str",
#     )

#     of_nums = df.unpivot(
#         cs.numeric(),
#         variable_name="key",
#         value_name="value_num",
#     )

#     of_str_lists = df.unpivot(
#         cs.by_dtype(pl.List(pl.Utf8)),
#         variable_name="key",
#         value_name="value_list_str",
#     )

def record_wide_to_long(
        obj: dict, 
        record_id: str) -> list[dict]:
    """
    Convert record from wide to long format.
    Similar to unpivot in polars.

    obj: the object
    record_id: the record id
    parent_id: the parent id, if there is nesting
    """
    assert isinstance(obj, dict), "Input must be a dictionary"

    self_id = 0
    def generate_self_id():
        nonlocal self_id
        self_id += 1
        return self_id
    return _record_wide_to_long(obj, record_id, 0, '', generate_self_id)

def _primitive_wide_to_long(
    value: str | int | float | bool,
    row_base: dict
) -> Optional[dict]:
    if value is None:
        return row_base
    elif isinstance(value, bool):
        return {
            **row_base,
            'value_bool': value,
        }
    elif isinstance(value, int):
        return {
            **row_base,
            'value_int': value,
        }
    elif isinstance(value, float):
        return {
            **row_base,
            'value_num': value,
        }
    elif isinstance(value, str):
        return {
            **row_base,
            'value_str': value,
        }
    return None
    

def _record_wide_to_long(
    obj: dict, 
    record_id: str,
    self_id: int,
    key_prefix: str,
    generate_id: Callable[[], int]) -> list[dict]:

    collector = []
    base = {
        'record_id': record_id,
        'self_id': self_id,
    }

    for key, value in obj.items():
        row_base = {
            **base,
            'key': key_prefix + key,
        }
        prim_row = _primitive_wide_to_long(value, row_base)
        if prim_row is not None:
            collector.append(prim_row)
            continue
        
        # non-primitive types (list, dict)
        if isinstance(value, dict):
            child_id = generate_id()
            collector.append({
                **row_base,
                'value_dict_ref': child_id
            })
            collector.extend(
                _record_wide_to_long(
                    value,
                    record_id,
                    self_id=child_id,
                    key_prefix=key_prefix + key + '.',
                    generate_id=generate_id
                )
            )
        elif isinstance(value, list):
            collector.extend(
                _het_list_wide_to_long(
                    value,
                    record_id,
                    self_id,
                    key_prefix + key,
                    generate_id=generate_id
                )
            )
        else:
            # unknown type
            raise TypeError(f"Unknown type: {type(value)}")
    return collector

def _het_list_wide_to_long(
    value: list,
    record_id: str,
    self_id: int,
    key: str,
    generate_id
):
    
    collector = []
    row_base = {
        'record_id': record_id,
        'self_id': self_id,
        'key': key,
    }
    # check for homogenous list
    all_numeric = all(x is None or isinstance(x, (float, int)) for x in value)
    all_string = all(x is None or isinstance(x, str) for x in value)
    all_dict = all(x is None or isinstance(x, dict) for x in value)
    
    # now manage lists
    if all_numeric:
        collector.append({
            **row_base,
            'value_list_num': value,
        })
        return collector
    elif all_string:
        collector.append({
            **row_base,
            'value_list_str': value,
        })
        return collector

    # In general, (ie. list of dicts or heterogeneous data)
    # create references for dicts
    # otherwise, add heterogeneous data as a row with key

    # use refs ONLY for dicts (primitives get added as rows)
    # refer to list elements as children
    child_refs = []
    children_records = []
    for idx, item in enumerate(value):
        list_row_base = {
            **row_base,
            'key': key + f'[{idx}]',
        }
        # check for primitives
        prim_row = _primitive_wide_to_long(item, list_row_base)
        if prim_row is not None:
            child_refs.append(None)
            children_records.append(prim_row)
            continue

        # non-primitive types (list, dict)
        if isinstance(item, list):
            child_id = generate_id()
            child_refs.append(child_id)
            children_records.extend(
                _het_list_wide_to_long(
                    item,
                    record_id,
                    self_id=child_id,
                    key=key + f'[{idx}]',
                    generate_id=generate_id
                )
            )
        elif isinstance(item, dict):
            child_id = generate_id()
            child_refs.append(child_id)
            children_records.extend(
                _record_wide_to_long(
                    item,
                    record_id,
                    self_id=child_id,
                    key_prefix=key + f'[{idx}].',
                    generate_id=generate_id
                )
            )
    collector.append({
        **row_base,
        'value_list_ref': child_refs
    })
    collector.extend(children_records)
    return collector
            

            
def record_wide_to_long_df(
    obj: dict, 
    record_id: str) -> pl.DataFrame:
    """
    Convert record from wide to long format.
    Similar to unpivot in polars.

    obj: the object
    record_id: the record id
    parent_id: the parent id, if there is nesting
    """
    out = record_wide_to_long(obj, record_id)
    return pl.DataFrame(out, schema=rulebreakers_schema)

def records_wide_to_long_df(
    obj: list[dict], 
    record_id: str) -> pl.DataFrame:
    """
    Convert record from wide to long format.
    Similar to unpivot in polars.

    obj: the object
    record_id: the record id
    parent_id: the parent id, if there is nesting
    """
    out = []
    for i, item in enumerate(obj):
        out.extend(record_wide_to_long(item, f"{record_id}_{i}"))
    return pl.DataFrame(out, schema=rulebreakers_schema)

if __name__ == "__main__":
    # Test the function with a sample dictionary
    sample_dict = {
        "name": "John",
        "age": 30,
        "address": {
            "street": "123 Main St",
            "city": "New York"
        },
        "hobbies": ["reading", "gaming"],
        "scores": [95, 85, 90],
        "nested_list": [
            {"item": "apple", "quantity": 5},
            {"item": "banana", "quantity": 10}
        ]
    }
    res = record_wide_to_long_df(sample_dict, 'test')
    print(res)
