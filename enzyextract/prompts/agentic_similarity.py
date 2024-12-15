from typing import List, Optional
from pydantic import BaseModel

def enzyme_substrate_v0_0_0(es="enzyme"):
    return f"""
You are a helpful assistant familiar with many {es}s. 
You are asked to compare the names of two similar {es}s and determine if they are equivalent.

For each item, try to provide the full name of the {es} (inferring from an abbreviation if necessary).
Then, determine if the two items are equivalent, and provide a confidence score (0 to 1, where 1 is most confident).
"""



class SimilarityItem(BaseModel):
    item_number: int
    a_full_name: str
    b_full_name: str
    are_equivalent: bool
    confidence: float



class ESSimilaritySchema(BaseModel):
    items: List[SimilarityItem]