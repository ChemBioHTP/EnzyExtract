from enum import Enum
from typing import List
from pydantic import BaseModel


abbr_v0_0_0 = f"""
You are a helpful assistant and scientific expert. 
You are asked to extract the abbreviations and full names specified by a body of text.
Then, categorize the abbreviations into exactly one of the following categories:
protein, substrate, organism, other
"""

class AbbreviationCategory(str, Enum):
    protein = 'protein'
    substrate = 'substrate'
    organism = 'organism'
    other = 'other'

class AbbreviationItem(BaseModel):
    abbreviation: str
    full_name: str
    category: AbbreviationCategory

class AbbreviationsSchema(BaseModel):
    items: List[AbbreviationItem]