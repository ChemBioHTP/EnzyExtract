from pydantic import BaseModel


pick_uniprot = """
Given a target enzyme, rank the top 3 UniProt entries that best match the target enzyme and organism. \
Only report complete matches. If no such match exists, return null. Output your answer in JSON. \

# Example 1

**Input**

Target Enzyme: AAT-2
Target Fullname: aspartate transaminase
Target Organism: Rabbit

P12345: Aspartate aminotransferase, Oryctolagus cuniculus (Rabbit)
P13221: Aspartate aminotransferase, Rattus norvegicus (Rat)
P11111: Neck protein gp14, Bacteriophage T4

**Output**

1. P12345
2. null
3. null
"""


class PickAccessionSchema(BaseModel):
    thoughts_and_comments: str
    best: str | None 
    second_best: str | None
    third_best: str | None



pick_accessions = """
Given a target enzyme, rank the top 3 accessions that best match the target enzyme and organism. \
Accept synonyms. Only report complete matches. If no match exists, return null. Output your answer in JSON. \

# Example 1

**Input**

Target Enzyme: AAT-2
Target Fullname: aspartate transaminase
Target Organism: Rabbit

P12345: Aspartate aminotransferase, Oryctolagus cuniculus (Rabbit)
P13221: Aspartate aminotransferase, Rattus norvegicus (Rat)
P11111: Neck protein gp14, Bacteriophage T4

**Output**

1. P12345
2. null
3. null
"""

class PickAccessionSchemaAdditional(BaseModel):
    thoughts_and_comments: str
    best: str | None 
    second_best: str | None
    third_best: str | None
    additional: list[str] | None


