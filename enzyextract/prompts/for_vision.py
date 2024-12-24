from pydantic import BaseModel


for_vision = """
Given a table image, identify and correct mistakes in the yaml. 
Only correct these mistakes: 
1. Identify any differences in the Km, kcat, and kcat/Km units
2. Identify any mistakes in the use of scientific notation
3. Identify instances where ± is encoded as a number, like 5.060.8 instead of 5.0 ± 0.8
Finally, provide your final answer. If the yaml is all correct, simply respond "All correct". Otherwise, provide the yaml with mistakes corrected. 
To express scientific notation, write "× 10^n". Not all tables need scientific notation. Do NOT add fields to the schema. Do NOT change the descriptor.

Your steps should fit within a JSON schema.
{
"unit_step": "..."
"scientific_notation_step": "..."
"plus_minus_step": "..."
"final_answer": "..."
}
"""


class VisionCorrectionSchema(BaseModel):
    unit_step: str
    scientific_notation_step: str
    # substrates_step: str
    plus_minus_step: str
    final_answer: str



cls_vision = """
You are a helpful assistant that helps extract key information from images of tables.
Only extract these features: 
1. Identify whether the tables mention Km, kcat (turnover number), and kcat/Km. Ignore Vmax, Kd, Ki, etc.
1. Identify all units that the tables use for Km, kcat, and kcat/Km. Ignore units for all other parameters. 
2. If the table explicitly uses scientific notation, \
identify the UNIQUE exponents (× 10^n) for each parameter. THIS MAY BE BLANK.
Your response should fit within a JSON schema.
"""


class VisionClsSchema(BaseModel):
    has_km: bool
    has_kcat: bool
    has_kcat_km: bool

    km_units: list[str] | None
    kcat_units: list[str] | None
    kcat_km_units: list[str] | None

    km_exponents: list[str] | None
    kcat_exponents: list[str] | None
    kcat_km_exponents: list[str] | None

