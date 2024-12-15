from pydantic import BaseModel
from openai import OpenAI

from openai.lib._pydantic import to_strict_json_schema

from enzyextract.utils.construct_batch import to_openai_dict_message

client = OpenAI()

# goal: convert this schema to pydantic
# ```yaml
# data:
#     - descriptor: wild-type cat-1
#       substrate: H2O2
#       kcat: 1 min^-1
#       Km: null
#       kcat/Km: null
#     - descriptor: R190Q cat-1; 25°C
#       substrate: H2O2
#       kcat: 33 ± 0.3 s^-1
#       Km: "2.3 mM"
#       kcat/Km: null
#     - descriptor: R203Q cat-1; (with NADPH); 25°C
#       substrate: H2O2
#       kcat: null
#       Km: 9.9 ± 0.1 µM
#       kcat/Km: 4.4 s^-1 mM^-1
# context:
#     enzymes:
#         - fullname: catalase
#           synonyms: cat-1
#           mutants: wild-type; R190Q; R203Q
#           organisms: Escherichia coli
#     substrates: 
#         - fullname: hydrogen peroxide
#           synonyms: H2O2
#         - fullname: water
#     temperatures: 25°C; 30°C
#     pHs: 7.4
#     other: NADPH
# ```
from typing import List, Optional
from pydantic import BaseModel


class DataItem(BaseModel):
    descriptor: str
    substrate: str
    kcat: Optional[str]
    Km: Optional[str]
    kcat_Km: Optional[str]


class Enzyme(BaseModel):
    fullname: str
    synonyms: List[str]
    mutants: List[str]
    organisms: List[str]


class Substrate(BaseModel):
    fullname: str
    synonyms: List[str]


class Context(BaseModel):
    enzymes: List[Enzyme]
    substrates: List[Substrate]
    temperatures: List[str]
    pHs: list[str] # formerly float
    other: Optional[str]


class RootModel(BaseModel):
    data: List[DataItem]
    context: Context


def to_openai_batch_request_with_schema(uuid: str, system_prompt: str, docs: list[str], model_name='gpt-4o-mini', schema=None) -> dict:

    if schema is None:
        schema = RootModel
    if isinstance(docs, str):
        docs = [docs]
    messages = [to_openai_dict_message("system", system_prompt)] + [to_openai_dict_message("user", doc) for doc in docs]
    return {
        "custom_id": uuid,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            # This is what you would have in your Chat Completions API call
            "model": model_name,
            "temperature": 0,
            "messages": messages,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "strict": True,
                    "schema": to_strict_json_schema(schema),
                    "name": schema.__name__,
                }
            }
        },
    }
    completion = client.beta.chat.completions.parse(
        model="gpt-4o-2024-08-06",
        messages=[
            {"role": "system", "content": "You are an expert at structured data extraction. You will be given unstructured text from a research paper and should convert it into the given structure."},
            {"role": "user", "content": "..."}
        ],
        response_format=EnzyExtractOutput,
    )

    research_paper = completion.choices[0].message.parsed

if __name__ == "__main__":

    # Example instantiation of the model with the provided YAML structure
    example_data = {
        "data": [
            {
                "descriptor": "wild-type cat-1",
                "substrate": "H2O2",
                "kcat": "1 min^-1",
                "Km": None,
                "kcat_Km": None,
            },
            {
                "descriptor": "R190Q cat-1; 25°C",
                "substrate": "H2O2",
                "kcat": "33 ± 0.3 s^-1",
                "Km": "2.3 mM",
                "kcat_Km": None,
            },
            {
                "descriptor": "R203Q cat-1; (with NADPH); 25°C",
                "substrate": "H2O2",
                "kcat": None,
                "Km": "9.9 ± 0.1 µM",
                "kcat_Km": "4.4 s^-1 mM^-1",
            },
        ],
        "context": {
            "enzymes": [
                {
                    "fullname": "catalase",
                    "synonyms": ["cat-1"],
                    "mutants": ["wild-type", "R190Q", "R203Q"],
                    "organisms": ["Escherichia coli"],
                }
            ],
            "substrates": [
                {"fullname": "hydrogen peroxide", "synonyms": ["H2O2"]},
                {"fullname": "water", "synonyms": []},
            ],
            "temperatures": ["25°C", "30°C"],
            "pHs": [7.4],
            "other": "NADPH",
        },
    }

    root_model = RootModel(**example_data)
    print(root_model)
    
    print()
    print(RootModel.model_json_schema())

    exit(0)
    