from dataclasses import dataclass
from typing import Literal, Optional


persistent_input = None
def get_user_y_n():
    global persistent_input
    if persistent_input is True:
        inp = 'y'
    elif persistent_input is False:
        inp = 'n'
    else:
        inp = input("Proceed? (y/n): ")
    if inp == 'Y':
        persistent_input = True
    elif inp == 'N':
        persistent_input = False
    return inp

@dataclass
class LLMCommonBatch:
    """
    Common interface.
    See openai.types.batch.py at https://github.com/openai/openai-python
    """
    endpoint: str
    
    _underlying: None
    status: Literal[
        "validating", "failed", "in_progress", "finalizing", "completed", "expired", "cancelling", "cancelled"
    ] = None
    output_file_id: Optional[str] = None
    error_file_id: Optional[str] = None
    

