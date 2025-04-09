from dataclasses import dataclass
from enum import Enum
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

class SubmitConsent(Enum):
    """
    Consent to submit batch. 
    """

    SUBMIT = "yes"
    YES = "yes"
    """Submit batch, track in llm_log, keep local copy"""

    LOCAL = "no"
    """Do not submit batch, track in llm_log, keep local copy"""

    UNTRACK = "untrack"
    """Do not submit batch, ignore in llm_log, keep local copy"""

    REMOVE = "remove"
    """Do not submit batch, ignore in llm_log, delete local copy"""

persist_state = {}

def get_user_submit_consent() -> SubmitConsent:
    """
    Get user consent to submit batch.
    """
    global persist_state
    persist_key = 'submit_to_llm'
    if persist_key in persist_state:
        return persist_state[persist_key]
    
    user_advice = "Proceed? ([y]es, [l]ocal, [u]ntrack, [r]emove, [h]elp): "
    inp = input(user_advice).lower()
    match inp:
        case 'Y':
            persist_state[persist_key] = SubmitConsent.SUBMIT
            return SubmitConsent.SUBMIT
        case 'y' | 'yes':
            return SubmitConsent.SUBMIT
        case 'N':
            persist_state[persist_key] = SubmitConsent.LOCAL
            return SubmitConsent.LOCAL
        case 'n' | 'no' | 'l' | 'local':
            return SubmitConsent.LOCAL
        case 'u' | 'untrack':
            return SubmitConsent.UNTRACK
        case 'r' | 'remove' | 'd' | 'delete':
            return SubmitConsent.REMOVE
        case _:
            print("Help:")
            print("y: Submit batch to LLM provider and track in llm_log.")
            print("l: Do not submit batch, but save locally and track in llm_log.")
            print("u: Do not submit batch, ignore in llm_log, keep local copy.")
            print("r: Do not submit batch, ignore in llm_log, delete local copy.")
            print("h: Show this help message.")
            return get_user_submit_consent()

def do_presubmit(
    *, 
    filepath: str = None, 
    count: int = None,
    submit_suffix: str = 'Submit to OpenAI?',
    
    ):
    """couple of quality of life checks before submission."""

    # read to make sure
    if count is not None:
        print(f"Batch of {count} items ready for submission. {submit_suffix}")
    elif filepath is not None:
        assert filepath.endswith('.jsonl')
        with open(filepath, 'r') as f:
            count = sum(1 for _ in f)
        print(f"Batch of {count} items at {filepath} ready for submission. {submit_suffix}")
    else:
        raise ValueError("Must provide either filepath or count.")

    return get_user_submit_consent()

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
    

