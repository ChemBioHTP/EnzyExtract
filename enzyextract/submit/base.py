from dataclasses import dataclass
from enum import Enum
import os
from typing import Literal, Optional, Union
from colorama import just_fix_windows_console, Fore, Style

just_fix_windows_console()
persistent_input = None
def get_user_y_n():
    global persistent_input
    if persistent_input is True:
        inp = 'y'
    elif persistent_input is False:
        inp = 'n'
    else:
        inp = input(f"{Fore.GREEN}Proceed?{Style.RESET_ALL} (y/n): ")
    if inp == 'Y':
        persistent_input = True
    elif inp == 'N':
        persistent_input = False
    return inp

class SubmitPreference(Enum):
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

def get_user_submit_consent() -> SubmitPreference:
    """
    Get user consent to submit batch.
    """
    global persist_state
    persist_key = 'submit_to_llm'
    if persist_key in persist_state:
        return persist_state[persist_key]
    
    user_advice = f"{Fore.GREEN}Proceed?{Style.RESET_ALL} ([y]es, [l]ocal, [u]ntrack, [r]emove, [h]elp): "
    inp = input(user_advice).lower()
    match inp:
        case 'Y':
            persist_state[persist_key] = SubmitPreference.SUBMIT
            return SubmitPreference.SUBMIT
        case 'y' | 'yes':
            return SubmitPreference.SUBMIT
        case 'N':
            persist_state[persist_key] = SubmitPreference.LOCAL
            return SubmitPreference.LOCAL
        case 'n' | 'no' | 'l' | 'local' | 'a' | 'abort':
            return SubmitPreference.LOCAL
        case 'u' | 'untrack':
            return SubmitPreference.UNTRACK
        case 'r' | 'remove' | 'd' | 'delete':
            return SubmitPreference.REMOVE
        case _:
            print("Help:")
            print("y: Submit batch to LLM provider and track in llm_log.")
            print("l: Do not submit batch, but save locally and track in llm_log.")
            print("u: Do not submit batch, ignore in llm_log, keep local copy.")
            print("r: Do not submit batch, ignore in llm_log, delete local copy.")
            print("a: Abort. Same as local.")
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
    

class ReusePreference(Enum):
    """
    Preference for whether an existing batch should be reused or overwritten.
    """
    REUSE = "reuse"
    """Reuse existing batch. Do not recalculate anything."""

    REUSE_AS_NEEDED = "reuse_as_needed"
    """Recalculate everything, but do not overwrite"""

    OVERWRITE = "overwrite"
    """Overwrite existing batch."""

    ABORT = "abort"
    """Abort submission."""

def get_user_reuse_preference() -> ReusePreference:
    """
    Get user preference for whether an existing batch should be reused or overwritten.
    """
    inp = input(f"{Fore.GREEN}Reuse existing batch?{Style.RESET_ALL} ([r]euse, [o]verwrite, [a]bort): ").lower()
    match inp:
        case 'r' | 'reuse':
            return ReusePreference.REUSE
        case 'reuse_as_needed':
            return ReusePreference.REUSE_AS_NEEDED
        case 'o' | 'overwrite':
            return ReusePreference.OVERWRITE
        case 'a' | 'abort':
            return ReusePreference.ABORT
        case _:
            print("Invalid input. Please enter 'r', 'o', or 's'.")
            return get_user_reuse_preference()

def check_file_destinations_and_ask(
    fpaths: list[str],
):
    """
    Check if file destinations already exist, and if so, ask the user if they want to overwrite or reuse.
    """
    existing = []
    for fpath in fpaths:
        if os.path.exists(fpath):
            existing.append(fpath)
    if existing:
        print(f"Files that already exist: {existing}")
        pref = get_user_reuse_preference()
        return pref
    return ReusePreference.OVERWRITE

class VersioningPreference(Enum):
    """
    Preference for whether to assign a new version or reuse an existing one.
    """
    PREVIOUS = "previous"
    """Reuse previous existing version."""

    NEW = "new"
    """Assign a new version."""

    ABORT = "abort"
    """Abort the process."""

def get_user_versioning_preference(namespace, previous_version) -> Union[VersioningPreference, str]:
    """
    Get user preference for whether to assign a new version or reuse an existing one.
    """
    inp = input(f"Namespace {namespace} already exists under version {previous_version}.\n"
            f"{Fore.GREEN}Which version?{Style.RESET_ALL} (reuse [p]revious version, assign [n]ew version, or [a]bort): ")
    match inp:
        case 'previous' | 'p':
            # use the last provided version
            return VersioningPreference.PREVIOUS
        case 'new' | 'n':
            return VersioningPreference.NEW
        case 'abort' | 'a':
            return VersioningPreference.ABORT
        case 'custom' | 'c':
            # Secret: allow user to enter a custom version number
            while True:
                custom_version = input("Enter a custom version number: ")

                if custom_version and custom_version.startswith('v'):
                    return custom_version
                else:
                    print("Invalid version number. Please enter a version number starting with 'v'.")
        case _:
            print("Invalid input. Please enter 'previous', 'new', or 'abort'.")
            return get_user_versioning_preference(namespace, previous_version)

