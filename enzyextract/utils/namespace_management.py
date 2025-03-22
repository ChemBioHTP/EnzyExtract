

from enzyextract.utils import prompt_collections
import re


def validate_namespace(namespace):
    # make sure no bad characters in the namespace
    if re.search(r'[*/\\<>:|?]', namespace):
        raise ValueError("Namespace contains invalid characters")


def glean_model_name(namespace, task='ingestion'):
    """
    
    """

    structured = False
    prompt = None
    if namespace.endswith('-mini'):
        
        # prompt = prompt_collections.table_oneshot_v2 # v1
        model_name = 'gpt-4o-mini' # 'ft:gpt-4o-mini-2024-07-18:personal:oneshot:9sZYBFgF' # gpt-4o
    elif namespace.endswith('-tuned'):
        
        prompt = prompt_collections.table_oneshot_v1
        model_name = 'ft:gpt-4o-mini-2024-07-18:personal:oneshot:9sZYBFgF' # gpt-4o
    elif namespace.endswith('-tuneboth'):
            
        prompt = prompt_collections.table_oneshot_v1_2
        model_name = 'ft:gpt-4o-mini-2024-07-18:personal:readboth:9wwLXS4i' # gpt-4o
    elif namespace.endswith('-t2neboth'):
                
        prompt = prompt_collections.table_oneshot_v3
        model_name = 'ft:gpt-4o-mini-2024-07-18:personal:t2neboth:9zuhXZVV' # gpt-4o

    elif namespace.endswith('-t3neboth'):
        prompt = prompt_collections.table_oneshot_v3
        model_name = 'ft:gpt-4o-mini-2024-07-18:personal:t3neboth:AOpwZY6M'
    elif namespace.endswith('-t4neboth'):
        prompt = prompt_collections.table_oneshot_v3
        model_name = 'ft:gpt-4o-mini-2024-07-18:personal:t4neboth:AQOYyPCz'
    
    elif namespace.endswith('-manifold'):
        prompt = prompt_collections.for_manifold
        model_name = 'ft:gpt-4o-mini-2024-07-18:personal:manifold:AhVt9j8B'

    elif namespace.endswith('-oneshot') or namespace.endswith('-4o'):
            
        # prompt = prompt_collections.table_oneshot_v1
        model_name = 'gpt-4o-2024-05-13'
    elif namespace.endswith('-4os'):
        model_name = 'gpt-4o-2024-08-06' 
    elif namespace.endswith('-4o-str') or namespace.endswith('-4ostruct'): # structured output
        model_name = 'gpt-4o-2024-08-06' 
        structured = True
    
        
    else:
        raise ValueError("Unrecognized namespace", namespace)
    
    if task != 'ingestion':
        prompt = None # TODO: not implemented yet to give the right prompt for other tasks

    return model_name, prompt, structured
    