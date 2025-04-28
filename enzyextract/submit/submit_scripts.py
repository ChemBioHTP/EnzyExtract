import asyncio
import os
from typing import Generator, Literal, Optional, Tuple
import random
from tqdm import tqdm

from enzyextract.pipeline.llm_log import read_log, update_log
from enzyextract.submit.base import SubmitPreference, do_presubmit
from enzyextract.submit.batch_utils import to_openai_batch_request, write_to_jsonl
from enzyextract.submit.litellm_management import submit_litellm_batch_file
from enzyextract.submit.openai_management import process_env
from enzyextract.submit.openai_schema import to_openai_batch_request_with_schema
from enzyextract.utils.namespace_management import validate_namespace
import polars as pl
import PIL


from pydantic import BaseModel


def allocate_new_version(previous_log: pl.DataFrame) -> int:
    if previous_log.height == 0:
        version = 1
    else:
        # should ignore nulls
        version = previous_log['version'].cast(pl.UInt32, strict=False).max() + 1
    return version

def enzy_root_setup(
    *, 
    namespace: str, # ids
    # model settings
    previous_log: pl.DataFrame,
    batch_folder: str, # write to
    corresp_folder: str, # write any data matching custom_id to 
    assign_new_version: bool = True, # if False, will use the previous version
):
    update_if_exists = False
    # do validation, making sure namespace is unique
    validate_namespace(namespace)

    if assign_new_version:
        version = allocate_new_version(previous_log)
        update_if_exists = False
    else:
        # use the previous version
        update_if_exists = False
        previous_namespace = previous_log.filter(
            pl.col('namespace') == namespace
        ) # .select('namespace')
        if previous_namespace.height > 0:
            yn = input(f"Namespace {namespace} version {version} already exists, do you want to (r)euse, (o)verwrite, or (a)bort?")
            if yn == 'o' or yn == 'overwrite':
                version = previous_namespace.item(0, 'version')
                update_if_exists = True
            elif yn == 'r' or yn == 'reuse':
                raise NotImplementedError("Reuse not implemented yet.")
            else:
                raise ValueError("Aborting.")

    os.makedirs(batch_folder, exist_ok=True)
    os.makedirs(corresp_folder, exist_ok=True)

    return version, update_if_exists


def stream_submit_batch(
    *,
    need_to_submit: list[str], # list of file paths to submit
    llm_provider: str,
) -> Generator[Tuple[Literal['submitted', 'local'], str, str, str], None, None]:
    """
    Should yield status, file_uuid, batchname, batch_fpath, corresp_fpath
    """
    print("Time to submit!")
    for i, batch_fpath in enumerate(need_to_submit):

        # special case with 1 shard
        if len(need_to_submit) == 1:
            i = None
        
        # read to make sure
        inp = do_presubmit(
            filepath=batch_fpath,
            submit_suffix=f"Submit to {llm_provider}?",
        )
        
        if inp == SubmitPreference.REMOVE:
            print("Removing.")
            os.remove(batch_fpath)
            continue
        elif inp == SubmitPreference.UNTRACK:
            print("Saved untracked copy at", batch_fpath)
            continue
        elif inp == SubmitPreference.YES:
            try:
            
                file_uuid, batchname = asyncio.run(submit_litellm_batch_file(batch_fpath, custom_llm_provider=llm_provider))
                # if i is None:
                
                # else:
                    # corresp_fpath = f'{corresp_folder}/{namespace}_{version}.{i}.parquet'
                
                yield 'submitted', file_uuid, batchname, batch_fpath
            except Exception as e:
                print("Error submitting batch", batch_fpath)
                print(e)
                yield 'local', None, None, batch_fpath
        elif inp == SubmitPreference.LOCAL:
            print("Tracked local copy at", batch_fpath)
            yield 'local', None, None, batch_fpath
        else:
            print("Unknown consent", inp, "exiting.")
            return

def script_classify_images(
    *, 
    namespace: str, # ids
    image_folder: str, # read from
    # model settings
    model_name: str,
    system_prompt: Optional[str],
    prompt: Optional[str],
    prompt_schema_cls: Optional[type],


    log_location: str,
    batch_folder: str, # write to
    corresp_folder: str, # write any data matching custom_id to 

    structured = True,
    detail='auto',
    llm_provider: str = 'openai',
    max_images: int = None, # max number of images to process. None = all
    assign_new_version: bool = True, # if False, will use the previous version
):

    if structured:
        assert prompt_schema_cls is not None, "prompt_schema_cls must be provided for structured mode"

    process_env('.env')


    previous_log = read_log(log_location)
    version, update_if_exists = enzy_root_setup(
        namespace=namespace,
        previous_log=previous_log,
        batch_folder=batch_folder,
        corresp_folder=corresp_folder,
        assign_new_version=assign_new_version,
    )



    batch = []
    correspondences = []
    corresp_fpath = f'{corresp_folder}/{namespace}_{version}.parquet'

    print("Namespace: ", namespace)

    will_write_to = f'{batch_folder}/{namespace}_{version}.jsonl'

    # TODO: default: we should use code overwrite vs reuse semantics, because sometimes we want different ones
    # if os.path.exists(will_write_to):
    #     print(f"File {will_write_to} already exists, skipping.")

    #     assert os.path.exists(corresp_fpath), f"Correspondence file {corresp_fpath} does not exist."

    #     need_to_submit = [will_write_to]
    #     i = 0
    # else:
    samples = os.listdir(image_folder)
    if max_images is not None:
        # shuffle
        random.seed(42)
        random.shuffle(samples)
        samples = samples[:max_images]
    for j, image_fname in enumerate(tqdm(samples)):
        if not any([image_fname.endswith(x) for x in ['.png', '.jpg', '.jpeg']]):
            continue

        fpath = image_folder + '/' + image_fname
        
        docs = []
        if prompt is not None:
            docs.append(prompt)
        # load the image
        try:
            # skip any that are over 10MB
            if os.path.getsize(fpath) > 10 * 1024 * 1024:
                print(f"Skipping {fpath} due to size.")
                continue
            img = PIL.Image.open(fpath)
            img = img.convert('RGB')
            # img = img.resize((512, 512))
            # img = img.tobytes()
            docs.append(img)
        except Exception as e:
            print(f"Error loading image {fpath}: {e}")
            continue

        # obtain original annotation from part A
        # use the table_md_root

        custom_id = f'{namespace}_{version}_{j}'
        if structured:
            req = to_openai_batch_request_with_schema(
                custom_id, 
                system_prompt=system_prompt, 
                docs=docs,
                model_name=model_name,
                schema=prompt_schema_cls,
                detail=detail
            )
        else:
            req = to_openai_batch_request(custom_id, prompt, docs, 
                                    model_name=model_name)
        batch.append(req)
        correspondences.append({"custom_id": custom_id, "index": j, "filepath": fpath})

    print("Using model", model_name)
    corr_df = pl.DataFrame(correspondences)
    corr_df.write_parquet(corresp_fpath) # NOTE: overwrites
        
    # write in chunks

    chunk_size = 1000
    have_multiple = len(batch) > chunk_size # need to enforce chunk size, since OpenAI has data size limit
    need_to_submit = []
    for i in range(0, len(batch), chunk_size):
        chunk = batch[i:i+chunk_size]


        if have_multiple:
            will_write_to = f'{batch_folder}/{namespace}_{version}.{i}.jsonl'
        write_to_jsonl(chunk, will_write_to) # NOTE: overwrites
        need_to_submit.append(will_write_to)


    for status, file_uuid, batchname, batch_fpath in stream_submit_batch(
        need_to_submit=need_to_submit,
        llm_provider=llm_provider
    ):

        # update log
        update_log(
            log_location=log_location,
            namespace=namespace,
            version=version,
            shard=i,
            status=status,

            model_name=model_name,
            llm_provider=llm_provider,
            prompt=prompt,
            structured=structured,

            file_uuid=file_uuid,
            batch_uuid=batchname,
            batch_fpath=batch_fpath,
            corresp_fpath=corresp_fpath,
            replace_existing_record=update_if_exists
        )




if __name__ == '__main__':
    raise NotImplementedError("This script is only an example.")
    enzy_root = 'experiments/runs/.enzy'
    image_folder = 'path/to/image/folder'
    script_classify_images(
        namespace='my-namespace-here',
        image_folder=image_folder,
        model_name='gpt-4o-mini',
        system_prompt=system_prompt,
        prompt=prompt, # TODO
        prompt_schema_cls=MyPydanticSchema, # TODO
        log_location=f'{enzy_root}/llm_log.tsv',
        batch_folder=f'{enzy_root}/batches',
        corresp_folder=f'{enzy_root}/corresp',
        structured=True,
        detail='auto',
        llm_provider='openai',
        max_images=1000,
        assign_new_version=True,
    )