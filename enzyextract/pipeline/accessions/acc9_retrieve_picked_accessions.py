from pathlib import Path
from typing import Literal, Optional, Union

import polars as pl
import os
from tqdm import tqdm
import json

from enzyextract.dependency.prereqs import export
from enzyextract.submit.openai_management import check_undownloaded, process_env

def accept_filename(
    x: str,
    accession_type: Literal["pdb", "ncbi", "uniprot"],
    known_namespace: Optional[str] = None
):
    if known_namespace is not None:
        return x.endswith(".jsonl") and known_namespace in x
    return x.endswith(".jsonl") and accession_type in x

def load_ingest(
    accession_type: Literal["pdb", "ncbi", "uniprot"],
    known_namespace: Optional[str] = None,
    *,
    batch_input_location: Union[str, Path] = "batches/pick",
):
    # get_stuff = re.compile("Target Enzyme:/ (.*)\nTarget Fullname: (.*)\n(Target Organism: (.*)\n)?")
    batch_input_location = Path(batch_input_location)
    collected = []
    for filename in tqdm(os.listdir(batch_input_location)):
        if not accept_filename(filename, accession_type, known_namespace=known_namespace):
            continue
        filepath = batch_input_location / filename
        with open(filepath, "r") as f:
            for line in f:
                req = json.loads(line)
                prompt = req["body"]["messages"][0]["content"]
                content = ""
                for msg in req["body"]["messages"][1:]:
                    content += msg["content"] + "\n"
                
                # gps = 
                # stuff_match = get_stuff.match(content)
                # enzyme, enzyme_full, _, organism = stuff_match.groups()
                assert "Target Enzyme: " in content
                enzyme, rest = content.split("Target Enzyme: ", 1)[1].split("\n", 1)
                enzyme_full = None
                organism = None
                if "Target Fullname: " in rest:
                    enzyme_full, rest = rest.split("Target Fullname: ", 1)[1].split("\n", 1)
                if "Target Organism: " in rest:
                    organism, rest = rest.split("Target Organism: ", 1)[1].split("\n", 1)


                # then, extract everything that comes after
                acc = []
                for line in rest.split("\n"):
                    if not line:
                        continue
                    if ": " in line:
                        key, value = line.split(": ", 1)
                        acc.append(key)
                
                if enzyme_full == "None":
                    enzyme_full = None
                cid = req["custom_id"]
                _, idx, pmid = cid.split("_", 2)
                collected.append((# cid, 
                                  idx, pmid, content, acc,
                    enzyme, enzyme_full, organism))
    df = pl.DataFrame(collected, 
        orient="row",
        schema=[# "custom_id", 
                "idx", "pmid", "content", "tried_accessions",
            "enzyme", "enzyme_full", "organism"],
        schema_overrides={
            # "custom_id": pl.Utf8,
            "idx": pl.UInt32,
            "pmid": pl.Utf8,
            "tried_accessions": pl.List(pl.Utf8),
            # "content": pl.Utf8,
            "enzyme": pl.Utf8,
            "enzyme_full": pl.Utf8,
            "organism": pl.Utf8
        })
    return df


def load_gpt(accession_type: Literal["pdb", "ncbi", "uniprot"], completions_folder: Union[str, Path] = "completions/pick"):
    collected = []
    for filename in tqdm(os.listdir(completions_folder)):
        if not accept_filename(filename, accession_type):
            continue
        filepath = Path(completions_folder) / filename
        with open(filepath, "r") as f:
            for line in f:
                req = json.loads(line)
                content = req["response"]["body"]["choices"][0]["message"]["content"]
                stop_reason = req["response"]["body"]["choices"][0]["finish_reason"]
                # if stop_reason != "stop":
                cid = req["custom_id"]
                
                _, idx, pmid = cid.split("_", 2)
                
                try:
                    obj = json.loads(content, strict=False)
                    thoughts = obj["thoughts_and_comments"]
                    best = obj["best"]
                    second = obj["second_best"]
                    third = obj["third_best"]
                    # additional = obj["additional"]
                except:
                    pass
                pmid = cid.split("_", 2)[2]
                collected.append((# cid, 
                                  idx, pmid, # content, 
                    thoughts, best, second, third, # additional, 
                    stop_reason))

    gpt_df = pl.DataFrame(collected, 
        orient="row",
        schema=[# "custom_id", 
                "idx", "pmid", # "content", 
            "thoughts", "best", "second", "third", # "additional", 
            "stop_reason"],
        schema_overrides={
            # "custom_id": pl.Utf8,
            "idx": pl.UInt32,
            "pmid": pl.Utf8,
            # "content": pl.Utf8,
            "thoughts": pl.Utf8,
            "best": pl.Utf8,
            "second": pl.Utf8,
            "third": pl.Utf8,
            # "additional": pl.List(pl.Utf8),
            "stop_reason": pl.Utf8
        })
    return gpt_df

@export("data/thesaurus/enzymes/uniprot_picked.parquet")
def retrieve_uniprot(
    known_namespace: Optional[str] = None,
    batch_input_location: Union[str, Path] = "batches/pick",
    batch_output_location: Union[str, Path] = "completions/pick",
):
    ingest_df = load_ingest("uniprot", known_namespace=known_namespace, batch_input_location=batch_input_location)

    gpt_df = load_gpt("uniprot", completions_folder=batch_output_location)
    df = ingest_df.join(gpt_df, on=["idx", "pmid"], how="inner")
    print(df)

    df = df.select("pmid", "enzyme", "enzyme_full", "organism", "best").rename({"best": "uniprot"})
    df = df.with_columns([
        pl.col("uniprot").replace("null", None)
    ])
    return df

@export("data/thesaurus/enzymes/pdb_picked.parquet")
def retrieve_pdb(
    known_namespace: Optional[str] = None,
    batch_input_location: Union[str, Path] = "batches/pick",
    batch_output_location: Union[str, Path] = "completions/pick",
):
    ingest_df = load_ingest("pdb", known_namespace=known_namespace, batch_input_location=batch_input_location)

    gpt_df = load_gpt("pdb", completions_folder=batch_output_location)
    df = ingest_df.join(gpt_df, on=["idx", "pmid"], how="inner")
    print(df)

    # df.select("pmid", "enzyme", "enzyme_full", "organism", "best")
    df = df.rename({"best": "pdb"})
    return df

@export("data/thesaurus/enzymes/ncbi_picked.parquet")
def retrieve_ncbi(
    known_namespace: Optional[str] = None,
    batch_input_location: Union[str, Path] = "batches/pick",
    batch_output_location: Union[str, Path] = "completions/pick",
):
    ingest_df = load_ingest("ncbi", known_namespace=known_namespace, batch_input_location=batch_input_location)

    gpt_df = load_gpt("ncbi", completions_folder=batch_output_location)
    df = ingest_df.join(gpt_df, on=["idx", "pmid"], how="inner")
    print(df)

    df = df.select("pmid", "enzyme", "enzyme_full", "organism", "best").rename({"best": "ncbi"})
    df = df.with_columns([
        pl.col("ncbi").replace("null", None)
    ])
    return df

# print(gpt_df)

@export("data/thesaurus/enzymes/uniprots_cited.parquet")
def _generate_cited_chapter(
    gpt_df,
):
    """
    Deprecated; no longer necessary. Equivalent to uniprot_picked.
    """
    ingest_df = load_ingest("uniprot")

    gpt_df = load_gpt("uniprot")
    df = ingest_df.join(gpt_df, on=['idx', 'pmid'], how='inner')

    # 'data/gpt/uniprot_prod1.parquet'

    uniprot_dict = df.select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'best']).rename({'best': 'uniprot'})

    uniprot_seq = pl.read_parquet("data/enzymes/accessions/final/uniprot.parquet")
    uniprot_seq = uniprot_seq.select(['uniprot', 'sequence'])
    uniprot_seq = uniprot_seq.unique('uniprot')
    uniprot_dict = uniprot_dict.join(uniprot_seq, on='uniprot', how='left').drop_nulls(['sequence', 'uniprot'])
    uniprot_dict = uniprot_dict.with_columns([
        pl.lit('cited').alias('enzyme_source')
    ])
    uniprot_dict = uniprot_dict.with_columns([
        pl.when(pl.col('enzyme') == pl.col('enzyme_full')).then(None).otherwise(pl.col('enzyme_full')).alias('enzyme_full')
    ])
    return uniprot_dict



if __name__ == "__main__":
    process_env(".env")
    # preview_batches_uploaded()
    
    # check undownloaded

    Path("experiments/completions/pick").mkdir(parents=True, exist_ok=True)
    Path("experiments/completions/errors").mkdir(parents=True, exist_ok=True)
    redownloaded = check_undownloaded(
        download_folder="experiments/completions/pick",
        autodownload=True,
        path_to_pending="experiments/batches/pending.jsonl",
        _walkable_download_folder="experiments/completions",
        errors_folder="experiments/completions/errors",
    ) # , path_to_pending=None)
    
    if len(redownloaded) > 0:
        print("Detected Batches:")
        for batch, name in redownloaded:
            if name != batch.id:
                print(f"{name} was {batch.id}.")
            else:
                print(name)

    uniprot_df = retrieve_uniprot("pick-uniprot-dev2.0", "experiments/batches/pick", "experiments/completions/pick")
    pdb_df = retrieve_pdb("pick-pdb-dev2.0", "experiments/batches/pick", "experiments/completions/pick")
    ncbi_df = retrieve_ncbi("pick-ncbi-dev2.0", "experiments/batches/pick", "experiments/completions/pick")
    pass