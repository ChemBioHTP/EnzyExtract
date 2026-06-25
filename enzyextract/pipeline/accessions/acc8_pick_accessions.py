# first, get a list of all enzyme names we need disambiguated

from pathlib import Path
from typing import Literal, Union

import polars as pl
from tqdm import tqdm
from enzyextract.prompts.ask_best_uniprots import pick_accessions, PickAccessionSchema
from enzyextract.submit.batch_utils import chunked_write_to_jsonl
from enzyextract.submit.openai_management import process_env, submit_openai_batch_file
from enzyextract.submit.openai_schema import to_openai_batch_request_with_schema



def _clean_enzyme_str(x, clean_uppercase=False):
    if x:
        x = x.replace("\n", "")
        if clean_uppercase and x.isupper():
            return x.lower()
    return x

def _to_header(enzyme, enzyme_full, organism):
    if enzyme and "\n" in enzyme:
        enzyme = enzyme.replace("\n", "")
        print("Newline in", enzyme)
    if enzyme_full and "\n" in enzyme_full:
        enzyme_full = enzyme_full.replace("\n", "")
        print("Newline in", enzyme_full)
    if organism and "\n" in organism:
        organism = organism.replace("\n", "")
        print("Newline in", organism)

    builder = f"Target Enzyme: {enzyme}\n"
    if enzyme_full:
        builder += f"Target Fullname: {enzyme_full}\n"
    if organism:
        builder += f"Target Organism: {organism}\n"
    builder += "\n\n"
    return builder

def to_uniprot_target(enzyme, enzyme_full, organism, df_of_accessions):
    """
    Produces a document for GPT.
    """
    # pdb, descriptor, name, sys_name, organism_right, info
    builder = _to_header(enzyme, enzyme_full, organism)
        
    for uniprot, enzyme_name, organism_right, organism_common in \
        df_of_accessions.select(["uniprot", "enzyme_name", "organism_right", "organism_common"]).iter_rows():

        # if all uppercase, then lowercase it

        enzyme_name = _clean_enzyme_str(enzyme_name, clean_uppercase=False)
        organism_right = _clean_enzyme_str(organism_right, clean_uppercase=False)
        organism_common = _clean_enzyme_str(organism_common, clean_uppercase=False)
        
        # sys_name is most often EC number, not useful
        builder += f"{uniprot}: {enzyme_name}"
        if organism_right or organism_common:
            builder += " from"
            if organism_right:
                builder += f" {organism_right}"
            if organism_common:
                builder += f" ({organism_common})"
        builder += "\n"
    return builder

def to_pdb_target(enzyme, enzyme_full, organism, df_of_accessions):
    builder = _to_header(enzyme, enzyme_full, organism)

    for pdb, descriptor, name, sys_name, organism, info in \
        df_of_accessions.select(["pdb", "descriptor", "name", "sys_name", "organism_right", "info"]).iter_rows():

        # if all uppercase, then lowercase it

        descriptor = _clean_enzyme_str(descriptor, clean_uppercase=True)
        name = _clean_enzyme_str(name, clean_uppercase=True)
        sys_name = _clean_enzyme_str(sys_name, clean_uppercase=True)
        organism = _clean_enzyme_str(organism, clean_uppercase=True)
        info = _clean_enzyme_str(info, clean_uppercase=True)
        
        name_bdr = name if name else (sys_name if sys_name else "")
        # sys_name is most often EC number, not useful
        builder += f"{pdb}: "
        need_comma = False
        if name_bdr:
            builder += f"{name_bdr}"
            need_comma = True
        if info:
            if need_comma:
                builder += "; "
            builder += f"{info}"
            need_comma = True
        if descriptor:
            if need_comma:
                builder += ". "
            builder += f"{descriptor}"
            need_comma = True
        if organism:
            if need_comma:
                builder += "; from "
            builder += f"{organism}"
        builder += "\n"
    return builder

def to_ncbi_target(enzyme, enzyme_full, organism, df_of_accessions):
    """
    Produces a document for GPT.
    """
    # pdb, descriptor, name, sys_name, organism_right, info
    builder = _to_header(enzyme, enzyme_full, organism)
        
    for ncbi, descriptor in \
        df_of_accessions.select(["ncbi", "descriptor"]).iter_rows():

        # if all uppercase, then lowercase it

        descriptor = _clean_enzyme_str(descriptor, clean_uppercase=False)
        if descriptor and ncbi:
            # remove ncbi from descriptor
            descriptor = descriptor.replace(ncbi, "").strip()
        
        # sys_name is most often EC number, not useful
        builder += f"{ncbi}: {descriptor}"
        builder += "\n"
    return builder

def submit_enzyme_pick_file(
        
    accession_type: Literal["uniprot", "pdb", "ncbi"],
    read_from: Union[str, Path, tuple[str, pl.DataFrame]],
    *,
    pending_file: Union[str, Path],
    model_name: str = "gpt-4o",
    dest_dir: Union[str, Path] = "batches/pick",

    top_k: int = 12,
    require_organism: bool = False,
):
    """
    
    :param top_k: how many candidates to include in the prompt for GPT
    """
    # use filename as namespace
    if isinstance(read_from, tuple):
        namespace, info_view = read_from
    else:
        read_from = Path(read_from)
        namespace = read_from.stem
        info_view = pl.read_parquet(read_from)

    batch = []
    # for i, pmid, enzyme, enzyme_full, organism, \
        # pdb, descriptor, name, sys_name, organism_right, info in tqdm(

    # perfect_pdb = info_view.filter((pl.col("max_enzyme_similarity") >= 90) & (pl.col("similarity_organism") >= 95))
    # imperfect_pdb = info_view.filter((pl.col("max_enzyme_similarity") < 90) | (pl.col("similarity_organism") < 95))
    # imperfect_pdb = imperfect_pdb.join(perfect_pdb, on="index", how="anti") # perfect pdb no longer needs to be matched.

    info_view = info_view.with_columns([
        # give unknown organisms a neutral value
        # only relevant when PDB does not give organism
        (pl.col("max_enzyme_similarity").fill_null(0) + pl.col("max_organism_similarity").fill_null(50)).alias("total_similarity")
    ])
    # Mean enzyme similarity:  61.750864798010674
    # GOAL: to put non-organism ahead of non-matches, but behind any close match
    # can be validated by looking at the imperfect_pdb histogram
    if require_organism:
        info_view = info_view.filter(pl.col("organism").is_not_null())

    print("Mean organism similarity: ", info_view.filter(
        pl.col("max_organism_similarity") < 90
    )["max_organism_similarity"].mean())
    if "-dev" in namespace:
        print("RUNNING DEV MODE!")
        # sample 100 indices
        _indices = info_view["index"].sample(100, seed=42).to_list()
        info_view = info_view.filter(pl.col("index").is_in(_indices))
    for i, df in tqdm(
            info_view.partition_by("index", as_dict=True).items(), total=info_view["index"].n_unique()
        ):
        df = df.sort("total_similarity", descending=True)
        # if (df.height > top_k):
        #     if (df["organism"].drop_nulls().len()) and (df["organism_right"].drop_nulls().len() != df.height):
        #         pass
        df = df.head(top_k)

        pmid = df["pmid"][0]
        enzyme = df["enzyme"][0]
        enzyme_full = df["enzyme_full"][0]
        organism = df["organism"][0]
        i = i[0] # untuple
        custom_id = f"{namespace}_{i}_{pmid}"

        # pdb, descriptor, name, sys_name, organism_right, info
        # TODO add the actual accessions

        # refseq_df = desired_accessions.select("refseq").explode("refseq").drop_nulls()

        if accession_type == "uniprot":
            doc = to_uniprot_target(enzyme, enzyme_full, organism, df)
        elif accession_type == "pdb":
            doc = to_pdb_target(enzyme, enzyme_full, organism, df)
        elif accession_type == "ncbi":
            doc = to_ncbi_target(enzyme, enzyme_full, organism, df)
        else:
            raise ValueError(f"Unknown accession_type: {accession_type}")

        docs = [doc]
        req = to_openai_batch_request_with_schema(
            uuid=custom_id,
            system_prompt=pick_accessions,
            docs=docs,
            model_name=model_name,
            schema=PickAccessionSchema
        )
        batch.append(req)


    # dest_filepath = f"batches/pick/{namespace}.jsonl" # + namespace + ".jsonl"
    dest_filepath = Path(dest_dir) / f"{namespace}.jsonl"
    Path(dest_filepath).parent.mkdir(parents=True, exist_ok=True)

    print("Have", len(batch), "entries")
    chunk_dests = chunked_write_to_jsonl(batch, str(dest_filepath), 10000)
    for chunk_dest in chunk_dests:
        try:
            batchname = submit_openai_batch_file(chunk_dest, pending_file=pending_file) # will ask for confirmation
        except Exception as e:
            print("Error submitting batch", chunk_dest, e)


if __name__ == "__main__":
    process_env(".env")
    submit_enzyme_pick_file(
        "uniprot",
        ("pick-uniprot-dev2.0", pl.read_parquet("data/enzymes/pick/pick-uniprot-v1.parquet")),
        model_name="gpt-4o",
        dest_dir="experiments/batches/pick",
        pending_file="experiments/batches/pending.jsonl",
        require_organism=True,
    )
    submit_enzyme_pick_file(
        "pdb",
        ("pick-pdb-dev2.0", pl.read_parquet("data/enzymes/pick/pick-pdb-v1.parquet")),
        model_name="gpt-4o",
        dest_dir="experiments/batches/pick",
        pending_file="experiments/batches/pending.jsonl",
        require_organism=True,
    )
    submit_enzyme_pick_file(
        "ncbi",
        ("pick-ncbi-dev2.0", pl.read_parquet("data/enzymes/pick/pick-ncbi-v1.parquet")),
        model_name="gpt-4o",
        dest_dir="experiments/batches/pick",
        pending_file="experiments/batches/pending.jsonl",
        require_organism=True,
    )
