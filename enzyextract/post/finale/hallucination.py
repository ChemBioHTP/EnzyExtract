import os
import typing
from typing import Callable, List, Tuple, Union
import polars as pl

from enzyextract.dependency.injection import REQUIRE, resolve
from enzyextract.dependency.prereqs import export
from enzyextract.post.finale.deduplication import deduplicate
from enzyextract.post.metadata.doctype import _filter_out_xmls
from enzyextract.post.pl_validation import expect_columns



def _try_rejoin_custom_ids(data, custom_id_src):
    """
    Rejoin the custom_id_src to the data, to get the custom_id back.
    """
    expect_columns(data, ['pmid', 'descriptor'])
    expect_columns(custom_id_src, ['pmid', 'descriptor', 'custom_id'])

    return data.join(
        custom_id_src.select('pmid', 'descriptor', 'custom_id').unique(['pmid', 'descriptor'], maintain_order=True, keep='first'),
        on=['pmid', 'descriptor'],
        how='left'
    )
def _pivot(df: pl.DataFrame, on: str, value: str) -> pl.DataFrame:
    """
    Pivot the DataFrame from long to wide format.
    But NO aggregation is done.
    """
    dfs = []
    for original_col, subdf in df.partition_by(on, as_dict=True).items():
        dfs.append(subdf.with_columns(
            pl.col(value).alias(original_col[0])
        ).drop(on).drop(value))
    return pl.concat(dfs, how="diagonal_relaxed")

def recoalesce(
    df: pl.DataFrame,
    cols: Union[str, List[str]],
    *,
    suffix: str = "_right"
):
    """
    Coalesce {col}_right into {col}, preferring {col}.
    """

    if isinstance(cols, str):
        cols = [cols]

    return df.with_columns([
        pl.coalesce(
            pl.col(col),
            pl.col(f"{col}{suffix}")
        )
        for col in cols
        if f"{col}{suffix}" in df.columns
    ]).drop([
        f"{col}{suffix}" 
        for col in cols 
        if f"{col}{suffix}" in df.columns
    ])


def detect_hallucinations(pdfdata: pl.LazyFrame, scan_df: pl.LazyFrame, *, thedata_cols=["kcat", "km", "kcat_km"], thedata_pmid=["pmid"]):
    """
    thedata_df: pl.DataFrame
        The DataFrame containing the data to be checked for hallucinations.
        In particular, column "kcat" and "km" will be checked.

    scan_df: pl.DataFrame
        The scan DataFrame containing PDF text.
        Should have columns "pmid", "page_number", and "text".

    Returns:
        DataFrame with added columns "has_needle", "needles_found", "needles_total", "needles_missing"
    """
    pdfdata = pdfdata.lazy()
    scan_df = scan_df.lazy()

    # create analyte, a df with columns "analyte" and "pmid"
    analyte = []
    for col in thedata_cols:
        analyte.append(
            pdfdata.select(
                col,
                *thedata_pmid,
                pl.lit(col).alias('original_col')
            ).rename({col: "analyte"})
        )
    analyte = pl.concat(analyte, how="vertical")

    # extract the numerical values
    analyte = analyte.unique().with_columns(
        pl.col("analyte")
        .str.extract(r"(\d+\.?\d*)")
        .str.strip_chars_end(".") # remove trailing decimal
        .alias("needle")
    ).filter(
        pl.col("needle").is_not_null()
         & (pl.col("needle").str.len_chars() > 2) # 3+ digits
    )

    # construct a regex search pattern, pmid-wise
    needles = analyte.group_by(*thedata_pmid).agg(
        # ("(" + pl.col("num_string").str.replace(r".", r"\.", literal=True).str.join("|") + ")").alias("search_regex")
        pl.col("analyte"),
        pl.col("original_col"),
        pl.col("needle")
    )

    # join with the scan_df
    scan_pages_combined = scan_df.group_by("pmid").agg(
        pl.col("text")
    ).with_columns(
        pl.col("text")
        .list.join("\n")
        .str.replace_all(r"(\d+),\s*(\d+)", r"$1$2") # remove commas from numbers
        .str.replace_all(r"(\d+)\s*\.\s*(\d+)", r"$1.$2") # remove extra spaces from numbers
        .alias("text")
    )

    combined = scan_pages_combined.join(
        needles,
        left_on="pmid",
        right_on=thedata_pmid
    )

    def list_eval_ref(listcol, refcol, op: Callable[[pl.Expr, pl.Expr], pl.Expr]):
        return pl.concat_list(pl.struct(listcol, refcol)).list.eval(
            op(pl.element().struct[0].explode(), pl.element().struct[1])
        )
    does_contain = combined.with_columns(
        # pl.col("text").str.contains(
        #     pl.col("search_regex"),
        # ).alias("contains")

        # https://github.com/pola-rs/polars/issues/7210
        # pl.col("needles").list.eval(
        #     pl.col("text").str.contains(pl.element(), literal=True)
        # ).alias("has_needles")

        list_eval_ref(
            "needle", # actually a list of needles
            "text", 
            lambda element, ref: ref.str.contains(element, literal=True)
        ).alias("has_needle")

    ).drop('text').with_columns(
        pl.col('has_needle').list.sum().alias("needles_found"),
        pl.col('has_needle').list.len().alias("needles_total")
    ).with_columns(
        (pl.col("needles_total") - pl.col("needles_found")).alias("needles_missing"),
    ).collect()

    does_contain_explode = does_contain.explode("analyte", "original_col", "needle", "has_needle")
    # not_ok_df = does_contain_explode.filter(pl.col("needles_missing") > 0)

    # extremely_not_ok = does_contain_explode.filter(
    #     pl.col("needles_found") == 0
    # )

    # forgive if <= 1/6 of the needles are missing
    # suspicious = does_contain_explode.filter(
    #     (pl.col("needles_missing") > 0)
    #     & ((pl.col("needles_missing") / pl.col("needles_total")) > (1/6))
    # )

    # pivoted = does_contain_explode.select("pmid", "original_col", "analyte", "has_needle").pivot(
    #     on="original_col",
    #     index=["pmid", "has_needle"],
    #     values="analyte",
    #     aggregate_function="first"
    # )
    # pivoted = _pivot(does_contain_explode, on="original_col", value="analyte")
    # pivoted_sus = pivoted.filter(
    #     ~pl.col("has_needle")
    # ).select("pmid", "kcat", "km", "kcat_km", "has_needle")
    def _pick(name, does_contain_explode, pdfdata_sus):
        rhs = does_contain_explode.filter(
            ~pl.col("has_needle")
            & (pl.col("original_col") == name)
        ).select("pmid", "analyte", "has_needle", "needles_missing", "needles_total").unique()

        return recoalesce(pdfdata_sus.join(
            rhs,
            left_on=["pmid", name],
            right_on=["pmid", "analyte"],
            how="full",
            coalesce=True,
            validate='m:1'
        ), ["has_needle", "needles_missing", "needles_total"])

    pdfdata_sus = _pick("kcat", does_contain_explode, pdfdata.collect())
    pdfdata_sus = _pick("km", does_contain_explode, pdfdata_sus)
    pdfdata_sus = _pick("kcat_km", does_contain_explode, pdfdata_sus)
    pdfdata_sus = pdfdata_sus.with_columns(
        pl.col("has_needle").fill_null(True)
    )
    return pdfdata_sus


@resolve
def _preview_illegible(
    pdfdata, 
    manifest = REQUIRE('data/manifest.parquet'),
):
    # filter out those that are illegible
    pdfdata_illegible = pdfdata.join(
        manifest.filter(
            ~pl.col('readable')
        ),
        on='pmid',
        how='semi'
    ) # height = 0 (good!)

@resolve
@export("data/export/2_dedup/TheData_kcat_hallucinations.parquet")
def script_detect_hallucinations(
    thedata_df: pl.DataFrame = REQUIRE('data/export/TheData_kcat.parquet'),
    custom_ids: pl.DataFrame = REQUIRE('data/recontext/1_fromyaml/data.parquet')
):
    # Example usage
    # thedata_df = pl.read_parquet('data/export/TheData_kcat.parquet')
    # custom_ids = pl.read_parquet('data/recontext/1_fromyaml/data.parquet')
    
    thedata_df = deduplicate(thedata_df)
    # move pmid to the front
    thedata_df = thedata_df.select(
        'pmid',
        pl.selectors.exclude(
            'pmid',
            # 'cid', 
            # 'brenda_id', 
            # 'smiles', 
            # 'cid_full', 
            # 'brenda_id_full', 
            # 'enzyme_ecs', 
            # 'sequence', 
            # 'sequence_source', 
            # 'uniprot', 
            # 'ncbi', 
            # 'pdb', 
            # 'max_enzyme_similarity', 
            # 'max_organism_similarity', 
            # 'total_similarity'
        )
    )
    # thedata_df.write_parquet('data/export/2_dedup/TheData_kcat_dedup.parquet')

    # those that are derived from XML are safe - remove from the analysis
    pdfdata, xmldata = _filter_out_xmls(
        thedata_df,
        custom_id_src=custom_ids
    ) # 7890717

    scan_df = pl.concat([
        pl.scan_parquet('C:/conjunct/EnzyExtract/enzy_runner/data/scans/brenda.parquet'),
        pl.scan_parquet('C:/conjunct/EnzyExtract/enzy_runner/data/scans/scratch.parquet'),
        pl.scan_parquet('C:/conjunct/EnzyExtract/enzy_runner/data/scans/topoff.parquet'),
        pl.scan_parquet('C:/conjunct/EnzyExtract/enzy_runner/data/scans/wos.parquet'),
    ])

    # filter out those that are illegible
    manifest = pl.read_parquet('data/manifest.parquet')
    scan_df = scan_df.join(
        manifest.lazy().filter(
            pl.col('readable')
        ),
        on='pmid',
        how='semi'
    )

    halluc = detect_hallucinations(pdfdata, scan_df)


    suspicious_pmids = halluc.filter(
        ~pl.col('has_needle')
    ).select('pmid').unique()

    halluc_only = halluc.join(
        suspicious_pmids,
        on='pmid',
        how='semi'
    ).select(pl.selectors.exclude(
        'cid', 
        'brenda_id', 
        'smiles', 
        'cid_full', 
        'brenda_id_full', 
        'enzyme_ecs', 
        'sequence', 
        'sequence_source', 
        'uniprot', 
        'ncbi', 
        'pdb', 
        'max_enzyme_similarity', 
        'max_organism_similarity', 
        'total_similarity'
    ))
    # fill in needle stats
    needles_stats = halluc_only.select('pmid', 'needles_missing', 'needles_total').drop_nulls().unique()
    halluc_only = recoalesce(halluc_only.join(
        needles_stats,
        on='pmid',
        how='left'
    ), ['needles_missing', 'needles_total']).with_columns(
        # ((pl.col('needles_missing') + 1) / (pl.col('needles_total') + 2)).alias('suspiciousness')
        (pl.col('needles_missing') / (pl.col('needles_total'))).alias('suspiciousness')
    ).sort('suspiciousness', descending=True, maintain_order=True)
    halluc_only.write_parquet('data/export/2_dedup/TheData_kcat_hallucinations.parquet')

@export("data/export/2_dedup/pdf_hallucinations.parquet", autosave=True, cached=True)
def pdf_hallucinations(pdfdata: pl.DataFrame):

    pdf_scan_df = pl.concat([
        pl.scan_parquet('C:/conjunct/EnzyExtract/enzy_runner/data/scans/brenda.parquet'),
        pl.scan_parquet('C:/conjunct/EnzyExtract/enzy_runner/data/scans/scratch.parquet'),
        pl.scan_parquet('C:/conjunct/EnzyExtract/enzy_runner/data/scans/topoff.parquet'),
        pl.scan_parquet('C:/conjunct/EnzyExtract/enzy_runner/data/scans/wos.parquet'),
    ])

    halluc = detect_hallucinations(pdfdata, pdf_scan_df)

    suspicious_pmids = halluc.filter(
        ~pl.col('has_needle')
    ).select(
        'pmid',
        'needles_missing',
        'needles_total',
        (pl.col('needles_missing') / (pl.col('needles_total'))).alias('flag.hallucination')
    ).unique('pmid', keep='first')

    return suspicious_pmids

def attach_hallucination_flag(
    data: pl.DataFrame,
    threshold: float = 0.35,
):
    """
    Pre:
    - data: should contain 'pmid' and 'meta.doctype' columns. 
    """
    pdfdata = data.filter(
        pl.col('meta.doctype') == 'pdf'
    )
    # xmldata = data.filter(
    #     pl.col('meta.doctype') == 'xml'
    # )

    suspicious_pmids = pdf_hallucinations(pdfdata)

    data = data.join(
        suspicious_pmids.select('pmid', 'flag.hallucination'),
        on='pmid',
        how='left'
    )
    return data

if __name__ == "__main__":
    script_detect_hallucinations()
