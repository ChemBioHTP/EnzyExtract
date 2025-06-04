import polars as pl
from typing import List, Tuple
def _filter_out_xmls(data: pl.DataFrame, custom_id_src: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Hallucination detection is only currently available for PDFs, so we need to filter out
    which data was derived from XML versus PDF.

    custom_id_src: should contain 'pmid', 'descriptor', 'custom_id' columns.

    Uses magic strings stored in the custom_id. 
    ("openelse" and "vier" are the magic strings, since the Elsevier API returns XMLs)

    Returns: (pdfdata, xmldata)
    """
    # those that are derived from XML are safe - remove from the analysis
    xml_based_descriptors = custom_id_src.filter(
        pl.col('custom_id').str.contains_any(['openelse', 'vier'])
    ).select(
        pl.col('pmid'),
        pl.col('descriptor'),
        pl.col('custom_id')
    )

    xmldata = data.join(
        xml_based_descriptors,
        on=['pmid', 'descriptor'],
        how='semi'
    )
    pdfdata = data.join(
        xml_based_descriptors,
        on=['pmid', 'descriptor'],
        how='anti'
    )

    # examine pmids common to both thedata_safe and thedata_df, as they are a sign that
    # descriptors are being modified
    conflicting_pmids = xmldata.select('pmid').unique().join(
        pdfdata.select('pmid').unique(),
        on='pmid',
        how='inner'
    ) # height=0. Good. descriptors are left verbatim.

    # NOTE: it is important to call deduplicate(thedata_df) before this, as deduplicate() helps
    # remove documents processed twice. Otherwise the assertion below may fail.
    assert conflicting_pmids.height == 0, "conflicting pmids found between xmldata and pdfdata"
    assert pdfdata.height + xmldata.height == data.height, "pdfdata and xmldata do not cover all data"

    return pdfdata, xmldata

def attach_doctype_meta(
    data: pl.DataFrame, 
    custom_id_src: pl.DataFrame,
    magic_strings: List[str] = ['openelse', 'vier']
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Pre: 
    - data: should contain 'pmid' and 'descriptor' columns.
    - custom_id_src: should contain 'pmid', 'descriptor', 'custom_id' columns.

    Post:
    - data: additional column named 'meta.doctype' will be added, containing 'xml' or 'pdf'.

    Uses magic strings stored in the custom_id. 
    ("openelse" and "vier" are the magic strings, since the Elsevier API returns XMLs)

    Returns: (pdfdata, xmldata)
    """
    # those that are derived from XML are safe - remove from the analysis
    xml_based_descriptors = custom_id_src.filter(
        pl.col('custom_id').str.contains_any(magic_strings)
    ).select(
        pl.col('pmid'),
        pl.col('descriptor')
    ).with_columns(
        pl.lit('xml').alias('meta.doctype')
    ).unique(['pmid', 'descriptor'])

    data = data.join(
        xml_based_descriptors,
        on=['pmid', 'descriptor'],
        how='left',
        validate='m:1'
    ).with_columns(
        pl.when(pl.col('meta.doctype').is_null()).then(
            pl.lit('pdf')
        ).otherwise(
            pl.col('meta.doctype')
        ).alias('meta.doctype')
    )

    return data