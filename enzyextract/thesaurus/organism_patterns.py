import polars as pl

# TODO: https://www.uniprot.org/help/taxonomy
organism_patterns = {
    'Human': 'Homo sapiens',
    'human': 'Homo sapiens',
    'H. sapiens': 'Homo sapiens',
    'Mouse': 'Mus musculus',
    'mouse': 'Mus musculus',
    'Rat': 'Rattus norvegicus',
    'rat': 'Rattus norvegicus',
    'E. coli': 'Escherichia coli',
    'HIV-1': 'Human immunodeficiency virus 1',

    # these tend to be the same
    'bovine': 'Bos taurus',
    'Bovine': 'Bos taurus',
    'Porcine': 'Sus scrofa',
    'porcine': 'Sus scrofa',
    'pig': 'Sus scrofa',
    'Pig': 'Sus scrofa',
    'chicken': 'Gallus gallus',
    'Chicken': 'Gallus gallus',
    'barley': 'Hordeum vulgare',
    'Barley': 'Hordeum vulgare',
    'soybean': 'Glycine max',
    'Soybean': "Glycine max",
    'wheat': 'Triticum aestivum',
    'Wheat': 'Triticum aestivum',
    'maize': 'Zea mays',
    'Maize': 'Zea mays',
    'Corn': 'Zea mays',
    'corn': 'Zea mays',
    'Horse': 'Equus caballus',
    'horse': 'Equus caballus'
}

str_replacements = {
    'SARS-CoV': 'Severe acute respiratory syndrome coronavirus',
}
def pl_fix_organism(col: pl.Expr):
    return (
        col.replace(organism_patterns)
    ).str.replace_many(str_replacements)