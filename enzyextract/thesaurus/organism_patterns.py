import polars as pl

# TODO: https://www.uniprot.org/help/taxonomy
organism_patterns = {
    'human': 'Homo sapiens',
    'humans': 'Homo sapiens',
    'H. sapiens': 'Homo sapiens',
    'mouse': 'Mus musculus',
    'mice': 'Mus musculus',
    'rat': 'Rattus norvegicus',
    'rats': 'Rattus norvegicus',
    'E. coli': 'Escherichia coli',
    'E.coli': 'Escherichia coli',
    'HIV-1': 'Human immunodeficiency virus 1',

    # these tend to be the same
    'bovine': 'Bos taurus',
    'porcine': 'Sus scrofa',
    'pig': 'Sus scrofa',
    'pigs': 'Sus scrofa',
    'chicken': 'Gallus gallus',
    'barley': 'Hordeum vulgare',
    'soybean': 'Glycine max',
    'wheat': 'Triticum aestivum',
    'maize': 'Zea mays',
    'corn': 'Zea mays',
    'horse': 'Equus caballus'
}

# auto-add capitalized variant
for k, v in list(organism_patterns.items()):
    if k[0].islower():
        organism_patterns[k.capitalize()] = v

str_replacements = {
    'SARS-CoV': 'Severe acute respiratory syndrome coronavirus',
}
def pl_fix_organism(col: pl.Expr):
    return (
        col.replace(organism_patterns)
    ).str.replace_many(str_replacements)