
from enzyextract.dependency.prereqs import export
from enzyextract.pipeline.accessions.step1_regroup_accessions import get_known_accessions


@export("data/enzymes/accessions/final/pdb.parquet")
@export("data/enzymes/accessions/final/uniprot.parquet")
@export("data/enzymes/accessions/final/ncbi.parquet")
def finalize_accessions():
    pdb_known, uniprot_known, ncbi_known = get_known_accessions()
    pdb_known.write_parquet('data/enzymes/accessions/final/pdb.parquet')
    uniprot_known.write_parquet('data/enzymes/accessions/final/uniprot.parquet')
    ncbi_known.write_parquet('data/enzymes/accessions/final/ncbi.parquet')

    print("Finalized accessions")
    print("data/enzymes/accessions/final/pdb.parquet")
    print("data/enzymes/accessions/final/uniprot.parquet")
    print("data/enzymes/accessions/final/ncbi.parquet")

    # refseq_known = read_all_dfs('data/enzymes/accessions/refsesq', so=so)

if __name__ == '__main__':
    # get_unknown_accessions()
    finalize_accessions()