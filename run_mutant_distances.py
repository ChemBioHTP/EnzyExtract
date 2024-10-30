
import os
import pickle
import time

import pandas as pd
from tqdm import tqdm

from kcatextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from kcatextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls

from kcatextract.fetch_sequences.confirm_enzyme_sequences import idents_for_pmid, load_yaml_enzymes, form_mutant_matcher, to_regex, does_sequence_corroborate, sequence_search_regex
from kcatextract.fetch_sequences.confirm_enzyme_sequences import MutantMatcher

def load_df_from_folder(dirpath):
    ret = []
    for filename in os.listdir(dirpath):
        if filename.endswith(".tsv"):
            df = pd.read_csv(f"{dirpath}/{filename}", sep="\t")
            ret.append(df)
    return pd.concat(ret)

def _get_pmid2stuff(namespace, gpt_namespace, use_gpt):
    if use_gpt:
        if not gpt_namespace:
            raise ValueError("gpt_namespace must be provided")
        
        pmid2yaml = load_yaml_enzymes(gpt_namespace)
        pmid2mutants = {}
        # legacy
        # compl_folder = 'completions/enzy'
        # if True:
        #     # dumb stuff
        #     _compl_folder = 'C:/conjunct/table_eval/completions/enzy'
        #     filename, src_version = locate_correct_batch(src_folder=_compl_folder,
        #                                                 namespace='tableless-oneshot', version=None)
            
        #     pmid2yaml = load_yaml_enzymes(f'{_compl_folder}/{filename}')
        
        # # 18162462
        # filename, src_version = locate_correct_batch(src_folder=compl_folder, 
        #                             namespace=gpt_namespace, version=None) # , version=1)
        # pmid2yaml.update(load_yaml_enzymes(f'{compl_folder}/{filename}'))
        
    else:
        pmid2mutants = pd.read_csv(f"fetch_sequences/mutants/{namespace}_mutants.tsv", sep="\t")
        pmid2mutants['pmid'] = pmid2mutants['filename'].apply(lambda x: x.rsplit(".pdf", 1)[0])
        pmid2yaml = {}
    return pmid2yaml, pmid2mutants

def _get_enzymes_with_mutants(pmid, use_gpt, pmid2yaml=None, pmid2mutants=None):
    if use_gpt:
        inyaml = pmid2yaml.get(pmid)
        if not inyaml: # or not db_idents:
            return []
        enzymes_block = inyaml.get('enzymes', [])
        if not enzymes_block:
            return []
    else:
        # construct from results_mutants
        inyaml = pmid2mutants[pmid2mutants['pmid'] == pmid]
        if inyaml.empty or pd.isna(inyaml.iloc[0]['mutants']): # or not db_idents 
            return []
        enzymes_block = [{
            'fullname': '???',
            'mutants': (inyaml.iloc[0]['mutants'] or '').split('; ')
        }]
        
    
    if not any('mutants' in x for x in enzymes_block):
        return []
    return enzymes_block

def _search_idents_by_name(search_enzymes_by_name: pd.DataFrame, enzyme: dict):
    """
    search_enzymes_by_name: a dataframe with columns 'query_enzyme', 'query_organism', 'accession', 'sequence'
    enzyme: an enzyme straight from the yaml. needs 'fullname', 'name', 'organisms'
    """
    name = enzyme.get('fullname', enzyme.get('name'))
    organisms = enzyme.get('organisms')
    if not organisms:
        return {}
    if '; ' in organisms:
        organisms = organisms.split('; ')
    else:
        organisms = organisms.split(', ')
    if not name or not organisms or search_enzymes_by_name.empty:
        return {}
    # search by name and organism
    rows = search_enzymes_by_name[search_enzymes_by_name['query_enzyme'] == name]
    # filter by organism
    rows = rows[rows['query_organism'].apply(lambda x: any(y in x for y in organisms))]
    if rows.empty:
        return {}
    _bdr = {}
    for _, row in rows.iterrows():
        _bdr[row['accession']] = {'sequence': row['sequence']}
    db_idents = {'uniprot': _bdr}
    return db_idents

def _find_closest_match(db_idents: dict[str, dict[str, dict]], codes: MutantMatcher, 
                        og_desire, og_target, 
                        allow_mut=False):
    """
    """
    closest_idx = None
    closest_ident = None
    closest_target = None
    closest_sequence = None
    closest_distance = None
    _attempted = False
    for key, collection in sorted(db_idents.items()):
        for ident, data in sorted(collection.items()):
            if not data.get('sequence'):
                continue
            _attempted = True
            
            if key == 'pdb' and (_my_muts := data['pdb_mutants']):
                # for pdb mutant codes, may need to recalculate based on mutant codes
                desire, target = to_regex(codes, allow_mut=_my_muts)
            else:
                desire, target = og_desire, og_target
                
            target = max(target, 0) # sometimes, the mutant code will be 0-indexed like Y0T \shrug
            
            i, _, sequence = does_sequence_corroborate((desire, target), data['sequence'], allow_mut=allow_mut) # enzyme
            if i is not None:
                assert target != -1
                distance = abs(i - target)
                if isinstance(distance, int):
                    if closest_idx is None or distance < closest_distance: #abs(closest - closest_target):
                        closest_idx = i
                        closest_ident = ident
                        closest_target = target
                        closest_sequence = sequence
                        closest_distance = distance
                    if distance == 0:
                        break
            if closest_idx == -1:
                print("wtf happened?")    
        if closest_distance == 0:
            break
    if closest_idx is not None:
        return closest_idx, closest_distance, closest_ident, closest_target, closest_sequence, _attempted
    return -1, -1, None, og_target, None, _attempted
    # log += f"[{pmid}] No match for {desire}\n"
    # bulder.append((pmid, None, None, desire, target, -1))
    # if no match, give the desire
    # (this may occur if no pdb or uniprot or genbank is found)

def script0(namespace, uniprot_df, pdb_df, ncbi_df, *, use_gpt=False, gpt_namespace=None, allow_mut=False, write_dest=None,
            search_enzymes_by_name: pd.DataFrame=None, 
            use_pkl_dir='_debug/pkl/nonbrenda.pkl',
            pmid2seq: pd.DataFrame=None
    ):
    
    # namespace = 'rekcat'    
    # gpt_namespace = 'brenda-rekcat-t2neboth' # 'tableless-oneshot'
    
    if use_pkl_dir and os.path.exists(use_pkl_dir):
        with open(use_pkl_dir, 'rb') as f:
            pmid2yaml = pickle.load(f)
        pmid2mutants = {}
    else:
        pmid2yaml, pmid2mutants = _get_pmid2stuff(namespace, gpt_namespace, use_gpt)
        with open(use_pkl_dir, 'wb') as f:
            pickle.dump(pmid2yaml, f)
    
    if pmid2seq is None:
        pmid2seq = pd.read_csv(f"fetch_sequences/readpdf/{namespace}_enzymes.tsv", sep="\t", dtype=str)
        pmid2seq['pmid'] = pmid2seq['filename'].apply(lambda x: x.rsplit(".pdf", 1)[0])

    # not_all_null = pmid2seq.filter(['pdb', 'uniprot', 'refseq', 'genbank']).applymap(lambda x: len(x) > 0).any(axis=1)

    candidates = pmid2seq['pmid'].unique().tolist()

    print("These many pmids with sequences: ", len(candidates))
    print("These many pmids with yaml: ", len(pmid2yaml))
    # log = ""
    builder = []
    # ct = 10
    
    _num_attempts = 0
    _num_found = 0
    _num_mutant_codes = 0
    for pmid in tqdm(candidates):
        db_idents = idents_for_pmid(pmid, pmid2seq, uniprot_df, pdb_df, ncbi_df) or {}
        
        enzymes_block = _get_enzymes_with_mutants(pmid, pmid2yaml=pmid2yaml, pmid2mutants=pmid2mutants, use_gpt=use_gpt)
        if not enzymes_block:
            continue
        
        attempted = False
        found = False
        has_mutants = False
        has_sequence = False
        for enzyme in enzymes_block:
            mutants = enzyme.get('mutants')
            if not mutants:
                continue
            # find the closest match
            codes = form_mutant_matcher(mutants)
            if not codes:
                continue
            
            has_mutants = True
            if not db_idents:
                # search by name
                db_idents = _search_idents_by_name(search_enzymes_by_name, enzyme)
                
            
            og_desire, og_target = to_regex(codes, allow_mut=allow_mut)
            closest_idx, closest_distance, closest_ident, closest_target, closest_sequence, _attempted = _find_closest_match(db_idents, codes, og_desire, og_target, allow_mut=allow_mut)
            builder.append((pmid, closest_distance, closest_ident, enzyme['fullname'], og_desire, closest_target, closest_idx, closest_sequence))
            # print(f"Sequence: {sequence[i-10:i+10]}")
            found = found or (closest_idx != -1)
            attempted = attempted or _attempted

        if found:
            _num_found += 1
        if attempted:
            _num_attempts += 1
        if has_mutants:
            _num_mutant_codes += 1
    dists_df = pd.DataFrame(builder, columns=['pmid', 'distance', 'ident', 'enzyme_name', 'desire', 'target', 'index', 'sequence'])
    print("This many in the builder", len(builder))
    print("This many with mutant codes", _num_mutant_codes)
    print("This many with mutants and sequences", _num_attempts)
    print("This many found", _num_found)
    

    
    if write_dest is None:
        gpt_part = "_gpt" if use_gpt else ""
        write_dest = f"fetch_sequences/enzymes/{namespace}_mutant_distances{gpt_part}.tsv"
    dists_df.to_csv(write_dest, sep="\t", index=False)
    
    exit(0)
if __name__ == "__main__":
    
    unidf = load_df_from_folder("fetch_sequences/enzymatch")
    
    namespace = 'nonbrenda'
    uniprot_df = pd.read_csv(f"fetch_sequences/results/uniprot_fragments/{namespace}_uniprots.tsv", sep="\t") \
        .head(0)
    pdb_df = pd.read_csv(f"fetch_sequences/results/pdb_fragments/{namespace}_pdbs.tsv", sep="\t") \
        .head(0) # empty
    ncbi_df = pd.read_csv(f"fetch_sequences/results/ncbi_fragments/{namespace}_ncbis.tsv", sep="\t") \
        .head(0) # empty
    
    
    script0(namespace, uniprot_df=uniprot_df, pdb_df=pdb_df, ncbi_df=ncbi_df, 
            use_gpt=True, allow_mut=False, gpt_namespace='data/_compiled/nonbrenda.jsonl',
            search_enzymes_by_name=unidf,
            write_dest=f"fetch_sequences/enzymes/nonbrenda_mutant_distances_by_name2.tsv")
    
    # mutants = ["Ala110Arg", "R120Z/R123W", "S124W", "Y126A"]
    # out = sequence_search_regex(mutants)
    # assert out == ("A.........R..RS.Y", 109), out
    # print("All tests pass")