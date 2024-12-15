"""
Prerequisites:
run_read_pdfs_for_idents.py
"""
import os
import pickle
import time
from typing import TypedDict

import pandas as pd
from tqdm import tqdm

from enzyextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls

from enzyextract.fetch_sequences.confirm_enzyme_sequences import idents_for_pmid, load_yaml_enzymes, form_mutant_codes, to_regex, does_sequence_corroborate, sequence_search_regex, MutantCodes, to_amino_sequence, find_offset, find_p, find_p_offset

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

class _SequenceDict(TypedDict):
    sequence: str
class DBIdents(TypedDict):
    """
    dict of accession to dict of sequence
    """
    uniprot: dict[str, dict[str, _SequenceDict]]
    pdb: dict[str, dict[str, _SequenceDict]]
    genbank: dict[str, dict[str, _SequenceDict]]

    

def _search_idents_by_name(search_enzymes_by_name: pd.DataFrame, enzyme: dict) -> DBIdents:
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

def _find_closest_match(db_idents: dict[str, dict[str, dict]], codes: MutantCodes, 
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
            
            ## approach 1
            # if key == 'pdb' and (_my_muts := data.get('pdb_mutants', None)):
            #     # for pdb mutant codes, may need to recalculate based on mutant codes
            #     desire, target = to_regex(codes, allow_mut=_my_muts)
            # else:
            #     desire, target = og_desire, og_target
            # target = max(target, 0) # sometimes, the mutant code will be 0-indexed like Y0T \shrug
            # i, _, sequence = does_sequence_corroborate((desire, target), data['sequence'], allow_mut=allow_mut) # enzyme

            ## approach 2
            sequence = to_amino_sequence(data['sequence'])
            target = max(min(pos for _, pos, _ in codes) - 1, 0)
            offset = find_offset(sequence, codes)
            if offset is None:
                continue
            i = target + offset
            
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
            pmid2seq: pd.DataFrame=None,
            pmids: list[str]=None
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
    # builder = []
    builder = {'pmid': [], 
               'provenance': [], 
               'distance': [], 
               'ident': [], 
               'enzyme_name': [], 
               'desire': [], 
               'target': [], 
               'index': [], 
               'sequence': [],
               'N': [],
               'k': [],
               'P_any': [],
               'P_better': []}

    # ct = 10
    
    _num_attempts = 0
    _num_found = 0
    _num_mutant_codes = 0

    if 'pdb_unversioned' not in pdb_df.columns:
        pdb_df['pdb_unversioned'] = pdb_df['pdb'].str.split('_').str[0].str.lower()
    for pmid in tqdm(pmids): # tqdm(candidates):
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
            codes = form_mutant_codes(mutants)
            if not codes:
                continue
            
            has_mutants = True
            og_desire, og_target = to_regex(codes, allow_mut=allow_mut)
            # if not db_idents:
                # search by name
                # db_idents = _search_idents_by_name(search_enzymes_by_name, enzyme)
                
            
            provenance = None
            att = None
            ns = []
            _unique_codes = set(x[1] for x in codes)
            k = len(_unique_codes)
            if db_idents:
                att = _find_closest_match(db_idents, codes, og_desire, og_target, allow_mut=allow_mut)
                provenance = 'referenced'
                for key, collection in db_idents.items():
                    for seq in collection.values():
                        if 'sequence' in seq:
                            ns.append(len(seq['sequence']))
            if att is None or att[0] == -1:
                # try again, with the uniprot search
                db_idents_searched = _search_idents_by_name(search_enzymes_by_name, enzyme)
                att = _find_closest_match(db_idents_searched, codes, og_desire, og_target, allow_mut=allow_mut)
                provenance = 'searched'
                for key, collection in db_idents_searched.items():
                    for seq in collection.values():
                        if 'sequence' in seq:
                            ns.append(len(seq['sequence']))

            closest_idx, closest_distance, closest_ident, closest_target, closest_sequence, _attempted = att
            # builder.append((pmid, provenance, closest_distance, closest_ident, enzyme['fullname'], og_desire, closest_target, closest_idx, closest_sequence))
            builder['pmid'].append(pmid)
            builder['provenance'].append(provenance)
            builder['distance'].append(closest_distance) # att[1])
            builder['ident'].append(closest_ident) # att[2])
            builder['enzyme_name'].append(enzyme['fullname'])
            builder['desire'].append(og_desire)
            builder['target'].append(closest_target) # att[3])
            builder['index'].append(att[0])
            builder['sequence'].append(closest_sequence) # att[4])

            builder['N'].append(len(ns))
            builder['k'].append(k)
            if closest_distance == -1:
                builder['P_any'].append(None)
                builder['P_better'].append(None)
            else:
                P_any = find_p(ns, k, 20)
                P_better = 1 - (1 - find_p_offset(k, 20, closest_distance)) ** len(ns)
                builder['P_any'].append(P_any)
                builder['P_better'].append(P_better)

            # P 
            # print(f"Sequence: {sequence[i-10:i+10]}")
            found = found or (closest_idx != -1)
            attempted = attempted or _attempted

        if found:
            _num_found += 1
        if attempted:
            _num_attempts += 1
        if has_mutants:
            _num_mutant_codes += 1
    dists_df = pd.DataFrame(builder) # , columns=['pmid', 'provenance', 'distance', 'ident', 'enzyme_name', 'desire', 'target', 'index', 'sequence'])
    print("This many in the builder", len(builder['pmid']))
    print("This many with mutant codes", _num_mutant_codes)
    print("This many with mutants and sequences", _num_attempts)
    print("This many found", _num_found)
    

    
    if write_dest is None:
        gpt_part = "_gpt" if use_gpt else ""
        write_dest = f"fetch_sequences/enzymes/{namespace}_mutant_distances{gpt_part}.tsv"
    dists_df.to_csv(write_dest, sep="\t", index=False)
    
    exit(0)
if __name__ == "__main__":
    
    namespace = 'apogee-nonbrenda'
    unidf = load_df_from_folder(f"fetch_sequences/uniprot/{namespace}")
    
    uniprot_df = pd.read_csv(f"fetch_sequences/results/uniprot_fragments/{namespace}_uniprots.tsv", sep="\t") \
        # .head(0)
    pdb_df = pd.read_csv(f"fetch_sequences/results/pdb_fragments/{namespace}_pdbs.tsv", sep="\t") \
        # .head(0) # empty
    ncbi_df = pd.read_csv(f"fetch_sequences/results/ncbi_fragments/{namespace}_ncbis.tsv", sep="\t") \
        # .head(0) # empty
    

    reference_df = pd.read_csv(f"data/_compiled/apogee-nonbrenda.tsv", sep="\t", dtype={'pmid': str})
    pmids = reference_df['pmid'].unique().tolist()
    script0(namespace, uniprot_df=uniprot_df, pdb_df=pdb_df, ncbi_df=ncbi_df, 
            use_gpt=True, allow_mut=False, gpt_namespace='data/_compiled/apogee-nonbrenda.jsonl',
            search_enzymes_by_name=unidf,
            pmids=pmids,
            write_dest=f"fetch_sequences/enzymes/apogee_nonbrenda_mutant_sequences.tsv")
    
    # mutants = ["Ala110Arg", "R120Z/R123W", "S124W", "Y126A"]
    # out = sequence_search_regex(mutants)
    # assert out == ("A.........R..RS.Y", 109), out
    # print("All tests pass")