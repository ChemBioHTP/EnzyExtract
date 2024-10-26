# Use LLM to confirm that the enzyme matches the sequence

import difflib
import os
import re
import time
from typing import Optional

import Bio
import pandas as pd
import yaml

from Bio.Seq import Seq

from kcatextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from kcatextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls



def str_to_set(x):
    if x.startswith("['") and x.endswith("']"):
        x = x[2:-2]
        return set(x.split("', '"))
    return set()

def str_to_splitable(x): # split by ", "
    if x.startswith("['") and x.endswith("']"):
        x = x[2:-2]
        return ', '.join(x.split("', '"))
    elif x == '[]':
        return ''
    return x
    
def load_yaml_enzymes(filepath, yaml_parse=True):
    pmid2yaml = {}
    for custom_id, content, finish_reason in get_batch_output(filepath):
        pmid = str(pmid_from_usual_cid(custom_id))
        
        content = content.replace('\nextras:\n', '\ndata:\n') # blunder

        if finish_reason == 'length':
            print("Too long:", pmid)
            continue
        
        enzymes_re = re.compile(r"^    enzymes: ?$")
        finish_re = re.compile(r"^    [^ ]")

        for _, myyaml in fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=pmid)):
            # just obtain the "enzymes" key
            # obj = yaml.safe_load(myyaml)
            builder = ""
            within = False
            for line in myyaml.split("\n"):
                if enzymes_re.match(line):
                    within = True
                elif within and finish_re.match(line):
                    within = False
                if within:
                    assert line.startswith("    ")
                    builder += line[4:] + "\n" #de-indent
            if yaml_parse:
                try:
                    pmid2yaml[pmid] = yaml.safe_load(builder)
                except yaml.YAMLError as e:
                    print("Error parsing YAML for", pmid)
                    # FIXME: escape yaml
            else:
                pmid2yaml[pmid] = builder
    return pmid2yaml

def construct_window(pmid, yaml, pmid2seq, uniprot_df, pdb_df):
    
    
    results = idents_for_pmid(pmid, pmid2seq, uniprot_df, pdb_df)
    
    builder = ''
    builder += f"\n\n## PMID: {pmid}\n"
    builder += f"""```yaml
{yaml}```
"""
    
    for ident, fasta in results.get('genbank', {}).items():
        builder += f"[FASTA {ident}] {fasta['descriptor']}\n"
    
    for ident, row in results.get('uniprot', {}).items():
        builder += f"[UniProt {ident}] {row['enzyme']} {row['organism']}\n"
    
    for ident, row in results.get('pdb', {}).items():
        builder += f"[PDB {ident}] {row['enzyme']} {row['organism']}\n"
    
    return builder

def read_fasta(fasta_file) -> tuple[str, str]:
    """returns (descriptor, sequence)"""
    with open(fasta_file) as f:
        lines = f.readlines()
    return lines[0].strip(), ''.join(lines[1:]).replace('\n', '')

def idents_for_pmid(pmid, pmid2seq: pd.DataFrame, uniprot_df: pd.DataFrame, pdb_df: pd.DataFrame, ncbi_df: pd.DataFrame):
    # return {pmid: str, uniprots: list[dict], pdbs: list[dict], fastas: list[dict]}
    # each member of uniprots, pdbs, and fastas:
    # {identifier: str, sequence: str, descriptor: str, enzyme: str, organism: str}
    
    if uniprot_df.empty and pdb_df.empty and ncbi_df.empty:
        return None
    
    subset = pmid2seq[pmid2seq['pmid'] == pmid]
    
    subset = subset.replace(pd.NA, '')
    
    uniprots = set()
    pdbs = set()
    refseq = set()
    genbank = set()
    for i, row in subset.iterrows():
        uniprots.update(row['uniprot'].split(', '))
        pdbs.update(row['pdb'].split(', '))
        refseq.update(row['refseq'].split(', '))
        genbank.update(row['genbank'].split(', '))
    
    uniprot_subset = uniprot_df[uniprot_df['uniprot'].isin(uniprots)]
    # pdb_subset = pdb_df[pdb_df['pdb'].isin(pdbs)]
    
    storage = {'refseq': {}, 'genbank': {}, 'uniprot': {}, 'pdb': {}}
    # also look for downloaded fasta
    # also look for genbank
    # for (key, col) in [('refseq', refseq), ('genbank', genbank)]: #], ('uniprot', uniprots)]:
    #     for ident in col:
    #         if os.path.exists(f"fetch_sequences/genbank/{ident}.fasta"):
    #             # print(f"Found {ident}.fasta")
    #             # print the first line
    #             desc, seq = read_fasta(f"fetch_sequences/genbank/{ident}.fasta")
    #             storage[key][ident] = {'sequence': seq, 'descriptor': desc}
    
    refseq_subset = ncbi_df[ncbi_df['ncbi'].isin(refseq)]
    genbank_subset = ncbi_df[ncbi_df['ncbi'].isin(genbank)]
    for ident in refseq:
        rows = refseq_subset[refseq_subset['ncbi'] == ident]
        if not rows.empty:
            row = rows.iloc[0]
            storage['refseq'].setdefault(ident, {})['sequence'] = row['sequence']
            storage['refseq'].setdefault(ident, {})['descriptor'] = row['descriptor']
    for ident in genbank:
        rows = genbank_subset[genbank_subset['ncbi'] == ident]
        if not rows.empty:
            row = rows.iloc[0]
            storage['genbank'].setdefault(ident, {})['sequence'] = row['sequence']
            storage['genbank'].setdefault(ident, {})['descriptor'] = row['descriptor']
    
    for ident in uniprots:
        rows = uniprot_subset[uniprot_subset['uniprot'] == ident]
        # assume unique
        if not rows.empty:
            row = rows.iloc[0]
            storage['uniprot'].setdefault(ident, {})['name'] = row['enzyme']
            storage['uniprot'].setdefault(ident, {})['organism'] = row['organism']
    
    # pdb_subset = pdb_df[pdb_df['pdb'].isin(pdbs)]
    for pdb in pdbs:
        # we can read from tsv
        # subset['pdb'] startswith PDB, to allow for newer version
        if not pdb or len(pdb) < 4:
            continue # skip empty, which is otherwise disastrous
        rows = pdb_df[pdb_df['pdb'].str.lower().str.startswith(pdb.lower())]
        for i, row in rows.iterrows():
            versioned_pdb = row['pdb']
            
            # search for mutant codes
            storage['pdb'][versioned_pdb] = {
                'name': (row['sys_name'] or row['name']),
                'organism': row['organism'],
                'sequence': row['seq_can'] or row['seq'],
                'descriptor': (row['descriptor'] or row['name']) or row['sys_name']
            }
            mutant_codes = grep_mutant_codes(row.get('descriptor', ''))
            storage['pdb'][versioned_pdb]['pdb_mutants'] = mutant_codes
    
    # for ncbi in 

            
    
    # if uniprot_subset.empty and pdb_subset.empty and not fastas:
        # print("No matches found for", pmid)
        # return ''
    if not any(storage.values()):
        return None
    return storage


def string_block_matches(s1, s2) -> list[str]:
    sequence_matcher = difflib.SequenceMatcher(None, s1, s2)
    lcs = []
    for op in sequence_matcher.get_matching_blocks():
        i, j, size = op
        if size > 0:
            lcs.append(s1[i:i+size])
    return lcs # ''.join(lcs)
    
from Bio.Data.IUPACData import protein_letters_3to1, extended_protein_letters

def grep_mutant_codes(long_descriptor: str):
    return parse_mutant_codes(re.findall(r"\b[A-Z]\d{2,4}[A-Z]\b", long_descriptor))
    

def parse_mutant_codes(mutants: list[str]) -> list[tuple[str, int, str]]:
    amino1 = extended_protein_letters # "ACDEFGHIKLMNPQRSTVWY"
    amino3 = protein_letters_3to1.keys()
    
    # mut_code_re = re.compile(r"()(\d+)([A-Z])")
    # parse 
    point_muts = []
    for desc in mutants:
        for mut in desc.split("/"):
            if len(mut) < 3:
                continue
            start_amino = None
            end_amino = None
            is_amino3 = False
            if mut[0] in amino1 and mut[1].isdigit():
                start_amino = mut[0]
                mut = mut[1:]
            elif mut[:3] in amino3:
                start_amino = protein_letters_3to1[mut[:3]]
                mut = mut[3:]
                is_amino3 = True
            else:
                continue
            
            if not len(mut):
                continue
            
            if len(mut) == 1:
                # single digit mutation
                if mut[0].isdigit():
                    end_amino = None
                    mut = mut
                else:
                    continue # BAD
            elif mut[-1] in amino1 and mut[-2].isdigit():
                end_amino = mut[-1]
                mut = mut[:-1]
            elif mut[-3:] in amino3:
                end_amino = protein_letters_3to1[mut[-3:]]
                mut = mut[:-3]
                is_amino3 = True
            else:
                pass
            
            if start_amino is None or not mut.isdigit() or (end_amino is None and not is_amino3): # or end_amino is None 
                # allow for unspecified tail mutation if it's amino 3
                continue    
            point_muts.append((start_amino, int(mut), end_amino))
    return point_muts

MutantMatcher = list[tuple[str, int, str]]

def form_mutant_matcher(mutants: list[str]) -> MutantMatcher:
    if isinstance(mutants, str):
        if '; ' in mutants:
            mutants = mutants.split('; ')
        else:
            mutants = mutants.split(', ')
    codes = parse_mutant_codes(mutants)
    if not codes:
        return None
    # min_point = min(x[1] for x in codes)
    # max_point = max(x[1] for x in codes)
    # for x in codes:
    #     x[1] -= min_point
    # sort by position
    codes.sort(key=lambda x: x[1])
    return codes

def closest_match(sequence: str, matcher: MutantMatcher) -> int:
    if not matcher:
        return -1, -1
    first_char, min_point, _ = matcher[0]
    target_index = min_point - 1
    min_distance = len(sequence) + 1
    min_i = -1
    i = -1
    while i < len(sequence):
        i = sequence.find(first_char, i+1)
        if i == -1 or i >= len(sequence):
            break
        
        # now check that the rest of the sequence matches
        for find, pos, repl in matcher:
            if i + pos >= len(sequence):
                break
            if sequence[i + pos] != find:
                break
        else:
            # all matched
            distance = abs(i - target_index)
            if distance < min_distance:
                min_distance = distance
                min_i = i
            elif i > target_index:
                break
    return min_i, target_index
        
def closest_dict_match(sequence: str, codes_dict: dict, min_point: int):
    if not codes_dict:
        return None, -1
    starter = codes_dict[min_point]
    closest = None
    target_idx = min_point - 1
    for i, anchor in enumerate(sequence):
        if anchor not in starter:
            continue
        # now look at every offset
        for point, desired in codes_dict.items():
            offset = point - min_point
            if i + offset >= len(sequence):
                # break early; no other sequence will be valid
                return closest, target_idx
            char = sequence[i + offset]
            if char not in desired:
                break
        else:
            # matches them all
            if closest is None or abs(i - target_idx) < abs(closest - target_idx):
                closest = i
    return closest, target_idx
            
                
            
        
        

def sequence_search_regex(mutants: list[str]):
    """Given a list of mutant codes (ie. R123A), return a regex that matches a fasta sequence
    Search the fasta sequence for the regex. If the string matches the regex at index desired_index, then the enzyme is corroborated.
    """
    codes = form_mutant_matcher(mutants)
    if not codes:
        return None
    return to_regex(codes)
    

def to_matcher_dict(codes: MutantMatcher, allow_mut=False):
    
    result = {}
    if not codes:
        return result
    first_char, min_point, _ = codes[0]
    for first_char, point, last_char in codes:
        if first_char not in result.setdefault(point, ''):
            result[point] += first_char
        if allow_mut:
            if last_char not in result.setdefault(point, ''):
                result[point] += last_char
    return result, min_point

def to_regex(codes: MutantMatcher, allow_mut: bool | MutantMatcher=False) -> tuple[str, int | None]:
    if not codes:
        return '', None
    min_point, max_point = codes[0][1], codes[-1][1]
    if allow_mut:
        regex = [set() for _ in range(max_point - min_point + 1)]
    else:
        regex = ['.'] * (max_point - min_point + 1)
    for start, pos, end in codes:
        if allow_mut == True:
            regex[pos - min_point].add(start)
            regex[pos - min_point].add(end)
        elif bool(allow_mut):
            # only allow a few specific mutants
            regex[pos - min_point].add(start)

        else:
            regex[pos - min_point] = start
    desired_index = min_point - 1
    if isinstance(allow_mut, list):
        # allow_mut is a MutantMatcher
        for first_char, pos, last_char in allow_mut:
            if not (0 <= pos - min_point < len(regex)):
                continue
            if first_char in regex[pos - min_point]:
                regex[pos - min_point].add(last_char) # add the finale of the mutation to the mix
    if allow_mut:
        return ''.join(('.' if not myset else f'[{"".join(sorted(myset))}]') for myset in regex), desired_index
    return ''.join(regex), desired_index

        

def does_sequence_corroborate(codes: MutantMatcher | tuple[str, int], sequence: Optional[str]=None, allow_mut=False) -> tuple[int, int, str]:
    """Should return (index, target_index, sequence)"""
    
    if all(x in 'CAGT' for x in sequence):
        # it's a DNA sequence
        # convert to protein
        # dna_seq = Seq(sequence)
        if len(sequence) % 3 != 0:
            # print("DNA sequence not divisible by 3")
            # add trailing N
            sequence += 'N' * (3 - len(sequence) % 3)
            # return False
        sequence = Bio.Seq.translate(sequence)
    # mutants = enzyme.get('mutants')
    # if not mutants:
    #     return None
    # find the closest match
    # codes = form_mutant_matcher(mutants)
    # if not codes:
    #     return None
    
    # code_dict, min_point = to_matcher_dict(codes, allow_mut=allow_mut)
    # regex is actually faster ;-;
    # return closest_dict_match(sequence, code_dict, min_point)
    if isinstance(codes, tuple):
        desire, target = codes
    else:
        desire, target = to_regex(codes, allow_mut=allow_mut)
    regex = rf"(?=({desire}))"
    
    # i, target = closest_match(sequence, codes)
    # target = codes[0][1] - 1
    matches = [m.start() for m in re.finditer(regex, sequence)]
    
    if not matches: # i == -1:
        return None, -1, ""
    
    i = min(matches, key=lambda x: abs(x - target))

    return i, target, sequence

    # simple_regex, target = sequence_search_regex(mutants)
    # # get indices where sequence matches regex
    # if simple_regex:
    #     regex = rf"(?=({simple_regex}))"
    #     matches = [m.start() for m in re.finditer(regex, sequence)]
    #     for match in matches:
    #         fit = abs(match - target)
            
                # points += 1
    
    
    
        
        




def does_enzyme_corroborate(enzyme: dict, descriptor: str=None, name: Optional[str]=None, organism: Optional[str]=None, sequence: Optional[str]=None) -> bool:
    # enzyme has these fields: organisms, fullname, synonyms
    if name is None and organism is None:
        name = descriptor
        organism = descriptor
    
    # the organism matches if there is the combined common diff-block has size >max(5, 0.4 * len(organisms))
    # the name matches if there is the combined common diff-block has size >max(5, 0.4 * len(fullname))
    
    organism_matches = string_block_matches(enzyme['organisms'].lower(), organism.lower())
    name_matches = string_block_matches(enzyme['fullname'].lower(), name.lower())
    
    # return len(organism_matches) > max(5, 0.4 * len(enzyme['organisms'])) and len(name_matches) > max(5, 0.4 * len(enzyme['fullname']))
    org_match_len = sum(len(x) for x in organism_matches)
    name_match_len = sum(len(x) for x in name_matches)
    
    points = 0
    if not org_match_len:
        pass # short circuit the others
    elif org_match_len == len(enzyme['organisms']):
        # perfect match
        points += 3
    elif org_match_len > max(0.8 * len(enzyme['organisms']), 5):
        # very good match
        points += 2
    elif org_match_len > max(0.4 * len(enzyme['organisms']), 5):
        # good match
        points += 1
    elif max(len(x) for x in organism_matches) > 10:
        # strong contiguous match
        points += 1
        
    if not name_match_len:
        pass # short circuit
    elif name_match_len == len(enzyme['fullname']):
        # perfect match
        points += 3
    elif name_match_len > max(0.8 * len(enzyme['fullname']), 5):
        # very good match
        points += 2
    elif name_match_len > max(0.4 * len(enzyme['fullname']), 5):
        # good match
        points += 1
    elif max((len(x) for x in name_matches), default=0) > 10:
        # strong contiguous match
        points += 1
    
    
    
    if points >= 2:
        return True
    return False



def script0(use_gpt=False, allow_mut=False):
    
    start = time.time()
    pmid2seq = pd.read_csv("fetch_sequences/results/rekcat_enzymes.tsv", sep="\t")

    pmid2seq['pmid'] = pmid2seq['filename'].apply(lambda x: x.rsplit(".pdf", 1)[0])
    
    if use_gpt:
        compl_folder = 'completions/enzy'
        filename, src_version = locate_correct_batch(src_folder=compl_folder, 
                                                     namespace='tableless-oneshot', version=None) # , version=1)
        
        pmid2yaml = load_yaml_enzymes(f'{compl_folder}/{filename}')
        
        # 18162462
        filename, src_version = locate_correct_batch(src_folder=compl_folder,
                                                    namespace='tabled-oneshot-tuned', version=None)
        pmid2yaml.update(load_yaml_enzymes(f'{compl_folder}/{filename}'))
        
    else:
        pmid2mutants = pd.read_csv("fetch_sequences/results/rekcat_mutants.tsv", sep="\t")
        pmid2mutants['pmid'] = pmid2mutants['filename'].apply(lambda x: x.rsplit(".pdf", 1)[0])
    
    uniprot_df = pd.read_csv("fetch_sequences/results/rekcat_uniprots.tsv", sep="\t")
    pdb_df = pd.read_csv("fetch_sequences/results/rekcat_pdbs.tsv", sep="\t")

    # print first 5 pmids   
    pmid2seq.replace('', pd.NA, inplace=True)
    pmid2seq.dropna(subset=['pdb', 'uniprot', 'refseq', 'genbank'], how='all', inplace=True)
    pmid2seq.replace(pd.NA, '', inplace=True)
    # not_all_null = pmid2seq.filter(['pdb', 'uniprot', 'refseq', 'genbank']).applymap(lambda x: len(x) > 0).any(axis=1)


    candidates = pmid2seq['pmid'].unique().tolist()

    print("These many candidates: ", len(candidates))
    
    # log = ""
    builder = []
    # ct = 10
    
    _num_attempts = 0
    _num_found = 0
    for pmid in candidates:
        db_idents = idents_for_pmid(pmid, pmid2seq, uniprot_df, pdb_df) or {}
        # pretty print
        # if result:
        #     print(json.dumps(result, indent=2))
        if use_gpt:
            inyaml = pmid2yaml.get(pmid)
            if not inyaml: # or not db_idents:
                continue
            enzymes_block = inyaml.get('enzymes', [])
            if not enzymes_block:
                continue
        else:
            # construct from results_mutants
            inyaml = pmid2mutants[pmid2mutants['pmid'] == pmid]
            if inyaml.empty or pd.isna(inyaml.iloc[0]['mutants']): # or not db_idents 
                continue
            enzymes_block = [{
                'fullname': '???',
                'mutants': (inyaml.iloc[0]['mutants'] or '').split('; ')
            }]
            
        
        if not any('mutants' in x for x in enzymes_block):
            continue
        attempted = False
        found = False
        for enzyme in enzymes_block:
            mutants = enzyme.get('mutants')
            if not mutants:
                continue
            # find the closest match
            codes = form_mutant_matcher(mutants)
            if not codes:
                continue
            og_desire, og_target = to_regex(codes, allow_mut=allow_mut)
            
            closest = None
            closest_ident = None
            closest_target = None
            closest_sequence = None
            for key, collection in sorted(db_idents.items()):
                
                    
                for ident, data in sorted(collection.items()):
                    if not data.get('sequence'):
                        continue
                    attempted = True
                    
                    if key == 'pdb' and (_my_muts := data['pdb_mutants']):
                        # special logic for pdb mutant codes
                        desire, target = to_regex(codes, allow_mut=_my_muts)
                    else:
                        desire, mytarget = og_desire, og_target
                    
                    i, target, sequence = does_sequence_corroborate((desire, mytarget), data['sequence'], allow_mut=allow_mut) # enzyme
                    if i is not None:
                        assert target != -1
                        distance = abs(i - target)
                        if isinstance(distance, int):
                            if closest is None or distance < abs(closest - closest_target):
                                closest = i
                                closest_ident = ident
                                closest_target = target
                                closest_sequence = sequence
                    if closest == -1:
                        print("wtf happened?")    
            if closest is not None:
                # log += f"[{pmid}] "
                found = True
                distance = abs(closest - closest_target)
                # if distance == 0:
                #     log += "Perfect "
                # else:
                #     log += f"Off by {distance} "
                # log += f"Matched {desire} with {closest_ident} in {enzyme['fullname']} at {i}\n"
                builder.append((pmid, distance, closest_ident, enzyme['fullname'], og_desire, closest_target, closest, closest_sequence))
                # print(f"Sequence: {sequence[i-10:i+10]}")
            else:
                # log += f"[{pmid}] No match for {desire}\n"
                # bulder.append((pmid, None, None, desire, target, -1))
                # if no match, give the desire
                # (this may occur if no pdb or uniprot or genbank is found)
                builder.append((pmid, -1, None, enzyme['fullname'], og_desire, og_target, -1, None))
                pass

        
        if found:
            _num_found += 1
        if attempted:
            _num_attempts += 1
        # ct -= 1
        # if ct == 0:
            # break
    dists_df = pd.DataFrame(builder, columns=['pmid', 'distance', 'ident', 'enzyme_name', 'desire', 'target', 'index', 'sequence'])
    print("This many with mutants and sequences", _num_attempts)
    print("This many found", _num_found)
    print("This many in the builder", len(builder))
    
    dists_df.to_csv("fetch_sequences/enzymes/rekcat_mutant_distances_gpt.tsv", sep="\t", index=False)
    
    end = time.time()
    print("Time: ", end-start) # regex: 118s
    exit(0)
if __name__ == "__main__":
    
    script0(use_gpt=True, allow_mut=False)
    
    mutants = ["Ala110Arg", "R120Z/R123W", "S124W", "Y126A"]
    out = sequence_search_regex(mutants)
    assert out == ("A.........R..RS.Y", 109), out
    print("All tests pass")