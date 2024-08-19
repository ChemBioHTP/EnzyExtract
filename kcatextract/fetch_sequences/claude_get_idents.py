import json
import pandas as pd
import requests
from collections import defaultdict

# def parse_jsonl(file_path):
#     with open(file_path, 'r') as file:
#         return [json.loads(line) for line in file]

# def group_identifiers(data):
#     grouped = defaultdict(lambda: defaultdict(set))
#     for entry in data:
#         for id_type, ids in entry.items():
#             if id_type != 'pmid':
#                 for id in ids:
#                     grouped[id_type][id].add(entry['pmid'])
#     return grouped


def _fetch_uniprot_info(uniprot_ids):
    # Use UniProt API to fetch information in bulk
    url = "https://rest.uniprot.org/uniprotkb/search"
    query = " OR ".join(uniprot_ids)
    params = {
        "query": query,
        "fields": "accession,protein_name,organism_name",
        "format": "json"
    }
    response = requests.get(url, params=params)
    return response.json()


def fetch_uniprots(uniprot_ids: list):
    results = []
    uniprot_info = _fetch_uniprot_info(uniprot_ids)
    if 'results' not in uniprot_info:
        print("Unexpected format")
        print(uniprot_info)
    for entry in uniprot_info['results']:
        uniprot_id = entry.get('primaryAccession', "not found")
        enzyme = entry.get('proteinDescription', {}).get('recommendedName', {}).get('fullName', {}).get('value', "not found")
        organism = entry.get('organism', {}).get('scientificName', "not found")
        results.append({'uniprot': uniprot_id, 'enzyme': enzyme, 'organism': organism})
    # to df
    df = pd.DataFrame(results)
    return df


def fetch_pdb_response(pdb_ids):
    # Use PDB API to fetch information in bulk
    url = f"https://data.rcsb.org/graphql"
    query = """
    query($ids: [String!]!) {
      entries(entry_ids: $ids) {
        rcsb_id
        struct {
          title
        }
        struct_keywords {
          text
        }
        citation {
          pdbx_database_id_DOI,
          pdbx_database_id_PubMed
        }
        polymer_entities {
          entity_poly {
            pdbx_seq_one_letter_code
            pdbx_seq_one_letter_code_can
          }
          rcsb_id
          rcsb_entity_source_organism {
            scientific_name
          }
          rcsb_polymer_entity_name_com {
            name
          }
          rcsb_polymer_entity_name_sys {
            name
          }
        }
      }
    }
    """
        #     rcsb_entity_source_organism {
        #   scientific_name
        # }
    response = requests.post(url, json={'query': query, 'variables': {'ids': list(pdb_ids)}})
    return response.json()

def fetch_pdbs(pdb_ids) -> pd.DataFrame:
    pdb_info = fetch_pdb_response(pdb_ids)
    result = []
    for entry in pdb_info['data']['entries']:
        pdb_id = entry['rcsb_id']
        descriptor = entry['struct']['title']
        info = entry['struct_keywords'].get('text', "")
        dois = []
        pmids = []
        if entry.get('citation'):
            for citation in entry['citation']:
                if not citation:
                    continue
                if citation.get('pdbx_database_id_DOI'):
                    dois.append(citation['pdbx_database_id_DOI'])
                if citation.get('pdbx_database_id_PubMed'):
                    pmids.append(citation['pdbx_database_id_PubMed'])
        
        # origins = ids[pdb_id]
        # organism = entry['rcsb_entity_source_organism'][0]['scientific_name']
        for entity in entry.get('polymer_entities') or []:
            pdb_id = entity['rcsb_id'] or pdb_id # prefer the polymer entity pdb id
            
            multi_names = {}
            for k, k2, writeas in [('rcsb_polymer_entity_name_com', 'name', 'name'), 
                          ('rcsb_polymer_entity_name_sys', 'name', 'sys_name'),
                          ('rcsb_entity_source_organism', 'scientific_name', 'organism')]:
                if entity.get(k):
                    names = []
                    for name in entity[k]:
                        if name.get(k2):
                            names.append(name[k2])
                    name = '|'.join(names)
                else:
                    name = None
                multi_names[writeas] = name
            
            if entity.get('entity_poly'):
                seq = entity['entity_poly'].get('pdbx_seq_one_letter_code')
                seq_can = entity['entity_poly'].get('pdbx_seq_one_letter_code_can')
            else:
                seq = None
                seq_can = None
                
            result.append((pdb_id, 
                           descriptor, 
                           multi_names.get('name'),
                           multi_names.get('sys_name'),
                           multi_names.get('organism'),
                           info, 
                           seq,
                           seq_can,
                            '|'.join([str(x) for x in pmids]),
                            '|'.join(dois),
                           ))
    return pd.DataFrame(result, columns=['pdb', 'descriptor', 'name', 'sys_name', 'organism', 'info', 'seq', 'seq_can', 'pmids', 'dois'])


def process_data(grouped_data):
    results = {"pdb": {}, "uniprot": {}, "refseq": {}, "genbank": {}}
    
    for id_type, ids in grouped_data.items():
        if id_type == 'pdb':
            continue
        elif id_type == 'uniprot':
            uniprot_info = _fetch_uniprot_info(ids.keys())
            for entry in uniprot_info['results']:
                uniprot_id = entry['primaryAccession']
                enzyme = entry['proteinDescription']['recommendedName']['fullName']['value']
                organism = entry['organism']['scientificName']
                results[uniprot_id] = {'enzyme': enzyme, 'organism': organism, 'pmids': ids[uniprot_id]}
        
        elif id_type in ['refseq', 'genbank']:
            continue
            ncbi_info = fetch_ncbi_info(id_type, ids.keys())
            for id, summary in ncbi_info['result'].items():
                if id != 'uids':
                    enzyme = summary['title']
                    organism = summary['organism']
                    results[id] = {'enzyme': enzyme, 'organism': organism, 'pmids': ids[id]}
    
    return results

# Main execution
# if __name__ == "__main__":
#     file_path = 'analysis/dbs/brenda_wiley_pdbs.txt'
#     write_path = 'analysis/dbs/brenda_wiley_uniprot_names.json'
#     data = parse_jsonl(file_path)
#     grouped_data = group_identifiers(data)
#     results = process_data(grouped_data)

#     # Print or further process the results
#     # for id, info in results["pdb"].items():
#         # turn sets into lists
#         # info['pmids'] = list(info['pmids'])
#         # print(f"ID: {id}")
#         # print(f"Enzyme: {info['enzyme']}")
#         # print(f"Organism: {info['organism']}")
#         # print(f"PMIDs: {', '.join(info['pmids'])}")
#         # print()

#     # write to file
#     with open(write_path, 'w') as f:
#         json.dump(results, f, indent=2)