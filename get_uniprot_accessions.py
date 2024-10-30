import pandas as pd
import requests
import time

from tqdm import tqdm

def get_uniprot_accession(query_organism, query_enzyme) -> list:
    # https://rest.uniprot.org/uniprotkb/search?query=organism_name:Escherichia+coli+AND+superoxide+dismutase&fields=accession,protein_name,organism_name,sequence
    url = f"https://rest.uniprot.org/uniprotkb/search?query=organism_name:{query_organism}+AND+{query_enzyme}&fields=accession,protein_name,organism_name,sequence"
    response = requests.get(url)
    
    returning = []
    if response.status_code == 200:
        # Parse the response JSON to extract the first accession number
        data = response.json()
        if data.get('results'):
            # get all accessions
            # return ', '.join([result['primaryAccession'] for result in data['results']])
            # return data['results'][0]['primaryAccession']  # Get the first accession
            for result in data['results']:
                acc = result.get('primaryAccession', None)
                org = None
                if 'organism' in result:
                    org = result['organism'].get('scientificName')
                    if not org:
                        org = result['organism'].get('commonName')
                    if not org:
                        org = (result['organism'].get('lineage') or [None])[-1]
                    if not org:
                        org = (result['organism'].get('synonyms') or [None])[0]
                name = None
                if 'proteinDescription' in result:
                    if 'recommendedName' in result['proteinDescription']:
                        # if 'fullName' in result['proteinDescription']['recommendedName']:
                        name = result['proteinDescription']['recommendedName'].get('fullName', {}).get('value')
                    if not name:
                        if 'alternativeName' in result['proteinDescription']:
                            name = result['proteinDescription']['alternativeName'][0].get('fullName', {}).get('value')
                # sequence too
                # if 'sequence' in result:
                seq = result.get('sequence', {}).get('value', None)
                returning.append((query_organism, query_enzyme, acc, org, name, seq))
            return returning        
        else:
            return []
    else:
        print(f'Error: {response.status_code}')
        return []

def process_dataframe(df):
    results = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        organism_name = row['organism_name']
        enzyme_name = row['enzyme_name']
        
        data = get_uniprot_accession(organism_name, enzyme_name)
        
        results.extend(data)
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(1)
    
    return pd.DataFrame(results, columns=['query_organism', 'query_enzyme', 'accession', 'organism', 'protein_name', 'sequence'])

if __name__ == '__main__':
    
    
    df = pd.read_csv('data/_compiled/nonbrenda.tsv', sep='\t')
    # perform the rename enzyme_full --> enzyme_name, and organism --> organism_name
    df = df[['enzyme_full', 'organism']]
    df.columns = ['enzyme_name', 'organism_name']
    
    # get unique rows
    df = df.drop_duplicates()
    
    # require both organism and enzyme name
    df = df.dropna()
    
    # do in batches of 100, because I'm scared
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    write_dest = 'fetch_sequences/enzymatch'
    for i in range(0, len(df), 100):
        if i <= 17500:
            continue # keep processing from where we left off
        result_df = process_dataframe(df[i:i+100])
        result_df.to_csv(f'{write_dest}/uniprot_{timestamp}_{i}.tsv', sep='\t', index=False)
        print(f'Written to {write_dest}/uniprot_{timestamp}_{i}.tsv')
    