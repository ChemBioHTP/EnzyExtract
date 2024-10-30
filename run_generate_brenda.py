import json
import pandas as pd
import re

from kcatextract.utils.pmid_management import cache_pmids_to_disk

brenda = None

def load_brenda():
    global brenda

    with open("C:/conjunct/vandy/yang/corpora/brenda/brenda_2023_1.json") as f:
        brenda = json.load(f)


def get_needles(needles):
    if needles == None:
        needles = ['turnover_number', 'km_value']
    good_refs = set()
    for ec in brenda['data']:
        for key in needles: # 'kcat_km',  'km_value', 
            for item in brenda['data'][ec].get(key, []):
                for ref in item['references']:
                    good_refs.add((ec, ref))
    good_references = []
    for ec, ref in good_refs:
        good_references.append((ec, ref, brenda['data'][ec]['references'][ref]))
    return good_references




def require_ref(row):
    # if the row has a comment, make sure that the comment cites the ref
    required = row['ref']
    # read citations in <1,2,3> format
    comments = row['comments']
    if comments == '':
        return True
    # look for the required reference
    myre = r"<[\d,]*>"
    found = re.findall(myre, comments)
    for f in found:
        inside_part = f[1:-1]
        splits = inside_part.split(',')
        if required in splits:
            return True
    if len(found) == 0:
        return True
    return False

def get_comment_citations(row):
    # because some comments do not cite self, we can't quite explode the comments until
    # we know that at least one comment cites the reference. 
    # thus, push all cited references into a list
    # if the row has a comment, make sure that the comment cites the ref
    # required = row['ref']
    # read citations in <1,2,3> format
    comments = row['comments']
    if comments == '':
        return []
    # look for the required reference
    myre = r"<[\d,]*>"
    found = re.findall(myre, comments)
    
    citations = []
    for f in found:
        inside_part = f[1:-1]
        citations.extend(inside_part.split(','))
    citations = set(citations)
    citations = [x.strip() for x in citations]
    return citations

def collect_from_brenda(good_pmids):
    """
    Reads from the brenda dict, and collects the data directly into a DataFrame.
    """
    brenda_refs = []

    for ec in brenda['data']: 
        for ref in brenda['data'][ec].get('references', {}):
            if 'pmid' in brenda['data'][ec]['references'][ref]:
                if brenda['data'][ec]['references'][ref]['pmid'] in good_pmids:
                    brenda_refs.append((ec, ref, brenda['data'][ec]['references'][ref]))
    found_stuff = []
    for ec, ref, obj in brenda_refs:
        for key in ['km_value', 'turnover_number', 'kcat_km']:
            for item in brenda['data'][ec].get(key, []):
                # citations = get_comment_citations({'comments': item.get('comment', '')})
                # this makes no difference
                if ref in item['references']: #  or ref in citations:
                    found_stuff.append((ec, ref, obj, key, item))
    # turn it into df
    prep_arr = []
    for found in found_stuff:
        ec, ref, obj, key, item = found
        num_range = str(item.get('min_value', '')) + ' -- ' + str(item.get('max_value', ''))
        if num_range == ' -- ':
            num_range = None
        prep_arr.append({'pmid': obj['pmid'],
                        'ec': ec, 
                        'ref': ref,
                        'key': key,  # "turnover_number", "kcat_km", "km_value"
                        'enzyme': brenda['data'][ec]['name'],
                        #  'aliases': brenda['data'][ec].get('synonyms', []),
                        'comments': item.get('comment', ""),
                        'substrate': item['value'], 
                        'num': item.get('num_value', None),
                        'num_range': num_range,
                        })
    df_all = pd.DataFrame(prep_arr)
    
    
    return df_all


    


def condense_rows(df_all):
    """
    Logic to separate the comments and then condense the rows. 
    (go from having kcat and km in separate rows to matching them and having them in the same row)
    """
    # separate the comments
    
    # get citations per comments
    df_all['citations'] = df_all.apply(get_comment_citations, axis=1)
    
    # outlier: when ref is not in citations, then that pmid does not reference itself
    df_no_comment = df_all[df_all['comments'] == '']
    
    df_self_strangers = df_all[df_all.apply(lambda x: x['comments'] != '' and x['ref'] not in x['citations'], axis=1)]
    print("These pmids do not reference themselves: ", str(set(df_self_strangers['pmid'])))
    
    # only explode comments that do cite itself, otherwise we'll lose data
    # df_all = df_all.assign(comments=df_all['comments'].str.split('; ')).explode('comments')
    df_reliable = df_all[df_all.apply(lambda x: x['ref'] in x['citations'], axis=1)] # if comments == '', it's impossible for it to have citations, so we're safe
    df_reliable = df_reliable.assign(comments=df_reliable['comments'].str.split('; ')).explode('comments')

    # make sure that the datapoint actually cites the reference which we're taking
    df_rematch = df_reliable[df_reliable.apply(require_ref, axis=1)]
    
    # then merge in the self strangers
    df_rematch = pd.concat([df_no_comment, df_rematch, df_self_strangers])
    
    # get excluded pmids; add them back to be safe
    excluded_pmids = set(df_all['pmid']) - set(df_rematch['pmid'])
    print("Furthermore, we have lost these PMIDs: ", str(excluded_pmids))
    df_excluded = df_all[df_all['pmid'].isin(excluded_pmids)]
    df_rematch = pd.concat([df_rematch, df_excluded])
    
    # condense num_range into num
    df_rematch['num'].fillna(df_rematch['num_range'], inplace=True)
    
    # Pivot the DataFrame to condense entries
    
    df_pivot = df_rematch.pivot_table(index=['pmid', 'ec', 'ref', 'enzyme', 'substrate', 'comments'],
                            columns='key',
                            values='num',
                            aggfunc='first').reset_index()
    # these pmids usually correspond to null values
    excluded_pmids = set(df_rematch['pmid']) - set(df_pivot['pmid'])

    # these pmids correspond to null values
    print("These pmids probably have only null values: ", str(excluded_pmids))
    print(str(set(df_rematch[df_rematch['pmid'].isin(excluded_pmids)]['num'])))
    return df_pivot, df_self_strangers

def split_mutants_are_combined():
    """
    example of issue: 
    #31# pH 7, 25ºC, wild type and D280 mutant, phosphoenolpyruvate-producing reaction <46>
    wild type and D280 mutant are combined in the same row
    """
    # this is a hard problem, 
    pass

def script_detect_combined_mutants():
    df = pd.read_csv('C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv')

    # get those with comments which matches this regex:
    sus_mutant_re = re.compile(r"(wild[ -]?type|WT|[A-Z]\d{2,3}[A-Z]|mutant),? and (wild[ -]?type|WT|[A-Z]\d{2,3}[A-Z]|mutant)")
    # sus_df = df[df['comments'].str.contains(sus_mutant_re)] # ValueError: Cannot mask with non-boolean array containing NA / NaN values
    sus_df = df[df['comments'].apply(lambda x: isinstance(x, str) and sus_mutant_re.search(x) != None)]
    print(sus_df['comments']) # only 356 rows

    # maybe exclude these pmids from evaluation?
    # because if the value for wild-type and mutant being is described in 1 line,
    # the cardinalities cannot be the same as ours, and it messes up hungarian
    # cache_pmids_to_disk(sus_df['pmid'], 'brenda_sus_combined_mutants')

def script_construct_brenda_df():
    load_brenda()

    good_km_kcat_references = get_needles(['turnover_number', 'km_value'])
    km_kcat_pmids = set([x['pmid'] for ec, refno, x in good_km_kcat_references if 'pmid' in x])
    df_km_kcat = collect_from_brenda(km_kcat_pmids)

    df_km_kcat_pivot, df_self_strangers = condense_rows(df_km_kcat)

    # write
    # df_km_kcat_pivot.to_csv('brenda_km_kcat_key_v3.csv', index=False)
    # df_self_strangers.to_csv('brenda_km_kcat_self_strangers.csv', index=False)

if __name__ == '__main__':
    script_detect_combined_mutants()