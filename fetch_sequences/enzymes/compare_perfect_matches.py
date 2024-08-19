

import pandas as pd


def script0():
    pmids_perf_gpt = set()
    pmids_perf_auto = set()

    with open('fetch_sequences/enzymes/perfect_matches.txt') as f:
        for i, line in enumerate(f):
            if line.startswith('['):
                pmid = line.split(']', 1)[0].strip('[]')
                if i < 60:
                    pmids_perf_gpt.add(pmid)
                else:
                    pmids_perf_auto.add(pmid)

    print(len(pmids_perf_gpt), len(pmids_perf_auto)) # 53 to 43
    print(len(pmids_perf_gpt & pmids_perf_auto)) # 26 in common
    print(len(pmids_perf_gpt - pmids_perf_auto)) # 27 special to gpt
    print(', '.join(pmids_perf_gpt - pmids_perf_auto))
    # 24220791, 12501179, 15210695, 12147691, 14512428, 19211556, 15894617, 17855366, 14690539, 30196454, 15190062, 10970743, 8999873, 17893142, 20403363, 12962489, 18325786, 15632186, 25450250, 14572311, 16332678, 22363663, 10441376, 14500895, 18407998, 11468288, 16309698
    print(len(pmids_perf_auto - pmids_perf_gpt)) # 17 special to auto
    print(', '.join(pmids_perf_auto - pmids_perf_gpt))
    # 2938733, 17873050, 18245280, 9576908, 23013430, 21602356, 18667417, 28272331, 17680775, 23100535, 20656485, 26217660, 19933367, 20715188, 26999531, 12878037, 21527346
    
def script1():
    df = pd.read_csv('fetch_sequences/enzymes/rekcat_mutant_distances.tsv', sep='\t')
    df = df[df['distance'] == 0]
    print('", "'.join([x.replace('_1', '') for x in df['ident']]))

if __name__ == '__main__':
    script1()