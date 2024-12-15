# polluted_validate = f"""1. P. falciparum HGXPRT, Allopurinol\nA. hypoxanthine phosphoribosyltransferase | guanine | #6# recombinant enzyme, with 5-phosphoribosyl 1-diphosphate, pH 8.5 <33>\n\n2. Human HGPRT, Guanine\nB. hypoxanthine phosphoribosyltransferase | allopurinol | #19# recombinant enzyme, with 5-phosphoribosyl 1-diphosphate, pH 8.5 <33>\n\n3. P. falciparum HGXPRT, Xanthine\nC. hypoxanthine phosphoribosyltransferase | hypoxanthine | #6# recombinant enzyme, with 5-phosphoribosyl 1-diphosphate, pH 8.5 <33>\n\n4. Human HGPRT, Hypoxanthine\nD. hypoxanthine phosphoribosyltransferase | xanthine | #19# recombinant enzyme, with 5-phosphoribosyl 1-diphosphate, pH 8.5 <33>\n\n5. P. falciparum HGXPRT, Guanine\nE. hypoxanthine phosphoribosyltransferase | guanine | #19# recombinant enzyme, with 5-phosphoribosyl 1-diphosphate, pH 8.5 <33>\n\n6. P. falciparum HGXPRT, Hypoxanthine\nF. hypoxanthine phosphoribosyltransferase | hypoxanthine | #19# recombinant enzyme, with 5-phosphoribosyl 1-diphosphate, pH 8.5 <33>\n\n7. Human HGPRT, Allopurinol\nG. hypoxanthine phosphoribosyltransferase | allopurinol | #6# recombinant enzyme, with 5-phosphoribosyl 1-diphosphate, pH 8.5 <33>\n\n"""

pmpt = f"""You are a diligent assistant specialized in enzyme catalysis. 
Given a list of descriptors, you must identify equivalent descriptors and synonyms. For each numbered item, you must find the matching descriptor in the lettered list.

Before your final answer, you may write thoughts and comments.
Here is an example:

### Input
1. In situ glycerol dehydratase, glycerol
A. glycerol dehydratase | glycerol | #2# enzyme in situ, toluene-treated cells <15>

2. In situ glycerol dehydratase, 1,2-ethanediol
B. glycerol dehydratase | 1,2-propanediol | #2# enzyme in situ, toluene-treated cells <15>

3. In situ glycerol dehydratase, Propane-1,2-diol
C. glycerol dehydratase | ethanediol | #2# enzyme in situ, toluene-treated cells <15>

### Output

For #2, 1,2-ethanediol better matches ethanediol from C.
For #3, Propane-1,2-diol better matches 1,2-propanediol from B.

```answer
1. A
2. C
3. B
```"""

# validate = f"""1. TMP kinase G146A, TMP, 30\u00b0C, pH 7.4\nA. dTMP kinase | dTMP | #4# pH 7.4, 30\u00b0C, mutant enzyme G146A <15>\n\n2. TMP kinase wild-type, 5-Br-dUMP, 30\u00b0C, pH 7.4\nB. dTMP kinase | dUMP | #4# pH 7.4, 30\u00b0C, mutant enzyme G146A <15>\n\n3. TMP kinase G146A, dUMP, 30\u00b0C, pH 7.4\nC. dTMP kinase | 5-bromo-2'-deoxyuridine 5'-monophosphate | #4# pH 7.4, 30\u00b0C, wild-type enzyme <15>\n\n4. TMP kinase wild-type, TMP, 30\u00b0C, pH 7.4\nD. dTMP kinase | TMP | #4# pH 7.4, 30\u00b0C, wild-type enzyme <15>\n\n5. TMP kinase wild-type, dUMP, 30\u00b0C, pH 7.4\nE. dTMP kinase | dUMP | #4# pH 7.4, 30\u00b0C, wild-type enzyme <15>\n\n6. TMP kinase G146A, 5-Br-dUMP, 30\u00b0C, pH 7.4\nF. dTMP kinase | 5-bromo-2'-deoxyuridine 5'-monophosphate | #4# pH 7.4, 30\u00b0C, mutant enzyme G146A <15>\n\n"""
# print(validate)

median_validate = f"""1. TMP kinase G146A, TMP, 30°C, pH 7.4
A. dTMP kinase | TMP | #4# pH 7.4, 30°C, mutant enzyme G146A <15>

2. TMP kinase wild-type, 5-Br-dUMP, 30°C, pH 7.4
B. dTMP kinase | dUMP | #4# pH 7.4, 30°C, mutant enzyme G146A <15>

3. TMP kinase G146A, dUMP, 30°C, pH 7.4
C. dTMP kinase | 5-bromo-2'-deoxyuridine 5'-monophosphate | #4# pH 7.4, 30°C, wild-type enzyme <15>

4. TMP kinase wild-type, TMP, 30°C, pH 7.4
D. dTMP kinase | TMP | #4# pH 7.4, 30°C, wild-type enzyme <15>

5. TMP kinase wild-type, dUMP, 30°C, pH 7.4
E. dTMP kinase | dUMP | #4# pH 7.4, 30°C, wild-type enzyme <15>

6. TMP kinase G146A, 5-Br-dUMP, 30°C, pH 7.4
F. dTMP kinase | 5-bromo-2'-deoxyuridine 5'-monophosphate | #4# pH 7.4, 30°C, mutant enzyme G146A <15>
"""



solution = f"""```answer\n1. A\n2. C\n3. B\n4. D\n5. E\n6. F\n\n```"""
print(solution)

# non-polluted (correct):
# gpt 4o fine-tuned: correct
# gpt 4o: correct

hard_validate = f"""1. TMP kinase wild-type, TMP, 30°C, pH 7.4
A. dTMP kinase | dTMP | #4# pH 7.4, 30°C, mutant enzyme G146A <15>

2. TMP kinase wild-type, dUMP, 30°C, pH 7.4
B. dTMP kinase | 5-bromo-2'-deoxyuridine 5'-monophosphate | #4# pH 7.4, 30°C, wild-type enzyme <15>

3. TMP kinase G146A, dUMP, 30°C, pH 7.4
C. dTMP kinase | dUMP | #4# pH 7.4, 30°C, mutant enzyme G146A <15>

4. TMP kinase wild-type, 5-Br-dUMP, 30°C, pH 7.4
D. dTMP kinase | 5-bromo-2'-deoxyuridine 5'-monophosphate | #4# pH 7.4, 30°C, mutant enzyme G146A <15>

5. TMP kinase G146A, TMP, 30°C, pH 7.4
E. dTMP kinase | dUMP | #4# pH 7.4, 30°C, wild-type enzyme <15>

6. TMP kinase G146A, 5-Br-dUMP, 30°C, pH 7.4
F. dTMP kinase | TMP | #4# pH 7.4, 30°C, wild-type enzyme <15>"""

hard_solution = f"""```answer\n1. F\n2. E\n3. C\n4. B\n5. A\n6. D\n\n```"""

# hard nonpolluted:
# gpt 4o fine-tuned: correct
# gpt 4o: correct (with many words)

# polluted:
# gpt 4o mini: incorrect
# gpt 4o fine-tuned: correct (with 770 tokens)
# gpt 4o: correct (but with 1430 tokens - much much more generated)

# polluted_solution = f"""```answer\n1. B\n2. A\n3. D\n4. C\n5. E\n6. F\n7. G\n\n```"""

