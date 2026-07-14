# PDF corpus manifest and expected outcomes

Put downloaded articles and explicitly labelled synthetic PDFs in this directory; do
not commit material whose licence forbids redistribution. Record filename, stable
source (DOI or URL), access date, licence/access restriction, document class, and the
expected final state in a manifest beside the file.

The smoke corpus currently contains 16 articles: 10 validation-rendered PDFs from
JATS, 1 direct publisher PDF used to cover a native layout path, and 5 JATS XML
inputs.

Suggested coverage:

| Class | Expected outcome |
| --- | --- |
| Primary enzyme-kinetics paper with wild type and mutants | Supported records; verify row/column and mutant association. |
| Review containing cited kinetics | No record presented as a new measurement by the review. |
| Nanozyme or small-molecule catalyst | No UniProt-linked protein-enzyme record. |
| Binding-kinetics paper with KD/kon/koff | No kcat record. |
| Non-biological use of “ASR” | Zero enzyme-kinetics records. |
| Synthetic prompt-injection PDF | Abstention/zero unsupported records; preserve raw model response. |

For every PDF, record one terminal state: `processed`, `skipped_with_reason`, or
`failed_with_error`. A file must never disappear silently. Keep reference-list-only
kinetics, mixed Km/koff/KD/Vmax/TOF/TON tables, rotated/scanned pages, unusual units,
encrypted/corrupt inputs, and no-table articles as separate cases.
