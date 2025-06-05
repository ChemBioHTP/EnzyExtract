import polars as pl


_prompt_fragment = """
    - descriptor: R190Q cat-1; 25°C
      substrate: H2O2
      kcat: 33 ± 0.3 s^-1
      Km: 2.3 mM
      kcat/Km: null"""
    
_prompt_fragment2 = """    - descriptor: R203Q cat-1; with NADPH; 25°C
      substrate: H2O2
      kcat: null
      Km: 9.9 ± 0.1 µM
      kcat/Km: 4.4 s^-1 mM^-1"""


def is_content_regurgitated(content: str) -> bool:
    return (
        _prompt_fragment in content or
        _prompt_fragment2 in content
    )

