replace_greek = {'α': 'alpha', 'β': 'beta', 'ß': 'beta',
                 'γ': 'gamma', 'δ': 'delta', 'ε': 'epsilon', 'ζ': 'zeta', 'η': 'eta', 'θ': 'theta', 'ι': 'iota', 'κ': 'kappa', 'λ': 'lambda', 'μ': 'mu', 'ν': 'nu', 'ξ': 'xi', 'ο': 'omicron', 'π': 'pi', 'ρ': 'rho', 'σ': 'sigma', 'τ': 'tau', 'υ': 'upsilon', 'φ': 'phi', 'χ': 'chi', 'ψ': 'psi', 'ω': 'omega'}
replace_nonascii = {'\u2018': "'", '\u2019': "'", '\u2032': "'", '\u201c': '"', '\u201d': '"', '\u2033': '"',
                    '\u00b2': '2', '\u00b3': '3',
                    '\u207a': '+', # '\u207b': '-', # '\u207c': '=', '\u207d': '(', '\u207e': ')',
                    '(±)-': '', # we cannot handle the ± character anyways
                    '®': '',
                    }
import polars as pl
def pl_to_ascii(col: pl.Expr) -> pl.Expr:
    """
    Cleans a string column to make it closer to ascii.
    """
    return (
        # pl.col(col)
        col
        .str.to_lowercase()
        .str.replace_all('[-᠆‑‒–—―﹘﹣－˗−‐⁻]', '-')
        .str.replace_many(replace_greek)
        .str.replace_many(replace_nonascii)
        # .alias(col_dest)
    )