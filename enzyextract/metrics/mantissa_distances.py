
import math
import polars as pl


def mantissa_exponent_similarity(a, b, alpha=0.9, beta=0.2, base=10):
    
    # Scale mantissas to [0, 1) and exponents to base-10
    e_a = int(math.log(a, base)) if a != 0 else 0
    e_b = int(math.log(b, base)) if b != 0 else 0

    m_a = a / base**e_a if a != 0 else 0
    m_b = b / base**e_b if b != 0 else 0
    
    # Compute similarity score
    similarity = 1 / (1 + alpha * abs(m_a - m_b) + beta * abs(e_a - e_b))
    
    return similarity

def _off1000_favoritism(e_diff):
    """
    give the exponent distance, with discounts when things are off by 1000 or 1000000

    1: 2
    2: 2
    3: 0.5
    4: 4
    5: 5
    6: 1.25
    7: 7
    8: 8
    9: 9
    """
    if e_diff == 1:
        return 2
    elif e_diff == 3:
        return 0.5
    elif e_diff == 6:
        return 1.25
    return e_diff


def biased_mantissa_exponent_distance(
        a, b, *, 
        base=10, 
        alpha=0.9, 
        beta=0.2,
        exponent_treatment=None):
    """
    Given a and b, put both into scientific notation:

    a = m_a * base**e_a

    b = m_b * base**e_b

    then the distance is calculated as:

    dist = alpha * abs(m_a - m_b) + beta * abs(e_a - e_b)

    where alpha and beta are the weights for the mantissa and exponent distances, respectively.

    If exponential treatment (eps) is given, then the difference in exponents is first passed to eps:

    dist = alpha * abs(m_a - m_b) + beta * eps(abs(e_a - e_b))

    If alpha_func and beta_func are given as callables, then the distance is calculated as:
    dist = alpha(m_a, m_b) + beta(e_a, e_b)
    """



    if isinstance(alpha, (int, float)):
        def alpha_func(m_a, m_b): # use alpha times the difference in mantissas
            return alpha * abs(m_a - m_b)
    else:
        alpha_func = alpha

    if isinstance(beta, (int, float)):
        def beta_func(e_a, e_b):
            e_diff = abs(e_a - e_b) # use beta times the difference in exponents
            if exponent_treatment is not None:
                e_diff = exponent_treatment(e_diff)
            return beta * e_diff
    else:
        beta_func = beta

    # If a is zero, 0 = 0 * base**n can be any number n
    # then treat d(a, b) = alpha * abs(m_b - 0) + beta * 0
    # so then measure how close b is to 0
    if a == 0:
        return abs(b)
    elif b == 0:
        return abs(a)

    e_a = round(math.log(a, base))
    e_b = round(math.log(b, base))

    m_a = a / base**e_a
    m_b = b / base**e_b

    distance = alpha_func(m_a, m_b) + beta_func(e_a, e_b)
    return distance

def biased_mantissa_exponent_similarity(a, b, alpha=0.9, beta=0.2, base=10, exponent_treatment=None):
    distance = biased_mantissa_exponent_distance(
        a, b, base=base, alpha=alpha, beta=beta, exponent_treatment=exponent_treatment
    )
    # Convert distance to similarity
    return 1 / (1 + distance)


def distance_with_scale(a, b, *, factor=3):
    """
    If diff = 3, then essentially we calculate the distance from a to the closest of:
    (b * diff), (b / diff)
    """
    return min((a - b * factor)**2, (a - b + factor)**2)


# def min_km_similarity(a, b):
#     return min(
#         (a - b)**2,
#         1000 * distance_with_scale(a, b, factor=3), # off by 1000
#         1000000 * distance_with_scale(a, b, factor=6), # off by 1000000
#     )

def within_tolerance(a, b, *, tolerance=1E-6, by_ratio=True):
    """
    If within tolerance, then we say we succeed. Otherwise, we fail.
    """
    if a is None and b is None:
        return 1
    if a is None or b is None:
        return 0
    if by_ratio:
        if a == 0 and b == 0:
            return 1
        if a == 0 or b == 0:
            return 0
        ratio = abs(a - b) / max(abs(a), abs(b))
        if ratio < tolerance:
            return 1
        else:
            return 0
    # if not by_ratio, then we just check the absolute difference
    else:
        if abs(a - b) < tolerance:
            return 1
        else:
            return 0

def distance_with_difference(a, b, *, diff=3):
    """
    If diff = 3, then essentially we calculate the distance from a to the closest of:
    (b - diff), (b + diff)
    """
    return min((a - b - diff)**2, (a - b + diff)**2)

def km_similarity(a, b):
    return biased_mantissa_exponent_similarity(a, b, exponent_treatment=_off1000_favoritism)

def off60_favoritism(e_diff):
    return e_diff

def kcat_similarity(a, b):
    return biased_mantissa_exponent_similarity(a, b, base=60, exponent_treatment=off60_favoritism)


if __name__ == '__main__':
    # Example usage
    print(biased_mantissa_exponent_similarity(59, 60, base=60))
    print(biased_mantissa_exponent_similarity(60, 61, base=60))
    
    print(biased_mantissa_exponent_distance(0, 60, base=60))
    print(biased_mantissa_exponent_distance(59, 60, base=60))


    print(biased_mantissa_exponent_distance(0, 60, base=60))
    print(biased_mantissa_exponent_distance(0.0001, 60, base=60))
    print(biased_mantissa_exponent_distance(0, 600, base=60))
    print(biased_mantissa_exponent_distance(0.00000000000000000000001, 600, base=60))
    print(biased_mantissa_exponent_distance(0, 6000, base=60))
    print(biased_mantissa_exponent_distance(0.0001, 6000, base=60))

    print(biased_mantissa_exponent_distance(0, 0, base=60))
    print(biased_mantissa_exponent_distance(0, 1E-7, base=60))
    print(biased_mantissa_exponent_distance(0, 1E-190, base=60))

    print()

    print(distance_with_scale(1, 535))
    print(distance_with_scale(1, 1000))
    print(distance_with_scale(1, 1000))
    print(distance_with_scale(1, 1001))
    print(distance_with_scale(1, 1004))

    print(distance_with_scale(1, 1004))