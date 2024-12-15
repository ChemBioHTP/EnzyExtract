import pytest
from enzyextract.hungarian.hungarian_matching import parse_value_and_unit


def test_scinot():
    # test a variety
    value, unit, _ = parse_value_and_unit("3.5e-05 mM")
    assert value == pytest.approx(3.5e-5)
    assert unit == "mM"

def test_scinot2():
    value, unit, _ = parse_value_and_unit("4.2 * 10^-5 M")
    assert value == pytest.approx(4.2e-5)
    assert unit == "M"

    value, unit, _ = parse_value_and_unit("4.2 x 10^-5 M")
    assert value == pytest.approx(4.2e-5)
    assert unit == "M"

    value, unit, _ = parse_value_and_unit("4.2 × 10^-5 M")
    assert value == pytest.approx(4.2e-5)
    assert unit == "M"

    value, unit, _ = parse_value_and_unit("4.2 x 10^5 M")
    assert value == pytest.approx(4.2e5)
    assert unit == "M"

def test_scinot3():
    value, unit, _ = parse_value_and_unit("10^-4 M")
    assert value == pytest.approx(1e-4)
    assert unit == "M"

    
    value, unit, _ = parse_value_and_unit("e-4 mM")
    assert value is None


def test_simple():
    value, unit, _ = parse_value_and_unit("10 mM")
    assert value == 10
    assert unit == "mM"

    value, unit, _ = parse_value_and_unit("10 M")
    assert value == 10
    assert unit == "M"

    value, unit, _ = parse_value_and_unit("10 µM")
    assert value == 10
    assert unit == "µM"

    value, unit, _ = parse_value_and_unit("42.5 mM")
    assert value == 42.5
    assert unit == "mM"

def test_SE():
    value, unit, _ = parse_value_and_unit("10 ± 1 mM")
    assert value == 10
    assert unit == "mM"

    # value, unit, _ = parse_value_and_unit("10 ± 1