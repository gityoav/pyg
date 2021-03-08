from pyg import as_float

def test_as_float():
    assert as_float('10,304') == 10304
    assert as_float('10, 304, 201 ') == 10304201
    assert as_float('10k') == 10000
    assert as_float('1m') == 1e6
    assert as_float('-1b') == -1e9
    assert as_float('-1.1t') == -1.1e12
    assert as_float(4.5) == 4.5
    assert as_float('I am not a float') is None
    assert as_float('') is None
    assert as_float('12e3k') == 12e6
