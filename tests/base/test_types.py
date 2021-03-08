from pyg import nan2none, is_ts, drange, is_lists, is_strs, is_zero_len
import numpy as np
import pandas as pd

def test_is_zero_len():
    assert is_zero_len(5)
    assert not is_zero_len([1,])


def test_nan2none():
    assert nan2none(np.nan) is None
    assert nan2none(np.inf) is None
    assert nan2none(5) == 5


def test_is_ts():
    assert is_ts(pd.Series(range(3), drange(2)))
    assert is_ts(pd.Series(range(3), drange(2)[::-1]))
    assert is_ts(pd.Series([],[]))
    assert not is_ts(pd.Series([1,2,3]))


def test_is_lists():
    assert is_lists([[1,2],[3,4],[5,6]])
    assert not is_lists([[1,2],[3,4],None])
    assert not is_lists([[1,2],[3,4],5])
    assert not is_lists([1,2,3])

def test_is_strs():
    assert not is_strs(dict(a = 1, b = 2))
    assert is_strs(dict(a = 1, b = 2).keys())
    assert is_strs(dict(a = 'a', b = 'b').values())