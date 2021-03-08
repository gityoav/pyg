from pyg import getitem, callitem, callattr, dictable, upper
import pytest
import pandas as pd

def test_getitem():
    a = dict(a = 1)
    assert getitem(a, 'a') == 1
    assert getitem(a, 'b', 5) == 5
    with pytest.raises(KeyError):
        getitem(a, 'b')


def test_callitem():
    a = dict(f = lambda a, b: a+b)
    assert callitem(a, 'f', (1,2)) == 3
    assert callitem(a, 'f', kwargs = dict(a=1,b=2)) == 3


def test_callattr():
    a = dictable(a = [1,2,3])
    assert callattr(a, ['rename', 'do'], args = [(upper,), (lambda v: v**2,)]) == dictable(A = [1,4,9])

