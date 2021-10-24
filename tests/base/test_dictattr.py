from pyg import dictattr
import pytest

def test_dictattr_access():
    d = dictattr(a = 1, b = 2, c = 3)
    assert d.a == 1
    assert d['a', 'b'] == [1,2]
    
    
def test_dictattr__sub__():
    d = dictattr(a = 1, b = 2, c = 3)
    assert d - ['b','c'] == dictattr(a = 1)
    assert d - 'c' == dictattr(a = 1, b = 2)
    assert d - 'key not there' == d
    #commutative
    assert (d - 'c').keys() == d.keys() - 'c'
    d = dictattr(a = 1, b = dictattr(c = 3, d = 4), e = 5)
    assert d - ('b', 'd') == dictattr(a = 1, b = dictattr(c = 3), e = 5)

def test_dictattr__and__():
    d = dictattr(a = 1, b = 2, c = 3)
    assert d & ('a', 'b') == dictattr(a = 1, b = 2)
    assert d & 'a' == dictattr(a = 1)
    assert d & ('a', 'b','other') == dictattr(a = 1, b = 2)
    

def test_dictattr_delete_fail():
    d = dictattr(a = 1, b = 2, c = 3)
    with pytest.raises(KeyError):
        del d['d']    
    with pytest.raises(AttributeError):    
        del d.d


def test_dictattr__dict__():
    d = dictattr(a = 1, b = 2, c = 3)
    assert dict(d) == dict(a = 1, b = 2, c = 3)
    assert d.__dict__() == dict(a = 1, b = 2, c = 3)
