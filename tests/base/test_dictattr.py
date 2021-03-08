from pyg import dictattr

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

