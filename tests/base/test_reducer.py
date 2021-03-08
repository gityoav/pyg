from pyg import reducer, reducing, dictable
import pytest
from operator import add, mul
from functools import reduce
    
def test_reducer():
    assert reducer(add, [1,2,3,4]) == 10
    assert reducer(mul, [1,2,3,4]) == 24
    assert reducer(add, [1]) == 1
    
    assert reducer(add, []) is None
    with pytest.raises(TypeError):
        reduce(add, [])



def test_reducing():
    from operator import mul
    assert reducing(mul)([1,2,3,4]) == 24    
    assert reducing(mul)(6,4) == 24    

    assert reducing('__add__')([1,2,3,4]) == 10
    assert reducing('__add__')(6,4) == 10
    
    d = dictable(a = [1,2,3,5,4])
    assert reducing('inc')(d, dict(a=1))
    
    f = lambda a, b, c: a+b+c
    assert reducing(f)([1,2,3,4,5], c = 0) == 15
    assert reducing(f)([1,2,3,4,5], c = 1) == 19
