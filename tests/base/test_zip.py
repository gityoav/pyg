from pyg import zipper, lens
import numpy as np
import pytest

def test_zipper():
    assert list(zipper([1,2,3], 4)) == [(1, 4), (2, 4), (3, 4)] 
    assert list(zipper([1,2,3], [4,5,6])) == [(1, 4), (2, 5), (3, 6)] 
    assert list(zipper([1,2,3], [4,5,6], [7])) ==  [(1, 4, 7), (2, 5, 7), (3, 6, 7)] 
    assert list(zipper([1,2,3], [4,5,6], None)) ==  [(1, 4, None), (2, 5, None), (3, 6, None)] 
    assert list(zipper((1,2,3), np.array([4,5,6]), None)) ==  [(1, 4, None), (2, 5, None), (3, 6, None)] 
    with pytest.raises(ValueError):
        zipper([1,2,3,4], [1,2,3])
    

def test_lens():
    assert lens([1,2,3,4], [1,2,3,4], [1,2,3,4]) == 4
    with pytest.raises(ValueError):
        lens([1,2,3,4], [1,2,3])
