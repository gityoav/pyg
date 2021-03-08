from pyg import ulist, rng

def test_rng():
    assert rng(3) == list(range(3))

def test_ulist():
    assert ulist([1,3,2,1]) == list([1,3,2])
    assert ulist([1,3,2,1]) + 4  == list([1,3,2,4])
    assert ulist([1,3,2,1]) + [4,1] == list([1,3,2,4])
    assert ulist([1,3,2,1]) + [4,1,5] == list([1,3,2,4,5])
    assert ulist([1,3,2,1]) & [1,3,4] == [1,3]
    assert ulist([1,3,2,1]) & 1 == [1]
    assert ulist([1,3,2,1]) & 4 == []
    assert ulist([1,3,2,1]) - [1,3,4] == [2]
    assert ulist([1,3,2,1]) - 4 == ulist([1,3,2])
    assert ulist([1,3,2,1]) - [4,5] == ulist([1,3,2])

def test_ulist_add():
    assert ulist([1,2,3]) + 1 == ulist([1,2,3])
    assert ulist([1,2,3]) + [1,2] == ulist([1,2,3])
    assert ulist([1,2,3]).copy() == ulist([1,2,3])    

