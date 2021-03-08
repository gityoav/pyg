from pyg.base import as_list, as_tuple, first, last, unique
import pytest

def test_as_list():
    assert as_list(None) == []
    assert as_list(1) == [1]
    assert as_list((1,)) == [1]
    assert as_list([1]) == [1]
    assert as_list(([1],)) == [1]
    assert as_list({1: 1}.keys()) == [1]
    assert as_list({1: 1}.values()) == [1]
    assert as_list(range(1,2)) == [1]
    assert as_list(zip([1,2], [3,4])) == [(1, 3), (2, 4)]

def test_as_tuple():
    assert as_tuple(None) == ()
    assert as_tuple(1) == (1,)
    assert as_tuple((1,)) == (1,)
    assert as_tuple([1]) == (1,)
    assert as_tuple(([1],)) == (1,)
    assert as_tuple({1: 1}.keys()) == (1,)
    assert as_tuple({1: 1}.values()) == (1,)
    assert as_tuple(zip([1,2], [3,4])) == ((1, 3), (2, 4))

def test_last():
    assert last('hello') == 'hello'
    assert last([]) == None
    assert last([1,2]) == 2
    assert last(range(5)) == 4
    assert last(zip([1,2], [3,4])) == (2,4)


def test_first():
    assert first('hello') == 'hello'
    assert first([]) == None
    assert first([1,2]) == 1
    assert first(range(5)) == 0
    assert first(zip([1,2], [3,4])) == (1,3)

def test_unique():
    assert unique('hello') == 'hello'
    assert unique([]) == None
    assert unique([1,1,1,1]) == 1
    assert unique([[2],[2]]) == [2]
    with pytest.raises(ValueError):
        unique([1,2])
    with pytest.raises(ValueError):
        unique([[1],[2],[2]])


