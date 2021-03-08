from pyg import rolling_quantile, rolling_quantile_, eq, nona, fnna, shape, dictable, expanding_rank, drange
import pandas as pd; import numpy as np
import pytest

def test_rolling_quantile_short_seq():
    a = np.random.normal(0, 1, 50)
    assert eq(rolling_quantile(a, 100), a + np.nan)


def test_rolling_quantile_reverse():
    a = np.random.normal(0, 1, 20)
    fwd = rolling_quantile(a, 10).T[0]
    bck = rolling_quantile(a, -10).T[0]
    assert eq(nona(fwd), nona(bck))
    assert fnna(fwd) == 9 and fnna(fwd, -1) == 19
    assert fnna(bck) == 0 and fnna(bck, -1) == 10


def test_rolling_quantile_fail_multicol():
    a = np.random.normal(0, 1, (20,3))
    with pytest.raises(ValueError):
        rolling_quantile(a, 10, [0.1,0.2,0.3])
        
    rolling_quantile(a, 10, [0.1])

def test_shape():
    assert shape(5) == ()
    assert shape([5]) == ()
    assert shape(dictable(a = [1,2,3])) == (3,1)
    assert shape(pd.DataFrame(dictable(a = [1,2,3]))) == (3,1)
    assert shape(pd.Series(dictable(a = [1,2,3]))) == (1,)


def test_expandning_rank():
    a = pd.Series([1.,2., np.nan, 0.,4.,2.], drange(-5)) 
    rank = expanding_rank(a) 
    assert eq(rank, pd.Series([0, 1, np.nan, -1, 1, 0.25], drange(-5))) 
    