import pandas as pd
import numpy as np
from pyg import *
from pyg import eq, add_, sub_, drange
import pytest

s1 = pd.Series([1., 2., 3. , np.nan], drange(-3))
a1 = np.array([1., 2., 3. , np.nan])
s2 = pd.Series([1., 2., 3. , np.nan], drange(-4, -1))
a2 = np.array([1., 2., 3. , 4., np.nan])

a3 = np.array([1., 2.])
a2d = np.array([[1., 2., 3. , np.nan],[0., 2., 4. , np.nan]]).T
df = pd.DataFrame(dict(a = [1,2,3,4.], b = [0,1,np.nan,3]), index = drange(-3))

ij = add_.ij
oj = add_.oj
lj = add_.lj
rj = add_.rj


def test_bi_s_v_s():
    # args = (s1,s2); kwargs = {}
    # self = add_
    assert eq(add_(s1,s2), pd.Series([3.,5.,np.nan], drange(-3,-1)))
    assert eq(add_.oj(s1,s2), pd.Series([np.nan, 3.,5.,np.nan, np.nan], drange(-4)))
    assert eq(add_.lj(s1,s2), pd.Series([3.,5.,np.nan, np.nan], drange(-3)))
    assert eq(add_.rj(s1,s2), pd.Series([np.nan, 3.,5.,np.nan], drange(-4,-1)))
    
    assert eq(add_.ij.ffill(s1,s2), pd.Series([3.,5.,6], drange(-3,-1)))
    assert eq(add_.oj.ffill(s1,s2), pd.Series([np.nan, 3.,5.,6,6 ], drange(-4)))
    assert eq(add_.lj.ffill(s1,s2), pd.Series([3.,5.,6,6], drange(-3)))
    assert eq(add_.rj.ffill(s1,s2), pd.Series([np.nan, 3.,5.,6], drange(-4,-1)))


def test_bi_s_v_a():
    for f in [add_.ij, add_.oj, add_.lj, add_.rj]:
        assert eq(f(s1,a1), pd.Series([2,4,6,np.nan], drange(-3)))
        assert eq(f.ffill(s1, a1), pd.Series([2.,4.,6, np.nan], drange(-3)))
        assert eq(f(s1, a2d), pd.DataFrame({0 : [2,4,6,np.nan], 1 : [1,4,7,np.nan]}, index = s1.index))
    
        with pytest.raises(ValueError):
            f(s1,a2)
    
    
def test_bi_s_v_n():
    for f in [add_.ij, add_.oj, add_.lj, add_.rj]:
        assert eq(f(s1,1), pd.Series([2,3,4,np.nan], drange(-3)))
        assert eq(f.ffill(s1,1), pd.Series([2,3,4,4], drange(-3)))
    
    
def test_bi_a_v_pd():
    for f in [add_.ij, add_.oj, add_.lj, add_.rj]:
        assert eq(f(a1,s2), pd.Series([2.,4.,6.,np.nan], s2.index))
        assert eq(f(s2,a1), pd.Series([2.,4.,6.,np.nan], s2.index))

    for f in [add_.ij, add_.oj, add_.lj, add_.rj]:
        assert eq(f(a1, df), pd.DataFrame(dict(a = [2.,4.,6.,np.nan], b = [1,3., np.nan, np.nan]), df.index))
        assert eq(f(df, a1), pd.DataFrame(dict(a = [2.,4.,6.,np.nan], b = [1,3., np.nan, np.nan]), df.index))

    for f in [add_.ij, add_.oj, add_.lj, add_.rj]:
        assert eq(f(a2d, df), pd.DataFrame(dict(a = [2.,4,6,np.nan], b = [0, 3., np.nan, np.nan]), df.index))
        assert eq(f(df, a2d), pd.DataFrame(dict(a = [2.,4,6,np.nan], b = [0, 3., np.nan, np.nan]), df.index))


def test_bi_a_v_a():
    assert eq(ij(a1,a2), a1[-4:] + a2[-4:])
    assert eq(oj(a1,a2), np.array([np.nan, 3., 5., 7., np.nan]))
    assert eq(oj.ffill(a1,a2), np.array([np.nan, 3., 5., 7., 7.]))
    assert eq(lj(a1,a2), np.array([3., 5., 7., np.nan]))
    assert eq(rj(a1,a2), np.array([np.nan, 3., 5., 7., np.nan]))
    assert eq(rj(a1,a3), np.array([4., np.nan]))


def test_bi_a_v_a2d():
    # self = ij; args = (a1, a2d); kwargs = {}
    assert eq(ij(a1, a2d), np.array([[2,1],[4,4],[6,7],[np.nan, np.nan]]))
    assert eq(ij(a2d, a1), np.array([[2,1],[4,4],[6,7],[np.nan, np.nan]]))

