from pyg import fnna, ffill, na2v, v2na, nona, eq
from pyg.timeseries._math import stdev_calculation_ewm, variance_calculation_ewm, cor_calculation_ewm, skew_calculation
import numpy as np; import pandas as pd
from numpy import nan, array

def test_fnna():
    a = np.array([np.nan, np.nan, 1, np.nan, np.nan, 2, np.nan, np.nan])    
    assert fnna(a) == 2
    assert fnna(a,2) == 5
    assert fnna(a,-1) == 5
    assert fnna(a,-2) == 2
    assert fnna(a,3) is None

def test_na2v():
    a = np.array([np.nan, np.nan, 1, np.nan, np.nan, 2, np.nan, np.nan])    
    assert eq(na2v(a), np.array([0., 0., 1., 0., 0., 2., 0., 0.]))
    assert eq(na2v(a, 10), np.array([10., 10., 1., 10., 10., 2., 10., 10.]))

def test_v2na():
    a = np.array([np.nan, np.nan, 1, np.nan, np.nan, 2, np.nan, np.nan])    
    assert eq(v2na(a, 1), np.array([nan, nan, nan, nan, nan,  2., nan, nan]))
    assert eq(v2na(na2v(a)), a)

def test_nona():
    a = np.array([np.nan, np.nan, 1, np.nan, np.nan, 2, np.nan, np.nan])    
    assert eq(nona(a), array([1., 2.]))

    a2 = np.array([np.nan, 1 , 1, np.nan, 1, np.nan, np.nan, np.nan])    
    b = np.array([a,a2]).T
    
    assert len(nona(b)) == 4
    assert eq(nona(b), array([[nan,1,nan,2], [1,1,1,nan]]).T)


def test_negative_variance():
    assert np.isnan(stdev_calculation_ewm(10, 100000, 20, 0, 0))
    assert np.isnan(variance_calculation_ewm(10, 100000, 20, 0, 0))
    assert np.isnan(skew_calculation(10, 100000, 20, 20, True, 0))

    assert variance_calculation_ewm(10, 10, 100, 0, 1, bias = True) == 9
    assert variance_calculation_ewm(10, 10, 100, 0, 1, bias = False) == 9
