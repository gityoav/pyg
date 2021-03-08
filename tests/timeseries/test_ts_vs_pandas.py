from pyg import *
from pyg import ts_mean, ts_sum, ts_skew, ts_rms, ts_max, ts_min, ts_count, ts_median, drange, cumsum, shift, diff, cumprod, sub_, ffill, bfill, eq
from pyg import expanding_max, expanding_mean, expanding_median, expanding_min, expanding_rank, expanding_rms, expanding_skew, expanding_std, expanding_sum
from pyg import  rolling_max, rolling_mean, rolling_median, rolling_min, rolling_quantile, rolling_rank, rolling_rms, rolling_skew, rolling_std, rolling_sum
from pyg import ewma, ewmrms, ewmstd, ewmskew, ewmvar
import numpy as np
import pandas as pd
from functools import partial

def _nona_data():
    s = pd.Series(np.random.normal(0,1,1000), drange(-999))
    df = pd.DataFrame(np.random.normal(0,1,(1000, 20)), drange(-999))
    return [s,df]

def _nan_data():
    s = pd.Series(np.random.normal(0,1,1000), drange(-999))
    s[s<0.1] = np.nan
    df = pd.DataFrame(np.random.normal(0,1,(1000, 20)), drange(-999))
    df[df<0.1] = np.nan
    return [s,df]


def test_ts_pandas_match():
    n2p = dict(ts_mean = lambda a, **kwargs: a.mean(**kwargs),
            ts_sum = lambda a, **kwargs: a.sum(**kwargs),
            ts_skew = lambda a, **kwargs: a.skew(**kwargs),
            ts_rms = lambda a, **kwargs: (a**2).mean(**kwargs)**0.5,
            ts_max = lambda a, **kwargs: a.max(**kwargs),
            ts_min = lambda a, **kwargs: a.min(**kwargs),
            ts_count = lambda a, **kwargs: a.count(**kwargs), 
            ts_median = lambda a, **kwargs: a.median(**kwargs))

    n2f = dict(ts_mean = ts_mean ,
            ts_sum = ts_sum,
            ts_skew = ts_skew,
            ts_rms = ts_rms,
            ts_max = ts_max,
            ts_min = ts_min,
            ts_count = ts_count, 
            ts_median = ts_median)
               
    s, df = _nona_data()
    for n, p in n2p.items():
        f = n2f[n]
        assert abs(f(s) - p(s)) < 1e-10
        assert abs(f(df) - p(df)).max() < 1e-10

    
    s, df = _nan_data()
    for n, p in n2p.items():
        f = n2f[n]
        assert abs(f(s) - p(s)) < 1e-10
        assert abs(f(df) - p(df)).max() < 1e-10
        assert abs(f(df, axis = 1) - p(df, axis = 1)).max() < 1e-10
        

def test_expanding_pandas_match():
    n2p = dict(expanding_max = lambda a, axis = 0, **kwargs: a.expanding(axis = axis).max(**kwargs),
            expanding_mean = lambda a, axis = 0, **kwargs: a.expanding(axis = axis).mean(**kwargs),
            expanding_median = lambda a, axis = 0, **kwargs: a.expanding(axis = axis).median(**kwargs),
            expanding_min = lambda a, axis = 0, **kwargs: a.expanding(axis = axis).min(**kwargs),
            expanding_rms = lambda a, axis = 0, **kwargs: (a**2).expanding(axis = axis).mean(**kwargs) **0.5,
            expanding_std = lambda a, axis = 0, **kwargs: a.expanding(axis = axis).std(**kwargs),
            expanding_sum = lambda a, axis = 0, **kwargs: a.expanding(axis = axis).sum(**kwargs))



    n2f = dict(expanding_max = expanding_max, 
               expanding_mean = expanding_mean, 
               expanding_median = expanding_median,
               expanding_min = expanding_min,
               expanding_rank = expanding_rank, 
               expanding_rms = expanding_rms,
               expanding_skew = expanding_skew, 
               expanding_std = expanding_std,
               expanding_sum= expanding_sum)
               
    s, df = _nona_data()
    for n, p in n2p.items():
        f = n2f[n]
        assert abs(f(s) - p(s)).max() < 1e-12
        assert abs(f(df) - p(df)).max().max() < 1e-10

    s, df = _nan_data()
    for n, p in n2p.items():
        f = n2f[n]
        assert abs(f(s) - p(s)).max() < 1e-12
        assert abs(f(df) - p(df)).max().max() < 1e-10
        assert abs(f(df, axis = 1) - p(df, axis = 1)).max().max() < 1e-10
        
def test_rolling_pandas_match():
    n2f = dict(
        rolling_max = rolling_max, 
        rolling_mean = rolling_mean, 
        rolling_median = rolling_median, 
        rolling_min = rolling_min, 
        rolling_quantile = partial(rolling_quantile, quantile = 0.3), 
        rolling_rank = rolling_rank, 
        rolling_rms = rolling_rms, 
        rolling_skew = rolling_skew, 
        rolling_std = rolling_std, 
        rolling_sum = rolling_sum)
    
    n2p = dict(
        rolling_max = lambda a, n, axis = 0, **kwargs: a.rolling(n, axis = axis).max(**kwargs), 
        rolling_mean = lambda a, n, axis = 0, **kwargs: a.rolling(n, axis = axis).mean(**kwargs), 
        rolling_median = lambda a, n, axis = 0, **kwargs: a.rolling(n, axis = axis).median(**kwargs), 
        rolling_min = lambda a, n, axis = 0, **kwargs: a.rolling(n, axis = axis).min(**kwargs), 
        rolling_quantile = lambda a, n, q = 0.3, axis = 0, **kwargs: a.rolling(n, axis = axis).quantile(q, **kwargs), 
        rolling_rms = lambda a, n, axis = 0, **kwargs: (a**2).rolling(n, axis = axis).mean(**kwargs)**0.5, 
        rolling_std = lambda a, n, axis = 0, **kwargs: a.rolling(n, axis = axis).std(**kwargs), 
        rolling_sum = lambda a, n, axis = 0, **kwargs: a.rolling(n, axis = axis).sum(**kwargs))
    
    s, df = _nona_data()
    for n, p in n2p.items():
        f = n2f[n]
        # args = (f(s, 10),p(s, 10)); kwargs = {}; self = sub_
        assert abs(sub_(f(s, 10),p(s, 10))).max() < 1e-12
        assert abs(f(df,10) - p(df,10)).max().max() < 1e-10
        assert abs(f(df,10,axis=1) - p(df,10, axis = 1)).max().max() < 1e-10
        
def test_ewma_pandas_match():
    n2f = dict(ewma = ewma, ewmstd = ewmstd, ewmrms = ewmrms, ewmskew = ewmskew, ewmvar = ewmvar)
    n2p = dict(ewma = lambda a, n, axis = 0: a.ewm(n, axis = axis).mean(),
               ewmstd = lambda a, n, axis = 0: a.ewm(n, axis = axis).std(),
               ewmvar = lambda a, n, axis = 0: a.ewm(n, axis = axis).var(),
               ewmrms = lambda a, n, axis = 0: (a**2).ewm(n, axis = axis).mean()**0.5)
    s, df = _nona_data()

    for n, p in n2p.items():
        f = n2f[n]
        assert abs(sub_(f(s, 10), p(s, 10))).max() < 1e-12
        assert abs(f(df,10) - p(df,10)).max().max() < 1e-10
        assert abs(f(df,10,axis=1) - p(df,10, axis = 1)).max().max() < 1e-10
        
    # for data with nan, pandas ewma can be calculated as clocking time whenever there is nan, hence time = 'index' match.
    # however, this is not the case for std(). I really have no idea how panda handles nans
    s, df = _nan_data()
    n = 'ewma'
    p = n2p[n]
    f = n2f[n]
    assert abs(sub_(f(s, 10, time = 'i'),p(s, 10))).max() < 1e-12
    assert abs(f(df,10, time = 'i') - p(df,10)).max().max() < 1e-10
    assert abs(f(df,10, time = 'i', axis=1) - p(df,10, axis = 1)).max().max() < 1e-10


def test_bits_vs_pandas():
    s, df = _nona_data()    
    assert abs(cumsum(s) - s.cumsum()).max() <1e-13
    assert abs(cumsum(df) - df.cumsum()).max().max() < 1e-13

    for n in [-4,-1,0,1,10]:
        assert abs(shift(s,n) - s.shift(n)).max() <1e-13
        assert abs(shift(df,n) - df.shift(n)).max().max() < 1e-13
        assert abs(diff(s,n) - s.diff(n)).max() <1e-13
        assert abs(diff(df,n) - df.diff(n)).max().max() < 1e-13

    s, df = _nan_data()    
    assert abs(np.log10(cumprod(s)) - np.log10(s).cumsum()).max()<1e-13
    assert abs(np.log10(cumprod(df)) - np.log10(df).cumsum()).max().max()<1e-10

    assert eq(ffill(s), s.fillna(method = 'ffill'))



