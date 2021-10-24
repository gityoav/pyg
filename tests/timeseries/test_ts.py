import numpy as np
import pandas as pd
from pyg import eq, nona, ffill, bfill
from pyg import Dict, drange, alphabet
from pyg import ewma, ewmskew, ewmrms, ewmstd, ewmvar, rolling_sum, rolling_mean, rolling_rms, rolling_std, rolling_skew, rolling_max, rolling_min, rolling_median
from pyg import expanding_mean, expanding_sum, expanding_std, expanding_rms, cumsum, cumprod
from pyg import ts_mean, ts_count, ts_max, ts_std, ts_rms, ts_skew, ts_median


np.random.seed(seed=0)

def _data():
    index = drange(-99)
    a2 = np.random.normal(0,1, (100,26))
    a1 = np.random.normal(0,1,100)
    na2 = np.random.normal(0,1, (100,26))
    na2[na2<0.1] = np.nan
    na1 = np.random.normal(0,1,100)
    na1[na1<0.1] = np.nan
    df = pd.DataFrame(a2, index, list(alphabet))
    ndf = pd.DataFrame(na2, drange(-99), list(alphabet))
    s = pd.Series(a1, index)
    ns = pd.Series(na1, index)
    return Dict(a1 = a1, a2 = a2, na1 = na1, na2 = na2, df = df, ndf = ndf, s = s, ns = ns, index = index)

d = _data()

def test_ts_vs_numpy():
    for f in ewma, ewmskew, ewmrms, ewmstd, ewmvar, rolling_sum, rolling_mean, rolling_rms, rolling_std, rolling_skew, rolling_max, rolling_min, rolling_median:
        assert eq(f(d.ndf, 10).values, f(d.ndf.values, 10))
        assert eq(f(d.df, 10).values, f(d.df.values, 10))
        assert eq(f(d.s, 10).values, f(d.s.values, 10))
        assert eq(f(d.ns, 10).values, f(d.ns.values, 10))
        assert eq(f(d.ndf, 5, axis=1).values, f(d.ndf.values, 5, axis=1))
        assert eq(f(d.df, 5, axis=1).values, f(d.df.values, 5, axis=1))

    for f in expanding_mean, expanding_sum, expanding_std, expanding_rms, cumsum, cumprod, ffill, bfill:
        assert eq(f(d.ndf).values, f(d.ndf.values))
        assert eq(f(d.df).values, f(d.df.values))
        assert eq(f(d.s).values, f(d.s.values))
        assert eq(f(d.ns).values, f(d.ns.values))
        assert eq(f(d.ndf, axis=1).values, f(d.ndf.values, axis=1))
        assert eq(f(d.df, axis=1).values, f(d.df.values, axis=1))

    for f in ts_mean, ts_count, ts_max, ts_std, ts_rms, ts_skew, ts_median:
        assert eq(f(d.ndf).values, f(d.ndf.values))
        assert eq(f(d.df).values, f(d.df.values))
        assert eq(f(d.ndf, axis=1).values, f(d.ndf.values, axis=1))
        assert eq(f(d.df, axis=1).values, f(d.df.values, axis=1))
        assert eq(f(d.s), f(d.s.values))
        assert eq(f(d.ns), f(d.ns.values))

    
def test_ts_and_nans():
    """
    We show that we can take an array, remove nans, run it without the nans and then resample back, and we get the same as running it outright
    """
    for f in ewma, ewmskew, ewmrms, ewmstd, ewmvar, rolling_sum, rolling_mean, rolling_rms, rolling_std, rolling_skew, rolling_max, rolling_min, rolling_median:
        assert eq(f(d.ns, 10), f(nona(d.ns), 10).reindex(d.ns.index))
        assert eq(f(d.ndf, 10)['c'], f(nona(d.ndf['c']), 10).reindex(d.ndf.index))

    for f in expanding_mean, expanding_sum, expanding_std, expanding_rms, cumsum, cumprod:
        assert eq(f(d.ns), f(nona(d.ns)).reindex(d.ns.index))
        assert eq(f(d.ndf)['c'], f(nona(d.ndf['c'])).reindex(d.ndf.index))
    
    for f in ts_mean, ts_count, ts_max, ts_std, ts_rms, ts_skew, ts_median:
        assert eq(f(d.ns), f(nona(d.ns)))
        assert eq(f(d.ndf)['c'], f(nona(d.ndf['c'])))
    

def test_ts_rolling_mean_with_time():
    a = np.arange(10) * 1.
    n = 2
    time = np.array([1,1,2,2,3,3,4,4,5,5.])
    raw = 2*rolling_mean(a, n)
    timed = 2*rolling_mean(a, n, time = time)
    assert eq(raw, np.array([np.nan, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5]))
    assert eq(timed, np.array([np.nan, np.nan, 1+2, 1+3 , 3+4, 3+5. , 5+6, 5+7 , 7+8, 7+9]))
    timed = rolling_sum(a, n, time = time)
    assert eq(timed, np.array([np.nan, np.nan, 1+2, 1+3 , 3+4, 3+5. , 5+6, 5+7 , 7+8, 7+9]))
    timed = rolling_std(a, n, time = time) ** 2
    assert eq(timed, np.array([np.nan, np.nan, 0.5, 2. , 0.5, 2. , 0.5, 2. , 0.5, 2. ]))
    timed = 2 * (rolling_rms(a, n, time = time) ** 2)
    assert np.max(np.abs((timed - np.array([ np.nan,  np.nan,   1+4,  1+9.,  9+16.,  9+25.,  25+36.,  25+49., 49+64., 49+81.]))[2:])) < 1e-10
