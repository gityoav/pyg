import pandas as pd; import numpy as np
from pyg import drange, cumprod, cumprod_, cumsum, cumsum_, diff, diff_, ewma, ewma_, ewmcor, ewmcor_,\
    ewmvar, ewmvar_, ewmrms, ewmrms_, ewmskew, ewmskew_, ewmstd, ewmstd_, \
    expanding_max, expanding_max_, expanding_mean, expanding_mean_, expanding_min, expanding_min_, expanding_rms, expanding_rms_, expanding_skew, expanding_skew_, expanding_std, expanding_std_, expanding_sum, expanding_sum_, \
    ratio, ratio_, rolling_max, rolling_max_, rolling_mean, rolling_mean_, rolling_median, rolling_median_, rolling_min, rolling_min_, \
    rolling_rank, rolling_rank_, rolling_rms, rolling_rms_, rolling_skew, rolling_skew_, rolling_std, rolling_std_, rolling_sum, rolling_sum_, \
    shift, shift_, rolling_quantile, rolling_quantile_, \
    ts_count, ts_count_, ts_max, ts_max_, ts_mean, ts_mean_, ts_min, ts_min_, ts_rms, ts_rms_, ts_skew, ts_skew_, ts_std, ts_std_, ts_sum, ts_sum_, eq

_data = 'data'

def _nona_data():
    s = pd.Series(np.random.normal(0,1,1000), drange(-999))
    df = pd.DataFrame(np.random.normal(0,1,(1000, 20)), drange(-999))
    return [s,df]


s, df = _nona_data()
s1 = s.iloc[:500]; s2 = s.iloc[500:]
df1 = df.iloc[:500]; df2 = df.iloc[500:]
t, ef = _nona_data()
t1 = t.iloc[:500]; t2 = t.iloc[500:]
ef1 = ef.iloc[:500]; ef2 = ef.iloc[500:]

#### here we test two things (1) that f_().data == f() and that (2) We can run additional data using the state we got from f_ run over history.

n2f = dict(cumprod = cumprod, cumsum = cumsum, diff = diff, ewma = ewma, ewmrms = ewmrms, ewmskew = ewmskew, ewmstd = ewmstd, 
           expanding_max = expanding_max, expanding_mean = expanding_mean, expanding_min = expanding_min, expanding_rms = expanding_rms, expanding_skew = expanding_skew, expanding_std = expanding_std, expanding_sum = expanding_sum, ratio = ratio, 
           rolling_max = rolling_max, rolling_mean = rolling_mean, rolling_median = rolling_median, rolling_min = rolling_min, rolling_rank = rolling_rank, rolling_rms = rolling_rms, rolling_skew = rolling_skew, rolling_std = rolling_std, rolling_sum = rolling_sum, 
           shift = shift, ts_count = ts_count, ts_max = ts_max, ts_mean = ts_mean, ts_min = ts_min, ts_rms = ts_rms, ts_skew = ts_skew, ts_std = ts_std, ts_sum = ts_sum)

n2f_ = dict(cumprod = cumprod_, cumsum = cumsum_, diff = diff_, ewma = ewma_, ewmrms = ewmrms_, ewmskew = ewmskew_, ewmstd = ewmstd_, 
            expanding_max = expanding_max_, expanding_mean = expanding_mean_, expanding_min = expanding_min_, expanding_rms = expanding_rms_, expanding_skew = expanding_skew_, expanding_std = expanding_std_, expanding_sum = expanding_sum_, ratio = ratio_, 
            rolling_max = rolling_max_, rolling_mean = rolling_mean_, rolling_median = rolling_median_, rolling_min = rolling_min_, rolling_rank = rolling_rank_, rolling_rms = rolling_rms_, rolling_skew = rolling_skew_, rolling_std = rolling_std_, rolling_sum = rolling_sum_, 
            shift = shift_, ts_count = ts_count_, ts_max = ts_max_, ts_mean = ts_mean_, ts_min = ts_min_, ts_rms = ts_rms_, ts_skew = ts_skew_, ts_std = ts_std_, ts_sum = ts_sum_)


def test_ts_states():
    n2f = dict(ts_count = ts_count, ts_max = ts_max, ts_mean = ts_mean, ts_min = ts_min, ts_rms = ts_rms, ts_skew = ts_skew, ts_std = ts_std, ts_sum = ts_sum)
    n2f_ = dict(ts_count = ts_count_, ts_max = ts_max_, ts_mean = ts_mean_, ts_min = ts_min_, ts_rms = ts_rms_, ts_skew = ts_skew_, ts_std = ts_std_, ts_sum = ts_sum_)

    for n in n2f:
        f = n2f[n]
        f_ = n2f_[n]
        res = f(s)
        res_ = f_(s)
        assert eq(res, res_.data)
        res1 = f_(s1)
        res2 = f_(s2, instate = res1.state)                    
        assert eq(res2 - _data, res_ - _data)
        assert eq(res2.data, res)

    for n in n2f:
        f = n2f[n]
        f_ = n2f_[n]
        res = f(df)
        res_ = f_(df)
        assert eq(res, res_.data)
        res1 = f_(df1)
        res2 = f_(df2, instate = res1.state)                    
        assert eq(res2 - _data, res_ - _data)
        assert eq(res2.data, res)


def test_window_double_states():
    n2f = dict(ewmcor = ewmcor) 
    n2f_ = dict(ewmcor = ewmcor_)
    for i in (5,10):
        for n in n2f:
            f = n2f[n]
            f_ = n2f_[n]
            res = f(s, t, i)
            res_ = f_(s , t, i)
            assert eq(res, res_.data)
            res1 = f_(s1, t1, i)
            res2 = f_(s2, t2, i, instate = res1.state)                    
            assert eq(res2 - _data, res_ - _data)
            assert eq(res2.data, res.iloc[500:])

    for i in (5, 10):
        for n in n2f:
            f = n2f[n]
            f_ = n2f_[n]
            res = f(df, ef, i)
            res_ = f_(a = df , b = ef, n = i)
            assert eq(res, res_.data)
            res1 = f_(df1, ef1, i)
            res2 = f_(df2, ef2, i, instate = res1.state)                    
            assert eq(res2 - _data, res_ - _data)
            assert eq(res2.data, res.iloc[500:])


def test_window_states():
    n2f = dict(ewma = ewma, ewmrms = ewmrms, ewmskew = ewmskew, ewmvar = ewmvar, ewmstd = ewmstd, diff = diff, shift = shift, ratio = ratio,
               rolling_max = rolling_max, rolling_mean = rolling_mean, rolling_median = rolling_median, rolling_min = rolling_min, 
               rolling_quantile = rolling_quantile, rolling_rank = rolling_rank, rolling_rms = rolling_rms, rolling_skew = rolling_skew, rolling_std = rolling_std, rolling_sum = rolling_sum)
    
    n2f_ = dict(ewma = ewma_, ewmrms = ewmrms_, ewmvar = ewmvar_, ewmskew = ewmskew_, ewmstd = ewmstd_, diff = diff_, shift = shift_, ratio = ratio_,
                rolling_max = rolling_max_, rolling_mean = rolling_mean_, rolling_median = rolling_median_, rolling_min = rolling_min_, 
                rolling_quantile = rolling_quantile_, rolling_rank = rolling_rank_, rolling_rms = rolling_rms_, rolling_skew = rolling_skew_, rolling_std = rolling_std_, rolling_sum = rolling_sum_)

    for i in (5, 10):
        for n in n2f:
            f = n2f[n]
            f_ = n2f_[n]
            res = f(s, i)
            res_ = f_(s , i)
            assert eq(res, res_.data)
            res1 = f_(s1, i)
            res2 = f_(s2, i, instate = res1.state)                    
            assert eq(res2 - _data, res_ - _data)
            assert eq(res2.data, res.iloc[500:])

    for i in (5, 10):
        for n in n2f:
            f = n2f[n]
            f_ = n2f_[n]
            res = f(df, i)
            res_ = f_(df , i)
            assert eq(res, res_.data)
            res1 = f_(df1, i)
            res2 = f_(df2, i, instate = res1.state)                    
            assert eq(res2 - _data, res_ - _data)
            assert eq(res2.data, res.iloc[500:])



def test_expanding_states():
    n2f = dict(expanding_max = expanding_max, expanding_mean = expanding_mean, expanding_min = expanding_min, expanding_rms = expanding_rms, expanding_skew = expanding_skew, expanding_std = expanding_std, expanding_sum = expanding_sum, cumprod = cumprod, cumsum = cumsum) 
    n2f_ = dict(expanding_max = expanding_max_, expanding_mean = expanding_mean_, expanding_min = expanding_min_, expanding_rms = expanding_rms_, expanding_skew = expanding_skew_, expanding_std = expanding_std_, expanding_sum = expanding_sum_, cumprod = cumprod_, cumsum = cumsum_)

    for n in n2f:
        f = n2f[n]
        f_ = n2f_[n]
        res = f(s)
        res_ = f_(s)
        assert eq(res, res_.data)
        res1 = f_(s1)
        res2 = f_(s2, instate = res1.state)                    
        assert eq(res2 - _data, res_ - _data)
        assert eq(res2.data, res.iloc[500:])

    for n in n2f:
        f = n2f[n]
        f_ = n2f_[n]
        res = f(df)
        res_ = f_(df)
        assert eq(res, res_.data)
        res1 = f_(df1)
        res2 = f_(df2, instate = res1.state)                    
        assert eq(res2 - _data, res_ - _data)
        assert eq(res2.data, res.iloc[500:])


def test_min_max_no_data():
    a1 = np.array([np.nan, np.nan])
    a2 = np.array([np.nan, 1., 2.])
    m1 = ts_max_(a1)
    assert np.isnan(m1.data)
    m2 = ts_max(a2, **m1)    
    assert m2 == 2
    m1 = ts_min_(a1)        
    assert np.isnan(m1.data)
    m2 = ts_min(a2, **m1)    
    assert m2 == 1
