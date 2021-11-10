import pandas as pd
import numpy as np
import datetime
from pyg import df_slice, drange, dt, dt_bump, dt2str, eq, dictable, df_unslice, nona, df_sync, Dict, add_, div_, mul_, sub_, pow_
from operator import add, itruediv, sub, mul, pow

def test_df_slice():
    df = pd.Series(np.random.normal(0,1,1000), drange(-999, 2000))
    assert len(df_slice(df, None, dt_bump(2000,'-1m'))) == 969
    assert len(df_slice(df, dt_bump(2000,'-1m'), None)) == 31


    df = pd.Series(np.random.normal(0,1,1000), drange(-999, 2020))
    jan1 = drange(2018, None, '1y')
    feb1 = drange(dt(2018,2,1), None, '1y')
    res = df_slice(df, jan1, feb1, openclose = '[)')
    assert set(res.index.month) == {1}


def test_df_slice_time():
    dates = drange(-5, 2020, '5n')
    df = pd.Series(np.random.normal(0,1,12*24*5+1), dates)
    assert len(df_slice(df, None, datetime.time(hour = 10))) == 606
    assert len(df_slice(df, datetime.time(hour = 5), datetime.time(hour = 10))) == 300
    assert len(df_slice(df, lb = datetime.time(hour = 10), ub = datetime.time(hour = 5))) == len(dates) - 300


def test_df_slice_roll():
    ub = drange(1980, 2000, '3m')
    df = [pd.Series(np.random.normal(0,1,1000), drange(-999, date)) for date in ub]
    res = df_slice(df, ub = ub)
    assert len(res) == 8305
    ub = drange(1980, 2000, '3m')
    df = [pd.Series(np.random.normal(0,1,1000), drange(-999, date)) for date in ub]
    res = df_slice(df, ub = ub, n = 5).iloc[500:]
    res.shape == (7805,5)
# -*- coding: utf-8 -*-


def test_df_slice_roll_symbol():
    ub = drange(1980, 2000, '3m')
    df = [dt2str(date) for date in ub]    
    res = df_slice(df, ub = ub, n = 3)
    assert list(res.iloc[-3].values) == ['19990701', '19991001', '20000101']
    assert res.index[-3] == dt('19990701')
    res = df_slice(df, lb = ub, n = 3, openclose = '[)')
    assert list(res.iloc[-3].values) == ['19990701', '19991001', '20000101']
    assert res.index[-3] == dt('19990701')

def test_df_unslice():
    ub = drange(1980, 2000, '3m')
    dfs = [pd.Series(date.year * 100 + date.month, drange(-999, date)) for date in ub]
    df = df_slice(dfs, ub = ub, n = 10)
    res = df_unslice(df, ub)
    rs = dictable(res.items(), ['ub', 'df'])
    assert eq(df_slice(df = rs.df, ub = rs.ub, n = 10), df)
    assert len(rs.inc(lambda df: len(set(nona(df)))>1)) == 0

def test_df_sync():
    a = pd.DataFrame(np.random.normal(0,1,(100,5)), drange(-100,-1), list('abcde'))
    b = pd.DataFrame(np.random.normal(0,1,(100,5)), drange(-99), list('bcdef'))
    c = 'not a timeseries'
    d = pd.DataFrame(np.random.normal(0,1,(100,1)), drange(-98,1), ['single_column_df'])
    s = pd.Series(np.random.normal(0,1,105), drange(-104))
    
    dfs = [a,b,c,d,s]
    res = df_sync(dfs, 'ij')
    assert len(res[0]) == len(res[1]) == len(res[-1]) == 98
    assert res[2] == 'not a timeseries'
    assert list(res[0].columns) == list('bcde')

    res = df_sync(dfs, 'oj')
    assert len(res[0]) == len(res[1]) == len(res[-1]) == 106; 
    assert res[2] == 'not a timeseries'
    assert list(res[0].columns) == list('bcde')

    res = df_sync(dfs, join = 'oj', method = 1)
    assert res[0].iloc[0].sum() == 4

    res = df_sync(dfs, join = 'oj', method = 1, columns = 'oj')
    assert res[0].iloc[0].sum() == 5
    assert list(res[0].columns) == list('abcdef')
    assert list(res[-2].columns) == ['single_column_df'] # single column unaffected

    dfs = Dict(a = a, b = b, c = c, d = d, s = s)
    res = df_sync(dfs, join = 'oj', method = 1, columns = 'oj')
    assert res.c == 'not a timeseries'
    assert res.a.shape == (106,6)

def test_bi():
    s = pd.Series([1,2,3.], drange(-2,2000))
    a = pd.DataFrame(dict(a = [1,2,3.], b = [4,5,6.]), drange(-2,2000))
    b = pd.DataFrame(dict(c = [1,2,3.], b = [4,5,6.]), drange(-3,2000)[:-1])
    c = 5
    
    assert eq(add_(s,a), pd.DataFrame(dict(a = [2,4,6.], b = [5,7,9.]), drange(-2,2000)))
 
    for f in [add_,sub_,div_,mul_,pow_]:
        assert f(s,b).shape == (2,2)
        assert f(s,b, 'oj').shape == (4,2)

        assert f(a,b).shape == (2,1)
        assert f(a,b, 'oj').shape == (4,1)
        assert f(a,b, 'oj', 0, 'oj').shape == (4,3)

    # operations with a constant
    for f,o in zip([add_,sub_,div_,mul_,pow_], [add, sub, itruediv,mul, pow]):
        for v in [a,b,s,c]:
            assert eq(f(v,c), o(v,c))

    
    
