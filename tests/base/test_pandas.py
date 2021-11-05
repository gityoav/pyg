import pandas as pd
import numpy as np
import datetime
from pyg import df_slice, drange, dt, dt_bump, dt2str

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

