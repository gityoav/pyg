from pyg import eq, ewma, ewmstd, ewmrms, ewmskew, dt, calendar, drange
import pandas as pd; import numpy as np
t = dt(2021,3,1)
cal = calendar('US')

def test_ewm_monthly():    
    months = drange('-500m', t, '1m')
    a = pd.Series(np.random.normal(0,1,501), months)
    days = cal.drange('-500m', t, 1)
    for f in ewma, ewmstd, ewmskew, ewmrms:
        assert eq(f(a, 3).reindex(days), f(a.reindex(days), 3))
        assert eq(f(a.reindex(days).ffill(), 3, time = 'm'), f(a.reindex(days),3).ffill())


def test_ewm_weekly():    
    months = drange('-500w', t, '1w')
    a = pd.Series(np.random.normal(0,1,501), months)
    days = cal.drange('-500w', t, '1b')
    for f in ewma, ewmstd, ewmskew, ewmrms:
        assert eq(f(a, 3).reindex(days), f(a.reindex(days), 3))
        assert eq(f(a.reindex(days).ffill(), 3, time = 'w'), f(a.reindex(days),3).ffill())

def test_ewm_yearly():    
    t = dt(2021,1,1)
    yearly = drange('-100y', t, '1y')
    a = pd.Series(np.random.normal(0,1,101), yearly)
    days = cal.drange('-100y', t, 1)
    for f in ewma, ewmstd, ewmskew, ewmrms:
        assert eq(f(a, 3).reindex(days), f(a.reindex(days), 3))
        assert eq(f(a.reindex(days).ffill(), 3, time = 'y'), f(a.reindex(days),3).ffill())

