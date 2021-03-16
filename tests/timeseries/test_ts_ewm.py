from pyg import eq, ewma, ewmstd, ewmrms, ewmskew, dt, calendar, drange, ewmLR, Dict
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

def test_ewm_LR():
    a0 = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    a1 = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    b = (a0 - a1) + pd.Series(np.random.normal(0,1,10000), drange(-9999))
    a = pd.concat([a0,a1], axis=1)
    LR = ewmLR(a,b,50)
    assert abs(LR.m.mean()[0]-1)<0.5
    assert abs(LR.m.mean()[1]+1)<0.5
    a = Dict(a0 = a0, a1 = a1)
    LR2 = ewmLR(a,b,50)
    assert eq(LR2.a0.m, LR.m[0])
    assert eq(LR2.a0.c, LR.c[0])
    assert 'state' not in LR2.a0
    a = [a0,a1]
    LR3 = ewmLR(a,b,50)
    assert eq(LR2.a0, LR3[0])
    assert eq(LR2.a1, LR3[1])
