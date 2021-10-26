from pyg import cache, dt
import time
import pandas as pd

def test_cache():
    @cache
    def f(a = None):
        return dt(a)    

    t0 = f()
    time.sleep(0.01)
    t1 = f()
    assert t0 == t1    
    f.clear_cache()
    assert f() > t0


def test_cache_revert_to_no_cache():
    @cache
    def f(a):
        return dt()

    ta0 = f(a = pd.Series([1,2,3]))
    tb0 = f(a = 5)
    time.sleep(0.01)
    ta1 = f(a = pd.Series([1,2,3]))
    tb1 = f(a = 5)
    
    assert ta1 > ta0
    assert tb0 == tb1
    
