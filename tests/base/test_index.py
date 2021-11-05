from pyg import presync, dt, drange, eq, ts_sum
from pyg import df_fillna, df_reindex, nona, reducing, Dict, np_reindex
import pandas as pd; import numpy as np
import pytest

def test_df_fillna():
    s = pd.Series([0., 1., 4., np.nan, 16.], np.arange(0,5))
    assert np.isnan(df_fillna(s)[3])
    assert df_fillna(s, 'bfill')[3] == 16
    assert df_fillna(s, 'ffill')[3] == 4
    assert df_fillna(s, 'linear')[3] == 10
    assert df_fillna(s, 5)[3] == 5
    assert round(df_fillna(s, 'quadratic')[3],10) ==  9.
    
    s = s.values
    assert np.isnan(df_fillna(s)[3])
    assert df_fillna(s, 'bfill')[3] == 16
    assert df_fillna(s, 'ffill')[3] == 4
    assert df_fillna(s, 'linear')[3] == 10
    assert df_fillna(s, 5)[3] == 5
    assert round(df_fillna(s, 'quadratic')[3],10) ==  9.

    s = pd.Series([np.nan, 1., 4., np.nan, 16.], np.arange(0,5))
    assert len(df_fillna(s, 'nona')) == 3
    assert len(df_fillna(s, 'fnna')) == 4


    df = pd.DataFrame(dict(a = [0,np.nan, 1, 2, 3], b = [0,np.nan, np.nan,3,4]))
    assert len(df_fillna(df, 'nona')) == 4
    assert eq(df_fillna(df, 'linear'), pd.DataFrame(dict(a = [0,0.5, 1, 2, 3], b = [0.,1,2,3,4])))

    
    
def test_df_fillna_multiple():
    s = pd.Series([np.nan, 1., 4., np.nan, 16.], np.arange(0,5))
    x = df_fillna(s, ['ffill', 'bfill'])
    assert x[0] == 1 and x[3] == 4        
    x = df_fillna(s, ['bfill', 'bfill'])
    assert x[0] == 1 and x[3] == 16

    s = s.values
    x = df_fillna(s, ['ffill', 'bfill'])
    assert x[0] == 1 and x[3] == 4        
    x = df_fillna(s, ['bfill', 'bfill'])
    assert x[0] == 1 and x[3] == 16


def test_df_fillna_no_overlap():
    s = pd.Series([1,2,3,4,5], np.arange(0,10,2))
    index = np.arange(1,11,2)
    assert len(nona(df_reindex(s, index))) == 0
    assert eq(s.reindex(index, method = 'ffill'), pd.Series([1,2,3,4,5], index))



def test_df_fillna_limit():
    s = pd.Series([np.nan, 1., 4., np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 16.], np.arange(12))

    x = df_fillna(s, 'ffill', limit = 4)
    assert x[6] == 4.0 and np.isnan(x[7])    

def test_df_reindex():
    tss = [pd.Series(np.arange(20), drange(i, 19+i)) for i in range(5)]    
    inner = pd.concat(df_reindex(tss, 'inner'), axis = 1)
    assert len(inner) == 16

    outer = pd.concat(df_reindex(tss, 'outer'), axis = 1)
    assert len(outer) == 24
    assert np.isnan(outer.iloc[-1,0])

    outer2 = pd.concat(df_reindex(tss, 'outer', 'ffill'), axis = 1)
    assert len(outer2) == 24
    assert outer2.iloc[-1,0] == 19.

def test_df_reindex_fail():
    ts = pd.Series(range(100), drange(-99))
    with pytest.raises(ValueError):
        df_reindex(ts, 100)
        
    assert eq(df_reindex(ts, ts[10:]), ts[10:])
    
def test_df_reindex_no_overlap():
    s = pd.Series(np.arange(5), np.arange(0,10,2))
    index = np.arange(1,11,2)
    for method in ('bfill', 'ffill', 'pad'):
        assert eq(df_reindex(s, index, method), s.reindex(index, method = method))

def test_df_reindex_dict():
    tss = {i : pd.Series(np.arange(20), drange(i, 19+i)) for i in range(5)}
    inner = df_reindex(tss, 'inner')    
    assert len(inner[0]) == 16
    outer = df_reindex(tss, 'outer')
    assert len(outer[0]) == 24
    assert np.isnan(outer[0][-1])
    outer2 = df_reindex(tss, 'outer', method = 'ffill')
    assert outer2[0][-1] == 19

    inner2 = df_reindex(Dict(tss), 'inner')    
    assert isinstance(inner2, Dict)
    assert len(inner2[0]) == 16


def test_np_renindex():
    ts = np.arange(1000) * 1.
    index = pd.Series(range(500), drange(-499))
    assert eq(np_reindex(ts, index), pd.Series(np.arange(500,1000), drange(-499)))
    index = pd.Series(range(1500), drange(-1499))
    assert eq(np_reindex(ts, index), pd.Series(np.arange(1000), drange(-999)))

    ts = np.random.normal(0,1,(1000,3)) * 1.
    index = drange(-999)
    assert eq(np_reindex(ts, index, ['a', 'b', 'c']), pd.DataFrame(ts, index, ['a', 'b', 'c']))


def test_presync_simple():
    x = pd.Series([1,2,3,4], drange(-3))
    y = pd.Series([1,2,3,4], drange(-4,-1))    
    z = pd.DataFrame([[1,2],[3,4]], drange(-3,-2), ['a','b'])
    f = lambda a, b: a+b    

    assert list(presync(f)(x,z).columns) == ['a', 'b']

    res = presync(f, index='outer', method = 'ffill')(x,z)
    assert eq(res.a.values, np.array([2,5,6,7]))

    res = presync(reducing(f), index='outer', method = 'ffill')([x,y,z])
    assert eq(res, pd.DataFrame(dict(a = [np.nan, 4, 8, 10, 11], b = [np.nan, 5, 9, 11, 12]), index = drange(-4)))    
    assert eq(reducing(presync(f, index='outer', method = 'ffill'))([x,y,z]), res)
    
def test_presync_with_dicts():
    function = lambda a, b: a['x'] + a['y'] + b    
    self = presync(function, 'outer', method = 'ffill')
    x = pd.Series([1,2,3,4], drange(-3))
    y = pd.Series([1,2,3,4], drange(-4,-1))    
    z = pd.DataFrame([[1,2],[3,4]], drange(-3,-2), ['a','b'])
    
    
    args = (dict(x = x, y = y),)
    kwargs = dict(b = z)
    res = self(*args, **kwargs)
    assert eq(res, pd.DataFrame(dict(a = [np.nan, 4, 8, 10, 11], b = [np.nan, 5, 9, 11, 12]), index = drange(-4)))

def test_presync_with_Dicts():
    function = lambda a, b: a.x + a.y + b    
    self = presync(function, 'outer', method = 'ffill')
    x = pd.Series([1,2,3,4], drange(-3))
    y = pd.Series([1,2,3,4], drange(-4,-1))    
    z = pd.DataFrame([[1,2],[3,4]], drange(-3,-2), ['a','b'])

    args = (Dict(x = x, y = y),)
    kwargs = dict(b = z)
    res = self(*args, **kwargs)
    assert eq(res, pd.DataFrame(dict(a = [np.nan, 4, 8, 10, 11], b = [np.nan, 5, 9, 11, 12]), index = drange(-4)))
    

def test_presync_various():
    x = pd.Series([1,2,3,4], drange(-3))
    y = pd.Series([1,2,3,4], drange(-4,-1))    
    z = pd.DataFrame([[1,2],[3,4]], drange(-3,-2), ['a','b'])
    addition = lambda a, b: a+b    
    assert list(addition(x,z).columns) ==  list(x.index) + ['a', 'b']
    
    #But:
        
    assert list(presync(addition)(x,z).columns) == ['a', 'b']
    res = presync(addition, index='outer', method = 'ffill')(x,z)
    assert eq(res.a.values, np.array([2,5,6,7]))
    
    
    #:Example 2: alignment works for parameters 'buried' within...
    #-------------------------------------------------------
    function = lambda a, b: a['x'] + a['y'] + b    
    f = presync(function, 'outer', method = 'ffill')
    res = f(dict(x = x, y = y), b = z)
    assert eq(res, pd.DataFrame(dict(a = [np.nan, 4, 8, 10, 11], b = [np.nan, 5, 9, 11, 12]), index = drange(-4)))
    
    
    #:Example 3: alignment of numpy arrays
    #-------------------------------------
    addition = lambda a, b: a+b
    a = presync(addition)
    assert eq(a(pd.Series([1,2,3,4], drange(-3)), np.array([[1,2,3,4]]).T),  pd.Series([2,4,6,8], drange(-3)))
    assert eq(a(pd.Series([1,2,3,4], drange(-3)), np.array([1,2,3,4])),  pd.Series([2,4,6,8], drange(-3)))
    assert eq(a(pd.Series([1,2,3,4], drange(-3)), np.array([[1,2,3,4],[5,6,7,8]]).T),  pd.DataFrame({0:[2,4,6,8], 1:[6,8,10,12]}, drange(-3)))
    assert eq(a(np.array([1,2,3,4]), np.array([[1,2,3,4]]).T),  np.array([2,4,6,8]))


    #:Example 4: inner join alignment of columns in dataframes by default
    #---------------------------------------------------------------------
    x = pd.DataFrame({'a':[2,4,6,8], 'b':[6,8,10,12.]}, drange(-3))
    y = pd.DataFrame({'wrong':[2,4,6,8], 'columns':[6,8,10,12]}, drange(-3))
    assert len(a(x,y)) == 0    
    y = pd.DataFrame({'a':[2,4,6,8], 'other':[6,8,10,12.]}, drange(-3))
    assert eq(a(x,y),x[['a']]*2)
    y = pd.DataFrame({'a':[2,4,6,8], 'b':[6,8,10,12.]}, drange(-3))
    assert eq(a(x,y),x*2)
    y = pd.DataFrame({'column name for a single column dataframe is ignored':[1,1,1,1]}, drange(-3)) 
    assert eq(a(x,y),x+1)
    
    a = presync(addition, columns = 'outer')
    y = pd.DataFrame({'other':[2,4,6,8], 'a':[6,8,10,12]}, drange(-3))
    assert sorted(a(x,y).columns) == ['a','b','other']    

    #:Example 4: ffilling, bfilling
    #------------------------------
    x = pd.Series([1.,np.nan,3.,4.], drange(-3))    
    y = pd.Series([1.,np.nan,3.,4.], drange(-4,-1))    
    assert eq(a(x,y), pd.Series([np.nan, np.nan,7], drange(-3,-1)))

    #but, we provide easy conversion of internal parameters of presync:

    assert eq(a.ffill(x,y), pd.Series([2,4,7], drange(-3,-1)))
    assert eq(a.bfill(x,y), pd.Series([4,6,7], drange(-3,-1)))
    assert eq(a.oj(x,y), pd.Series([np.nan, np.nan, np.nan, 7, np.nan], drange(-4)))
    assert eq(a.oj.ffill(x,y), pd.Series([np.nan, 2, 4, 7, 8], drange(-4)))
    
    #:Example 5: indexing to a specific index
    #----------------------------------------
    index = pd.Index([dt(-3), dt(-1)])
    a = presync(addition, index = index)
    x = pd.Series([1.,np.nan,3.,4.], drange(-3))    
    y = pd.Series([1.,np.nan,3.,4.], drange(-4,-1))    
    assert eq(a(x,y), pd.Series([np.nan, 7], index))
    
    
    #:Example 6: returning complicated stuff
    #----------------------------------------
    a = pd.DataFrame(np.random.normal(0,1,(100,10)), drange(-99))
    b = pd.DataFrame(np.random.normal(0,1,(100,10)), drange(-99))

    def f(a, b):
        return (a*b, ts_sum(a), ts_sum(b))

    old = f(a,b)    
    self = presync(f)
    args = (); kwargs = dict(a = a, b = b)
    new = self(*args, **kwargs)
    assert eq(new, old)



