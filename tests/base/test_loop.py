from pyg import loop, eq, drange, Dict
import pandas as pd; import numpy as np
import pytest
from numpy import array

SP = lambda a, b: Dict(s = a+b, p = a*b)
AB = lambda a, b: a+b

def S(v):
    if isinstance(v, list):
        return [S(w) for w in v]
    else:
        return v.s

def test_loop_dict():
    f = loop(dict)(AB)
    assert f(1,2) == 3
    assert f(1, b=2) == 3
    assert f(a=1, b=2) ==3
    assert f(dict(a=1,b=2), 2) == dict(a = 3, b = 4)
    assert f(dict(a=1,b=2), dict(a=2, b=3)) == dict(a = 3, b = 5)
    b = pd.Series([2,3], ['a','b'])
    assert f(dict(a=1,b=2), b) == dict(a = 3, b = 5)
    b = pd.DataFrame([[2,3],[3,4]], columns = ['a','b'])
    assert eq(f(dict(a=1,b=2), b), dict(a = 1 + b.a, b = 2 + b.b))
    
    with pytest.raises(TypeError):
        f(dict(a=1,b=2), dict(a=2, b=3, c=6))
    with pytest.raises(TypeError):
        f(dict(a=1,b=2), [2,2]) 

def test_loop_list():
    f = loop(list)(AB)
    assert f(1,2) == 3
    assert f(1, b=2) == 3
    assert f(a=1, b=2) ==3
    assert f([1,2], 2) == [3,4]
    assert f([1,2], [2,3]) == [3,5]
    assert f([1,2], (2,3)) == [3,5]
    assert f([1,2], np.array([2,3])) == [3,5]
    assert f([1,2], pd.Series([2,3])) == [3,5]
    b = pd.Series([2,3], drange(-1)) # a timeseries is considered a single object
    assert eq(f([1,2], b), [1+b, 2+b])
    b = pd.DataFrame([[2,3],[3,4]], columns = ['a','b'])
    assert eq(f([1,2], b), [1+b.a, 2+b.b])
    b = pd.DataFrame([[2,3,4],[3,4,5], [5,6,7]], columns = ['a','b','c'])
    assert eq(f([1,2], b), [1+b, 2+b])
    with pytest.raises(TypeError):
        f([1,2], [2,2,3]) 

    assert f((1,2),(3,4)) == (1,2,3,4)


def test_loop_list_dict():
    f = loop(list)(SP)
    assert f(1,2).s == 3
    assert f(1, b=2).s == 3
    assert f(a=1, b=2).s ==3
    assert S(f([1,2], 2)) == [3,4]
    assert S(f([1,2], [2,3])) == [3,5]
    assert S(f([1,2], (2,3))) == [3,5]
    assert S(f([1,2], np.array([2,3]))) == [3,5]
    assert S(f([1,2], pd.Series([2,3]))) == [3,5]
    b = pd.Series([2,3], drange(-1)) # a timeseries is considered a single object
    assert eq(S(f([1,2], b)), [1+b, 2+b])
    b = pd.DataFrame([[2,3],[3,4]], columns = ['a','b'])
    assert eq(S(f([1,2], b)), [1+b.a, 2+b.b])
    b = pd.DataFrame([[2,3,4],[3,4,5], [5,6,7]], columns = ['a','b','c'])
    assert eq(S(f([1,2], b)), [1+b, 2+b])
    with pytest.raises(TypeError):
        f([1,2], [2,2,3]) 



def test_loop_tuple():
    f = loop(tuple)(AB)
    assert f(1,2) == 3
    assert f(1, b=2) == 3
    assert f(a=1, b=2) ==3
    assert f((1,2), 2) == (3,4)
    assert f((1,2), [2,3]) == (3,5)
    assert f((1,2), np.array([2,3])) == (3,5)
    assert f((1,2), pd.Series([2,3])) == (3,5)
    b = pd.Series([2,3], drange(-1)) # a timeseries is considered a single object
    assert eq(f((1,2), b), (1+b, 2+b))
    b = pd.DataFrame([[2,3],[3,4]], columns = ['a','b'])
    assert eq(f((1,2), b), (1+b.a, 2+b.b))
    b = pd.DataFrame([[2,3,4],[3,4,5], [5,6,7]], columns = ['a','b','c'])
    assert eq(f((1,2), b), (1+b, 2+b))
    with pytest.raises(TypeError):
        f((1,2), [2,2,3]) 

    assert f([1,2],[3,4]) == [1,2,3,4]


def test_loop_series():
    f = loop(pd.Series)(AB)
    assert f(1,2) == 3
    assert f(1, b=2) == 3
    assert f(a=1, b=2) ==3
    a = pd.Series(dict(a=1,b=2))
    assert eq(f(a, 2) , a+2)
    assert eq(f(a,dict(a=2, b=3)) , pd.Series(dict(a = 3, b = 5)))
    ats = pd.Series([1,2], drange(-1))    
    assert eq(f(ats, 1) , ats+1)
    assert eq(list(f(a, [1,2]).values), [array([2, 3]), array([3, 4])])
    assert eq(list(f(a, array([1,2])).values), [array([2, 3]), array([3, 4])])
    f = loop(pd.Series)(np.std)
    assert eq(f(a) , pd.Series(dict(a=0, b=0)))
    assert f(ats) == 0.5
    
    
    
def test_loop_df():
    f = loop(pd.DataFrame)(AB)
    assert f(1,2) == 3
    assert f(1, b=2) == 3
    assert f(a=1, b=2) ==3
    a = pd.DataFrame(dict(a=[1,2],b=[2,3]))
    assert eq(f(a, 1), a + 1)
    assert eq(f(a, [1,2]), pd.DataFrame(dict(a=[2,3],b=[2+2,3+2])))
    assert eq(f(a, dict(a=1,b=2)), pd.DataFrame(dict(a=[2,3],b=[2+2,3+2])))
    ats = pd.DataFrame(dict(a=[1,2],b=[2,3]), drange(-1))
    f = loop(pd.Series)(np.std)
    assert eq(f(a), pd.Series(dict(a=0.5, b=0.5)))
    assert eq(f(ats), pd.Series(dict(a=0.5, b=0.5)))

    a = pd.DataFrame(dict(a=[1,2,3,4],b=[2,3,0,1]))
    b = np.array([1,2,3,4])
    f = loop(pd.DataFrame)(AB)
    assert eq(f(a,b).a.values, a.a.values + b)
    assert eq(f(a,b).b.values, a.b.values + b)

    b = np.array([[1,2,3,4],[0,-1,-2,-3]]).T
    assert eq(f(a,b).a.values, a.a.values + b.T[0])
    assert eq(f(a,b).b.values, a.b.values + b.T[1])

def test_loop_df_dict():
    f = loop(pd.DataFrame)(SP)
    assert f(1,2).s == 3
    assert f(1, b=2).s == 3
    assert f(a=1, b=2).s ==3
    a = pd.DataFrame(dict(a=[1,2],b=[2,3]))
    assert eq(f(a, 1).s, a + 1)
    assert eq(f(a, [1,2]).s, pd.DataFrame(dict(a=[2,3],b=[2+2,3+2])))
    assert eq(f(a, dict(a=1,b=2)).s, pd.DataFrame(dict(a=[2,3],b=[2+2,3+2])))
    ats = pd.DataFrame(dict(a=[1,2],b=[2,3]), drange(-1))
    f = loop(pd.Series)(lambda a: Dict(s = np.std(a)))
    assert eq(f(a).s, pd.Series(dict(a=0.5, b=0.5)))
    assert eq(f(ats).s, pd.Series(dict(a=0.5, b=0.5)))

    a = pd.DataFrame(dict(a=[1,2,3,4],b=[2,3,0,1]))
    b = np.array([1,2,3,4])
    f = loop(pd.DataFrame)(SP)
    assert eq(f(a,b).s.a.values, a.a.values + b)
    assert eq(f(a,b).s.b.values, a.b.values + b)

    b = np.array([[1,2,3,4],[0,-1,-2,-3]]).T
    assert eq(f(a,b).s.a.values, a.a.values + b.T[0])
    assert eq(f(a,b).s.b.values, a.b.values + b.T[1])


    

def test_loop_index_and_columns():
    a = pd.DataFrame(dict(a=[1,2,3,4],b=[2,3,0,1]), index = drange(3))
    b = np.array([1,2,3,4])
    f = loop(pd.DataFrame, pd.Series)(AB)
    assert eq(f(a,b).index, a.index)
    assert eq(f(a,b).columns, a.columns)
    s = pd.Series([1,2,3,4], drange(3))
    assert eq(f(s,b).index, s.index)

def test_loop_index_and_columns_dict():
    f = loop(pd.DataFrame, pd.Series)(SP)
    a = pd.DataFrame(dict(a=[1,2,3,4],b=[2,3,0,1]), index = drange(3))
    b = np.array([1,2,3,4])
    assert eq(f(a,b).s.index, a.index)
    assert eq(f(a,b).s.columns, a.columns)
    s = pd.Series([1,2,3,4], drange(3))
    assert eq(f(s,b).s.index, s.index)


    
def test_loop_df_axis1():
    f = loop(pd.DataFrame)(AB)
    a = pd.DataFrame(dict(a=[1,2,3,4],b=[2,3,0,1]))
    b = np.array([1,2])
    assert eq(f(a,b, axis=1).a.values, a.a.values + 1)
    assert eq(f(a,b, axis=1).b.values, a.b.values + 2)

def test_loop_df_axis1_dict():
    f = loop(pd.DataFrame)(SP)
    a = pd.DataFrame(dict(a=[1,2,3,4],b=[2,3,0,1]))
    b = np.array([1,2])
    assert eq(f(a,b, axis=1).s.a.values, a.a.values + 1)
    assert eq(f(a,b, axis=1).s.b.values, a.b.values + 2)



        
def test_loop_df_ndarray():
    f = loop(np.ndarray)(AB)
    a = pd.DataFrame(dict(a=[1,2,3,4],b=[2,3,0,1]))
    b = np.array([[1,2,3,4],[0,-1,-2,-3]]).T
    assert eq(f(b,a).T[0], a.a.values + b.T[0])
    assert eq(f(b,a).T[1], a.b.values + b.T[1])
    a = [1,2]        
    assert eq(f(b,a).T[0], b.T[0]+1)
    assert eq(f(b,a).T[1], b.T[1]+2)
    a = pd.DataFrame(dict(a=[1,2,3,4]))
    assert eq(f(b,a), np.array([[2,4,6,8],[1,1,1,1]]).T)
    a = a.values
    assert eq(f(b,a), np.array([[2,4,6,8],[1,1,1,1]]).T)

def test_loop_df_ndarray_dict():
    f = loop(np.ndarray)(SP)
    a = pd.DataFrame(dict(a=[1,2,3,4],b=[2,3,0,1]))
    b = np.array([[1,2,3,4],[0,-1,-2,-3]]).T
    assert eq(f(b,a).s.T[0], a.a.values + b.T[0])
    assert eq(f(b,a).s.T[1], a.b.values + b.T[1])
    a = [1,2]        
    assert eq(f(b,a).s.T[0], b.T[0]+1)
    assert eq(f(b,a).s.T[1], b.T[1]+2)
    a = pd.DataFrame(dict(a=[1,2,3,4]))
    assert eq(f(b,a).s, np.array([[2,4,6,8],[1,1,1,1]]).T)
    a = a.values
    assert eq(f(b,a).s, np.array([[2,4,6,8],[1,1,1,1]]).T)
    
 
    