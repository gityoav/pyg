from pyg import eq
import numpy as np
import pandas as pd
import datetime
from functools import partial

def test_eq():
    assert eq({}, {})
    assert eq(np.nan, np.nan) ## That's better
    assert eq(x = np.array([np.nan,2]), y = np.array([np.nan,2]))    
    assert eq(np.array([np.array([1,2]),2], dtype = 'object'), np.array([np.array([1,2]),2], dtype = 'object'))
    assert eq(np.array([np.nan,2]),np.array([np.nan,2]))    
    assert eq(dict(a = np.array([np.array([1,2]),2], dtype = 'object')) ,  dict(a = np.array([np.array([1,2]),2], dtype = 'object')))
    assert eq(dict(a = np.array([np.array([1,np.nan]),np.nan])) ,  dict(a = np.array([np.array([1,np.nan]),np.nan])))
    assert eq(np.array([np.array([1,2]),dict(a = np.array([np.array([1,2]),2]))]), np.array([np.array([1,2]),dict(a = np.array([np.array([1,2]),2]))]))
    
def test_eq_pandas():
    assert eq(x = pd.DataFrame([1,2]), y = pd.DataFrame([1,2]))
    assert eq(pd.DataFrame([np.nan,2]), pd.DataFrame([np.nan,2]))
    assert eq(pd.DataFrame([1,np.nan], columns = ['a']), pd.DataFrame([1,np.nan], columns = ['a']))
    assert not eq(pd.DataFrame([1,np.nan], columns = ['a']), pd.DataFrame([1,np.nan], columns = ['b']))

def test_eq_mistypes():
    assert eq(1, 1.0)
    assert eq(1, np.int32(1))
    assert eq(1, np.float32(1))
    assert eq(True, np.bool_(True))
    assert eq('True', np.str_('True'))
    x = datetime.datetime.now()
    y = np.datetime64(x)    
    assert eq(x,y)


def test_eq_inheritance():
    class FunnyDict(dict):
        def __getitem__(self, key):
            return 5
    assert dict(a = 1) == FunnyDict(a=1) ## equality seems to ignore any type mismatch
    assert not dict(a = 1)['a'] == FunnyDict(a = 1)['a'] 
    assert not eq(dict(a = 1), FunnyDict(a=1))    

def test_eq_partial():
    f = lambda a: a + 1    
    x = partial(f, a = 1)
    y = partial(f, a = 1)    
    assert not x == y
    assert eq(x, y)
    
def test_eq_weird():
    class Weird():
        def __eq__(self, other):
            raise ValueError('weird')
    x = Weird(); y = Weird()
    assert not eq(x, y)    
