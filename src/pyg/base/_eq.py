import numpy as np
import pandas as pd
from functools import partial

__all__ = ['eq', 'in_']

def _eq_attrs(x, y, attrs):
    for attr in attrs:
        if hasattr(x, attr) and not eq(getattr(x, attr), getattr(y, attr)):
            return False
    return True


def eq(x, y):
    """
    A better nan-handling equality comparison. Here is the problem:
    
    >>> import numpy as np
    >>> assert not np.nan == np.nan  ## What?
    
    The nan issue extends to np.arrays...

    >>> assert list(np.array([np.nan,2]) == np.array([np.nan,2])) == [False, True]
    
    but not to lists...

    >>> assert [np.nan] == [np.nan]
    
    But wait, if the lists are derived from np.arrays, then no equality...

    >>> assert not list(np.array([np.nan])) == list(np.array([np.nan]))
    
    The other issue is inheritance:

    >>> class FunnyDict(dict):
    >>>    def __getitem__(self, key):
    >>>        return 5
    >>> assert dict(a = 1) == FunnyDict(a=1) ## equality seems to ignore any type mismatch
    >>> assert not dict(a = 1)['a'] == FunnyDict(a = 1)['a'] 
    
    There are also issues with partial

    >>> from functools import partial
    >>> f = lambda a: a + 1    
    >>> x = partial(f, a = 1)
    >>> y = partial(f, a = 1)    
    >>> assert not x == y

    >>> import pandas as pd
    >>> import pytest
    >>> from pyg import eq
    
    >>> assert eq(np.nan, np.nan) ## That's better
    >>> assert eq(x = np.array([np.nan,2]), y = np.array([np.nan,2]))    
    >>> assert eq(np.array([np.array([1,2]),2], dtype = 'object'), np.array([np.array([1,2]),2], dtype = 'object'))
    >>> assert eq(np.array([np.nan,2]),np.array([np.nan,2]))    
    >>> assert eq(dict(a = np.array([np.array([1,2]),2], dtype = 'object')) ,  dict(a = np.array([np.array([1,2]),2], dtype = 'object')))
    >>> assert eq(dict(a = np.array([np.array([1,np.nan]),np.nan])) ,  dict(a = np.array([np.array([1,np.nan]),np.nan])))
    >>> assert eq(np.array([np.array([1,2]),dict(a = np.array([np.array([1,2]),2]))]), np.array([np.array([1,2]),dict(a = np.array([np.array([1,2]),2]))]))
    
    >>> assert not eq(dict(a = 1), FunnyDict(a=1))    
    >>> assert eq(1, 1.0)
    >>> assert eq(x = pd.DataFrame([1,2]), y = pd.DataFrame([1,2]))
    >>> assert eq(pd.DataFrame([np.nan,2]), pd.DataFrame([np.nan,2]))
    >>> assert eq(pd.DataFrame([1,np.nan], columns = ['a']), pd.DataFrame([1,np.nan], columns = ['a']))
    >>> assert not eq(pd.DataFrame([1,np.nan], columns = ['a']), pd.DataFrame([1,np.nan], columns = ['b']))
    
    """
    if x is y:
        return True
    elif isinstance(x, (tuple, list)):
        return type(x) == type(y) and len(x) == len(y) and _eq_attrs(x,y,['__shape__']) and (len(x) == 0 or min([eq(i,j) for i,j in zip(x,y)]))
    elif isinstance(x, np.ndarray):
        return type(x) == type(y) and len(x) == len(y) and _eq_attrs(x,y,['__shape__']) and (0 in x.shape or np.all(veq(x,y)))
    elif isinstance(x, (pd.DataFrame, pd.Series)):
        return type(x)==type(y) and _eq_attrs(x,y, attrs = ['__shape__', 'index', 'columns']) and (0 in x.shape or np.all(veq(x,y)))
    elif isinstance(x, dict):
        if type(x) == type(y) and len(x)==len(y):
            if len(x) == 0:
                return True
            xkey, xval = zip(*sorted(x.items()))
            ykey, yval = zip(*sorted(y.items()))
            return eq(xkey, ykey) and eq(np.array(xval, dtype='object'), np.array(yval, dtype='object'))
        else:
            return False
    elif isinstance(x, float) and np.isnan(x):
        return isinstance(y, float) and np.isnan(y)    
    elif isinstance(x, partial):
        return type(x) == type(y) and x.func == y.func and eq(x.keywords, y.keywords) and eq(x.args, y.args)
    else:
        try:
            res = x == y
            return np.all(res.__array__()) if hasattr(res, '__array__') else res
        except Exception:
            return False # if you really have no == supported, the two items are not the same

veq = np.vectorize(eq)


def in_(x, seq):
    """
    Evaluates if x is in seq, avoiding issues such as these:

    >>> s = pd.Series([1,2,3])
    >>> with pytest.raises(ValueError):
    >>>     s in [None]    
    >>> assert not in_(s, [None])
    >>> assert in_(s, [None, s])    
    """
    for s in seq:
        if eq(x, s):
            return True
    return False
