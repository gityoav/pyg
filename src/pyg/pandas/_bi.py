from pyg.base._types import is_pd, is_df, is_series, is_ts, is_arr, is_dict, is_num, is_str
from pyg.base._ulist import ulist
from pyg.base._loop import loop

from pyg.pandas._fill import fill
import operator
import pandas as pd
import numpy as np


__all__ = ['bi']

looperators = {key: loop(dict, pd.DataFrame, list, np.ndarray)(getattr(operator, key)) for key in ['add', 'and_', 'eq', 'floordiv', 'ge', 'gt', 'le', 'lt', 'mod', 'mul', 'ne', 'or_', 'pow', 'sub', 'truediv', 'xor']}


_how = dict(l = 'left', L = 'left', r = 'right', R = 'right', o = 'outer', O = 'outer', i = 'inner', I = 'inner', e = 'equal', E = 'equal')

        
    
def _as_how(how):
    return _how[(how or 'i')[0]]
    
def as_1d(value):
    if is_df(value) and value.shape[-1] == 1:
        return value.iloc[:,0]
    elif is_arr(value) and len(value.shape) == 2 and value.shape[1] == 1:
        return value.T[0]
    else:
        return value

def _columns(value):
    if isinstance(value, dict):
        return ulist(value)
    elif is_df(value):
        return ulist(value.columns)
    elif is_series(value) and not is_ts(value):
        return ulist(value.index)
    else:
        return None

def _joint_columns(lcols, rcols, how):
    """
    calculate the join columns 
    """
    how = _as_how(how)
    if how == 'equal':
        if sorted(rcols)!=sorted(lcols):
            raise ValueError('columns asserted to be equal but %s %s are not'%(sorted(lcols), sorted(rcols)))
    else:
        if how == 'inner':
            return lcols & rcols
        elif how == 'outer':
            return lcols | rcols
        elif how == 'left':
            return lcols
        elif how == 'right':
            return rcols
        else:
            raise ValueError('dont know how')

def _recolumn(value, cols, default = np.nan):
    """
    casts value to have the columns specified in cols
    """
    if is_dict(value):
        return type(value)({k: value.get(k, default) for k in cols})
    elif is_series(value):
        if is_ts(value):
            res = pd.concat([value] * len(cols), axis=1)
            res.columns = cols
            return res
        else: 
            return pd.Series({k: value[k] if k in value.index else default for k in cols})
    elif is_arr(value):
        if len(value.shape)==2 and value.shape[1] == len(cols):
            return value
        elif len(value.shape)==1 and value.shape[0] == len(cols):
            return value
        elif len(value.shape) == 1:
            return np.array([value]*len(cols)).T
        else:
            return value
    elif is_pd(value):
        return pd.DataFrame({k: value[k].values if k in value.columns else default for k in cols}, index = value.index)
    else:
        return value
        
@loop(list, tuple, dict)
def _np_reindex(a, n):
    """
    
    reshapes a to have length n
    
    >>> a = np.array([[1.,2.], [3.,4.]])
    >>> assert eq(_np_reindex(a, 1), np.array([[3., 4.]]))
    >>> assert eq(_np_reindex(a, 2), a)
    >>> assert eq(_np_reindex(a, 3), np.array([[np.nan,np.nan], [1.,2.], [3.,4.]]))

    >>> b = np.array([1.,2.])
    >>> assert eq(_np_reindex(b, 1), np.array([2.]))
    >>> assert eq(_np_reindex(b, 2), b)
    >>> assert eq(_np_reindex(b, 3), np.array([np.nan, 1.,2.]))


    """
    if not is_arr(a) or len(a.shape)<1:
        return a
    elif len(a) == n:
        return a
    elif len(a) > n:
        return a[-n:]
    else:
        if len(a.shape) == 1:
            return np.concatenate([np.full(n-len(a), np.nan), a])
        else:
            nshape = (n - len(a),) + a.shape[1:]
            return np.concatenate([np.full(nshape, np.nan), a])
        

def bi_pair(lhs, rhs, how = None, chow = None, method = None):
    lhs = as_1d(lhs); rhs = as_1d(rhs)
    if is_pd(lhs) and is_pd(rhs):
        how = _as_how(how)
        if how == 'equal':
            if len(lhs.index) != len(rhs.index) or not np.all(lhs.index == rhs.index):
                raise ValueError('indices asserted to be equal but %s %s are not'%(lhs.index, rhs.index))
        else:
            index = lhs.index.join(rhs.index, how = how)
            lhs = lhs.reindex(index = index)
            rhs = rhs.reindex(index = index)
    elif is_arr(lhs):
        if is_series(rhs) and lhs.shape == rhs.shape:
            lhs = pd.Series(lhs, rhs.index)                
        elif is_df(rhs):
            if len(lhs.shape) == 1: 
                if len(lhs) == len(rhs):
                    lhs = pd.Series(lhs, rhs.index)
                elif len(lhs) == rhs.shape[1]:
                    lhs = pd.Series(lhs, rhs.columns)            
            elif lhs.shape == rhs.shape:
                lhs = pd.DataFrame(lhs, index = rhs.index, columns = rhs.columns)
        elif is_arr(rhs):
            if len(lhs)!=len(rhs):
                how = _as_how(how)
                if how == 'equal':
                    raise ValueError('arrays asserted to be equal length but %s %s are not'%(len(lhs), len(rhs)))
                elif how == 'inner':
                    n = min(len(lhs), len(rhs))
                    lhs, rhs = _np_reindex([lhs, rhs], n)
                elif how == 'outer':
                    n = max(len(lhs), len(rhs))
                    lhs, rhs = _np_reindex([lhs, rhs], n)
                elif how == 'left':
                    rhs = _np_reindex(rhs, len(lhs))
                elif how == 'right':
                    lhs = _np_reindex(lhs, len(rhs))
            if len(lhs.shape)<len(rhs.shape):
                lhs = np.array([lhs] * rhs.shape[1]).T
            elif len(rhs.shape)<len(lhs.shape):
                rhs = np.array([rhs] * lhs.shape[1]).T
    lcols = _columns(lhs)
    rcols = _columns(rhs)
    if lcols is None and rcols is not None:
        lhs = _recolumn(lhs, rcols)
    elif lcols is not None and rcols is None:
        rhs = _recolumn(rhs, lcols)
    elif lcols is not None and rcols is not None:
        cols = _joint_columns(lcols, rcols, chow)
        lhs = _recolumn(lhs, cols)
        rhs = _recolumn(rhs, cols)
    return fill(lhs, method), fill(rhs, method)

class bi(object):
    """
    bi manages binary operations for pandas (and indeed dict)
    
    :Example:
    -------
    >>> from pyg.base import *; import pandas as pd; import numpy as np
    >>> df = pd.DataFrame(dict(a = [1,2,3],b=[4,5,6]))
    >>> assert eq(bi(df, chow='inner') + dict(a = 1, b = 2, c = 3), pd.DataFrame(dict(a = [2,3,4], b = [6,7,8])))
        
    >>> a = np.array([1,2,3])
    >>> b = np.array([2,3])
    >>> df.ij + a

    
    >>> lhs = pd.DataFrame(np.random.normal(0,1,(101,4)), drange(-100,100,2), columns = ['a','b','c','d'])
    >>> rhs = pd.DataFrame(np.random.normal(0,1,(101,4)), drange(-200,0,2), columns = ['a','b','c','e'])
    >>> lhs.ij + rhs    
    """
    def __init__(self, lhs, how = None, chow = 'ij', method = None, **kwargs):
        self.lhs = lhs
        self.how = how
        self.chow = chow
        self.method = method

    @property
    def ic(self):
        return bi(self.lhs, self.how, chow = 'inner', method = self.method)

    @property
    def oc(self):
        return bi(self.lhs, self.how, chow = 'outer', method = self.method)

    @property
    def lc(self):
        return bi(self.lhs, self.how, chow = 'left', method = self.method)

    @property
    def rc(self):
        return bi(self.lhs, self.how, chow = 'right', method = self.method)
 
    @property
    def ij(self):
        return bi(self.lhs, how = 'inner', chow = self.chow, method = self.method)

    @property
    def oj(self):
        return bi(self.lhs, how = 'outer', chow = self.chow, method = self.method)

    @property
    def lj(self):
        return bi(self.lhs, how = 'left', chow = self.chow, method = self.method)

    @property
    def rj(self):
        return bi(self.lhs, how = 'right', chow = self.chow, method = self.method)
 
    def pair(self, rhs):
        return bi_pair(self.lhs, rhs, how = self.how, chow = self.chow, method = self.method)
    
    def __str__(self):
        return 'bi(%s,%s)\n%s'%(self.how, self.method, self.lhs)

    def __repr__(self):
        return 'bi(%s,%s)\n%s'%(self.how, self.method, self.lhs.__repr__())
    
    def __call__(self, rhs):
        return pd.concat(self.pair(rhs), axis=1)


for key, function in looperators.items():
    __key__ = '__%s__'%key.replace('_', '')
    def f(self, rhs, function = function):
        return function(*self.pair(rhs))
    setattr(bi, __key__, f)    

    
pd.DataFrame.ij = property(lambda self: bi(self, how = 'inner'))
pd.DataFrame.oj = property(lambda self: bi(self, how = 'outer'))
pd.DataFrame.lj = property(lambda self: bi(self, how = 'left'))
pd.DataFrame.rj = property(lambda self: bi(self, how = 'right'))
pd.DataFrame.ej = property(lambda self: bi(self, how = 'equal'))

pd.Series.ij = property(lambda self: bi(self, how = 'inner'))
pd.Series.oj = property(lambda self: bi(self, how = 'outer'))
pd.Series.lj = property(lambda self: bi(self, how = 'left'))
pd.Series.rj = property(lambda self: bi(self, how = 'right'))
pd.Series.ej = property(lambda self: bi(self, how = 'equal'))

def ij(lhs, method = None, chow = 'ij', **kwargs):
    return bi(lhs, how = 'inner', chow = chow, method = method, **kwargs)

def oj(lhs, method = None, chow = 'ij', **kwargs):
    return bi(lhs, how = 'outer', chow = chow, method = method, **kwargs)

def lj(lhs, method = None, chow = 'ij', **kwargs):
    return bi(lhs, how = 'left', chow = chow, method = method, **kwargs)

def rj(lhs, method = None, chow = 'ij', **kwargs):
    return bi(lhs, how = 'right', chow = chow, method = method, **kwargs)

def ej(lhs, method = None, chow = 'ij', **kwargs):
    return bi(lhs, how = 'equal', chow = chow, method = method, **kwargs)
