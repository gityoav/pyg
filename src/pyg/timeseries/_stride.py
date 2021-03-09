import numpy as np
from pyg.timeseries._rolling import _fnna, _vec
from pyg.timeseries._decorators import _data_state, first_
from pyg.base import pd2np, as_list, loop_all, loop, is_pd


__all__ = ['rolling_quantile', 'rolling_quantile_']

def _as_strided(a, L, S = 1):
    nrows = ((a.size - L)//S) + 1
    n = a.strides[0]
    return np.lib.stride_tricks.as_strided(a, shape = (nrows, L), strides = (S*n,n))

def _cast_strided_result(a, va, res, mask, n):
    n0 = int(_fnna(a, n))
    if n>0:
        mask[:n0] = False
    else:
        mask[n0+1:] = False
    va[mask] = res
    va[~mask] = np.nan
    return va

@loop_all
@pd2np
def _rolling_quantile(a, n, quantile, vec = None):
    vec = _vec(vec,0)
    if len(vec):
        a_ = np.concatenate([vec,a])
    else:
        a_ = a
    mask = ~np.isnan(a_)
    na = a_[mask]
    if len(na) < n:
        return a + np.nan
    quantile = np.array(quantile)
    va = np.empty_like(a_) if len(quantile.shape) == 0 else np.empty([a_.shape[0], quantile.shape[0]])
    strided = _as_strided(na, abs(n), 1)
    res = np.quantile(strided, quantile, axis = 1).T
    rtn = _cast_strided_result(a_, va, res, mask, n)
    if len(vec):
        rtn = rtn[-len(a):]
    return rtn, na[-(n-1):]

def rolling_quantile(a, n, quantile = 0.5, axis = 0, data = None, state = None):
    """
    equivalent to a.rolling(n).quantile(q) except...
    - supports numpy arrays
    - supports multiple q values

    :Example:
    -------
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> res = rolling_quantile(a, 100, 0.3)
    >>> assert sub_(res, a.rolling(100).quantile(0.3)).max() < 1e-13

    :Example: multiple quantiles
    ---------------------------------------------
    >>> res = rolling_quantile(a, 100, [0.3, 0.5, 0.75])
    >>> assert abs(res[0.3] - a.rolling(100).quantile(0.3)).max() < 1e-13

    :Example: state management
    ---------------------------------------------
    >>> res = rolling_quantile(a, 100, 0.3)
    >>> old = rolling_quantile_(a.iloc[:2000], 100, 0.3)
    >>> new = rolling_quantile(a.iloc[2000:], 100, 0.3, **old)
    >>> both = pd.concat([old.data, new])
    >>> assert eq(both, res)
    
    :Parameters:
    ----------------
    a : array/timeseries
    n : integer
        window size.
    q : float or list of floats in [0,1]
        quantile(s).
    data: None.
        unused at the moment. Allow code such as func(live, **func_(history)) to work
    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 

    :Returns:
    -------
    timeseries/array of quantile(s)

    """
    qs = as_list(quantile)
    if len(getattr(a, 'shape', [])) == 2 and a.shape[1] > 1:
        if len(qs) > 1:
            raise ValueError('Can do multiple quantiles %s only for single-column data'%qs)
        else:
            qs = qs[0]
    state = state or {}
    res = first_(_rolling_quantile(a, n = n , quantile = qs, axis = axis, **state))
    qs = as_list(quantile)
    @loop(list, dict)
    def add_qs(res):
        if is_pd(res) and len(res.shape) == 2 and res.shape[1] == len(qs):
            res.columns = qs
        return res
    return add_qs(res)

def rolling_quantile_(a, n, quantile = 0.5, axis = 0, data = None, instate = None):
    """
    Equivalent to rolling_quantile(a) but returns also the state. 
    For full documentation, look at rolling_quantile.__doc__    
    """
    qs = as_list(quantile)
    if len(getattr(a, 'shape', [])) == 2 and a.shape[1] > 1:
        if len(qs) > 1:
            raise ValueError('Can do multiple quantiles %s only for single-column data'%qs)
        else:
            qs = qs[0]
    state = instate  or {}
    res = _data_state(['data','vec'],_rolling_quantile(a, n = n , quantile = qs, axis = axis, **state))
    qs = as_list(quantile)
    @loop(list, dict)
    def add_qs(res):
        if is_pd(res) and len(res.shape) == 2 and res.shape[1] == len(qs):
            res.columns = qs
        return res
    return add_qs(res)

rolling_quantile.ouput = ['data', 'state']