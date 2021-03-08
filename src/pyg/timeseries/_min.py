from pyg.timeseries._rolling import _rolling_window
import bottleneck as bn
from pyg.base import pd2np, loop_all
from pyg.timeseries._decorators import first_, compiled, _data_state
import numpy as np

__all__ = ['rolling_min', 'rolling_min_', 'expanding_min', 'expanding_min_']

def rolling_min(a, n, axis = 0, data = None, state = None):
    """
    equivalent to pandas a.rolling(n).min().
    
    - works with np.arrays
    - handles nan without forward filling.
    - supports state parameters
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
    n: int
        size of rolling window
    axis : int, optional
        0/1/-1. The default is 0.    
    vec,data:
        state parameters to instantiate the calculation. vec = recent history
        
    :Example: agreement with pandas
    --------------------------------
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> panda = a.rolling(10).min(); ts = rolling_min(a,10)
    >>> assert abs(ts-panda).min()<1e-10   

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not include the nans in the rolling calculation: it skips them.
    Since pandas rolling engine does not skip nans, they propagate. 
    In fact, having removed half the data points, rolling(10) will return 99% of nans

    >>> a[a<0.1] = np.nan
    >>> panda = a.rolling(10).min(); ts = rolling_min(a,10)
    >>> print('#original:', len(nona(a)), 'timeseries:', len(nona(ts)), 'panda:', len(nona(panda)), 'data points')
    >>> #original: 4534 timeseries: 4525 panda: 6 data points

    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.

    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = rolling_min(a,10)
    >>> old_ts = rolling_min_(old,10)
    >>> new_ts = rolling_min(new, 10, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(rolling_min(dict(x = a, y = a**2),10), dict(x = rolling_min(a,10), y = rolling_min(a**2,10)))
    >>> assert eq(rolling_min([a,a**2],10), [rolling_min(a,10), rolling_min(a**2,10)])

    """
    state = state or {}
    return first_(_rolling_window(a,n,n, func = bn.move_min, axis = axis, **state))

def rolling_min_(a, n, vec = None, axis = 0, data = None, instate = None):
    """
    Equivalent to rolling_min(a) but returns also the state. 
    For full documentation, look at rolling_min.__doc__    
    """
    state = instate or {}
    return _data_state(['data','vec'],_rolling_window(a,n,n, func = bn.move_min, axis = axis, **state))

rolling_min_.output = ['data','state']

@loop_all
@pd2np
@compiled
def _expanding_min(a, m = np.nan):
    res = np.empty_like(a)
    for i0 in range(a.shape[0]):
        if np.isnan(a[i0]):
            res[i0] = np.nan
        else:
            res[i0] = m = m if m<a[i0] else a[i0]
            break
    for i in range(i0+1, a.shape[0]):
        if np.isnan(a[i]):
            res[i] = np.nan
        else:
            res[i] = m = m if m<a[i] else a[i]
    return res, m


def expanding_min_(a, axis = 0, data = None, instate = None):
    """
    Equivalent to a.expanding().min() but returns the full state: i.e. both 
    data: the expanding().min()
    m: the current minimum
    """
    state = instate or {}
    return _data_state(['data','m'], _expanding_min(a, axis = axis, **state))

expanding_min_.output = ['data','state']


def expanding_min(a, axis = 0, data = None, state = None):
    """
    equivalent to pandas a.expanding().min().
    
    - works with np.arrays
    - handles nan without forward filling.
    - supports state parameters
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
        
    axis : int, optional
        0/1/-1. The default is 0.
    
    m,data:
        state parameters to instantiate the calculation. m = min so far
        
    :Example: agreement with pandas
    --------------------------------
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> panda = a.expanding().min(); ts = expanding_min(a)
    >>> assert eq(ts,panda)    

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not forward fill the nans.    

    >>> a[a<0.1] = np.nan
    >>> panda = a.expanding().min(); ts = expanding_min(a)
    
    >>> pd.concat([panda,ts], axis=1)
    >>>                    0         1
    >>> 1993-09-24       NaN       NaN
    >>> 1993-09-25       NaN       NaN
    >>> 1993-09-26  0.775176  0.775176
    >>> 1993-09-27  0.691942  0.691942
    >>> 1993-09-28  0.691942       NaN
    >>>              ...       ...
    >>> 2021-02-04  0.100099  0.100099
    >>> 2021-02-05  0.100099       NaN
    >>> 2021-02-06  0.100099       NaN
    >>> 2021-02-07  0.100099  0.100099
    >>> 2021-02-08  0.100099  0.100099
    
    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.
    
    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = expanding_min(a)
    >>> old_ts = expanding_min_(old)
    >>> new_ts = expanding_min(new, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(expanding_min(dict(x = a, y = a**2)), dict(x = expanding_min(a), y = expanding_min(a**2)))
    >>> assert eq(expanding_min([a,a**2]), [expanding_min(a), expanding_min(a**2)])

    """
    state = state or {}
    return first_(_expanding_min(a, axis = axis, **state))

