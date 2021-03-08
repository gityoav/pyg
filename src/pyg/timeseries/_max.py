from pyg.timeseries._rolling import _rolling_window
import bottleneck as bn
from pyg.base import pd2np, loop_all
from pyg.timeseries._decorators import first_, compiled, _data_state
import numpy as np

__all__ = ['rolling_max', 'rolling_max_', 'expanding_max', 'expanding_max_']

def rolling_max(a, n, axis = 0, data = None, state = None):
    """
    equivalent to pandas a.rolling(n).max().
    
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
    >>> panda = a.rolling(10).max(); ts = rolling_max(a,10)
    >>> assert abs(ts-panda).max()<1e-10   

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not include the nans in the rolling calculation: it skips them.
    Since pandas rolling engine does not skip nans, they propagate. 
    In fact, having removed half the data points, rolling(10) will return 99% of nans

    >>> a[a<0.1] = np.nan
    >>> panda = a.rolling(10).max(); ts = rolling_max(a,10)
    >>> print('#original:', len(nona(a)), 'timeseries:', len(nona(ts)), 'panda:', len(nona(panda)), 'data points')
    >>> #original: 4534 timeseries: 4525 panda: 6 data points

    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.

    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = rolling_max(a,10)
    >>> old_ts = rolling_max_(old,10)
    >>> new_ts = rolling_max(new, 10, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(rolling_max(dict(x = a, y = a**2),10), dict(x = rolling_max(a,10), y = rolling_max(a**2,10)))
    >>> assert eq(rolling_max([a,a**2],10), [rolling_max(a,10), rolling_max(a**2,10)])

    """
    state = state or dict(vec = None) 
    return first_(_rolling_window(a, window = n, min_count = n, func = bn.move_max, axis = axis, **state))

def rolling_max_(a, n, axis = 0, data = None, instate = None):
    """
    Equivalent to rolling_max(a) but returns also the state. 
    For full documentation, look at rolling_max.__doc__    
    """
    state = instate or dict(vec = None) 
    return _data_state(['data','vec'],_rolling_window(a, window = n, min_count = n, func = bn.move_max, **state))

rolling_max_.output = ['data','state']


@loop_all
@pd2np
@compiled
def _expanding_max(a, m = np.nan):
    res = np.empty_like(a)
    for i0 in range(a.shape[0]):
        if np.isnan(a[i0]):
            res[i0] = np.nan
        else:
            res[i0] = m = m if m>a[i0] else a[i0]
            break
    for i in range(i0+1, a.shape[0]):
        if np.isnan(a[i]):
            res[i] = np.nan
        else:
            res[i] = m = m if m>a[i] else a[i]
    return res, m


def expanding_max_(a, axis = 0, data = None, instate = None):
    """
    Equivalent to a.expanding().max() but returns the full state: i.e. both 
    data: the expanding().max()
    m: the current maximum
    """
    state = instate or {}
    return _data_state(['data','m'], _expanding_max(a, axis = axis, **state))

expanding_max_.output = ['data','state']


def expanding_max(a, axis = 0, data = None, state = None):
    """
    equivalent to pandas a.expanding().max().
    
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
        state parameters to instantiate the calculation. m = max so far
        
    :Example: agreement with pandas
    --------------------------------
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> panda = a.expanding().max(); ts = expanding_max(a)
    >>> assert eq(ts,panda)    

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not forward fill the nans.    

    >>> a[a<0.1] = np.nan
    >>> panda = a.expanding().max(); ts = expanding_max(a)
    
    >>> pd.concat([panda,ts], axis=1)
    >>>                    0         1
    >>> 1993-09-24       NaN       NaN
    >>> 1993-09-25       NaN       NaN
    >>> 1993-09-26  0.875409  0.875409
    >>> 1993-09-27  0.875409       NaN
    >>> 1993-09-28  0.875409       NaN
    >>>              ...       ...
    >>> 2021-02-04  3.625858  3.625858
    >>> 2021-02-05  3.625858       NaN
    >>> 2021-02-06  3.625858  3.625858
    >>> 2021-02-07  3.625858       NaN
    >>> 2021-02-08  3.625858       NaN
    
    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.
    
    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = expanding_max(a)
    >>> old_ts = expanding_max_(old)
    >>> new_ts = expanding_max(new, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(expanding_max(dict(x = a, y = a**2)), dict(x = expanding_max(a), y = expanding_max(a**2)))
    >>> assert eq(expanding_max([a,a**2]), [expanding_max(a), expanding_max(a**2)])

    """
    state = state or {}
    return first_(_expanding_max(a, axis = axis, **state))