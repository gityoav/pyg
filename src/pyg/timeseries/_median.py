from pyg.timeseries._rolling import _rolling_window
from pyg.timeseries._decorators import first_, _data_state
import bottleneck as bn


__all__ = ['rolling_median', 'rolling_median_', 'expanding_median']

def rolling_median(a, n, axis = 0, data = None, state = None):
    """
    equivalent to pandas a.rolling(n).median().
    
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
    data: None.
        unused at the moment. Allow code such as func(live, **func_(history)) to work
    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 
        
    :Example: agreement with pandas
    --------------------------------
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> panda = a.rolling(10).median(); ts = rolling_median(a,10)
    >>> assert abs(ts-panda).max()<1e-10   

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not include the nans in the rolling calculation: it skips them.
    Since pandas rolling engine does not skip nans, they propagate. 
    In fact, having removed half the data points, rolling(10) will return 99% of nans

    >>> a[a<0.1] = np.nan
    >>> panda = a.rolling(10).median(); ts = rolling_median(a,10)
    >>> print('#original:', len(nona(a)), 'timeseries:', len(nona(ts)), 'panda:', len(nona(panda)), 'data points')
    #original: 4634 timeseries: 4625 panda: 4 data points

    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.

    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = rolling_median(a,10)
    >>> old_ts = rolling_median_(old,10)
    >>> new_ts = rolling_median(new, 10, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(rolling_median(dict(x = a, y = a**2),10), dict(x = rolling_median(a,10), y = rolling_median(a**2,10)))
    >>> assert eq(rolling_median([a,a**2],10), [rolling_median(a,10), rolling_median(a**2,10)])

    """
    state = state or dict(vec = None)
    return first_(_rolling_window(a,n,n, func = bn.move_median, axis = axis, **state))

def rolling_median_(a, n, axis = 0, data = None, instate = None):
    """
    Equivalent to rolling_median(a) but returns also the state. 
    For full documentation, look at rolling_median.__doc__    
    """
    state = instate or dict(vec = None)
    return _data_state(['data','vec'],_rolling_window(a, window = n, min_count = n, func = bn.move_median, axis = axis, **state))

rolling_median_.output = ['data','state']


def expanding_median(a, axis = 0):
    """
    equivalent to pandas a.expanding().median().
    
    - works with np.arrays
    - handles nan without forward filling.
    - There is no state-aware version since this requires essentially the whole history to be stored.
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
        
    axis : int, optional
        0/1/-1. The default is 0.
            
    :Example: agreement with pandas
    --------------------------------
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> panda = a.expanding().median(); ts = expanding_median(a)
    >>> assert eq(ts,panda)    

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not forward fill the nans.    

    >>> a[a<0.1] = np.nan
    >>> panda = a.expanding().median(); ts = expanding_median(a)
    
    >>> pd.concat([panda,ts], axis=1)
    >>>                    0         1
    >>> 1993-09-23  1.562960  1.562960
    >>> 1993-09-24  0.908910  0.908910
    >>> 1993-09-25  0.846817  0.846817
    >>> 1993-09-26  0.821423  0.821423
    >>> 1993-09-27  0.821423       NaN
    >>>              ...       ...
    >>> 2021-02-03  0.870358  0.870358
    >>> 2021-02-04  0.870358       NaN
    >>> 2021-02-05  0.870358       NaN
    >>> 2021-02-06  0.870358       NaN
    >>> 2021-02-07  0.870353  0.870353
    
    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(expanding_median(dict(x = a, y = a**2)), dict(x = expanding_median(a), y = expanding_median(a**2)))
    >>> assert eq(expanding_median([a,a**2]), [expanding_median(a), expanding_median(a**2)])

    """

    return first_(_rolling_window(a,0,1, vec = None, func = bn.move_median, axis = axis))
