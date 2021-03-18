import numpy as np
from pyg.timeseries._math import stdev_calculation, skew_calculation
from pyg.timeseries._decorators import compiled, first_, _data_state
from pyg.timeseries._rolling import _vec
from pyg.base import pd2np, Dict, loop, loop_all

__all__ = ['ts_std', 'ts_mean', 'ts_skew', 'ts_count', 'ts_min', 'ts_max', 'ts_rms', 'ts_median',  'ts_sum', 'nona',
           'ts_std_', 'ts_mean_', 'ts_skew_', 'ts_count_', 'ts_min_', 'ts_max_', 'ts_rms_', 'ts_sum_']

###############
##
## calculators
##
###############

@loop_all
@pd2np
def _median(a):
    return np.median(a[~np.isnan(a)])

def ts_median(a, axis = 0):
    return _median(a, axis = axis)

@pd2np
@compiled
def _moments(a, vec):
    res = vec.copy()
    for i in range(a.shape[0]):
        if not np.isnan(a[i]):
            v = 1
            for j in range(vec.shape[0]-1):
                res[j] += v
                v = v  * a[i]
            res[-1] += v
    return res

@pd2np
@compiled
def _min(a, m = np.inf):
    for i in range(a.shape[0]):
        if not np.isnan(a[i]) and a[i]<m:
            m = a[i]
    return m

@pd2np
@compiled
def _max(a, m = np.inf):
    for i in range(a.shape[0]):
        if not np.isnan(a[i]) and a[i]>m:
            m = a[i]
    return m

@loop_all
def _ts_min(a, m = None):
    if m is None or np.isnan(m):
        m = np.inf
    m = _min(a, m)
    if np.isinf(m):
        return np.nan, m
    else:
        return m, m

def ts_min(a, axis = 0, data = None, state = None):
    """
    ts_min(a) is equivalent to pandas a.min()
    """
    state = state or {}
    return first_(_ts_min(a, axis = axis, **state))
    
def ts_min_(a, axis = 0, data = None, instate = None):
    """
    ts_min(a) is equivalent to pandas a.min()
    """
    state = instate or {}
    res = _ts_min(a, axis = axis, **state)
    return _data_state(['data', 'm'], res)

ts_min_.output = ['data', 'state']

@loop_all
def _ts_max(a, m = None):
    if m is None or np.isnan(m):
        m = -np.inf
    m = _max(a, m)
    if np.isinf(m):
        return np.nan, m
    else:
        return m, m
    
def ts_max(a, axis = 0, data = None, state = None):
    """
    ts_max(a) is equivalent to pandas a.min()
    """
    state = state or {}
    return first_(_ts_max(a, axis = axis, **state))

def ts_max_(a, axis = 0, data = None, instate = None):
    """
    ts_max(a) is equivalent to pandas a.min()
    """
    state = instate or {}
    res = _ts_max(a, axis = axis, **state)
    return _data_state(['data', 'm'], res)

ts_max_.output = ['data', 'state']

def _zip(value):
    return _data_state(['data', 'vec'], value)

@loop_all
def _ts_count(a, vec = None):
    vec = _vec(vec,1,0)
    vec = _moments(a, vec)
    return vec[0], vec
    

def ts_count(a, axis = 0, data = None, state = None):
    """
    ts_count(a) is equivalent to a.count() (though slightly slower)
    
    - supports numpy arrays 
    - skips nan
    - supports state management
    
    :Example: pandas matching
    -----------------------------------
    >>> # create sample data:
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999)); a[a>0] = np.nan
    >>> assert ts_count(a) == a.count()

    :Example: numpy 
    -----------------------------------
    >>> assert ts_count(a.values) == ts_count(a)

    :Example: state management
    -------------------------------------------
    >>> old = ts_count_(a.iloc[:2000])
    >>> new = ts_count(a.iloc[2000:], state = old.state)
    >>> assert new == ts_count(a)

    """
    state = state or {}
    return first_(_ts_count(a, axis = axis, **state))

def ts_count_(a, axis = 0, data = None, instate = None):
    """
    ts_count_(a) is equivalent to ts_count(a) except vec is also returned.
    See ts_count for full documentation        
    """
    state = instate or {}
    return _zip(_ts_count(a, axis = axis, **state))

@loop_all
def _ts_sum(a, vec = None):
    vec = _vec(vec,2,0.)
    vec = _moments(a, vec)
    return vec[1], vec
    
def ts_sum(a, axis = 0, data = None, state = None):
    """
    ts_sum(a) is equivalent to a.sum()
    
    - supports numpy arrays 
    - handles nan
    - supports state management

    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
        
    axis : int, optional
        0/1/-1. The default is 0.

    data: None
        unused at the moment. Allow code such as func(live, **func_(history)) to work

    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 
    

    :Example: pandas matching
    -----------------------------------
    >>> # create sample data:
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999)); a[a>0] = np.nan
    >>> assert ts_sum(a) == a.sum()

    :Example: numpy 
    -----------------------------------
    >>> assert ts_sum(a.values) == ts_sum(a)

    :Example: state management
    -------------------------------------------
    >>> old = ts_sum_(a.iloc[:2000])
    >>> new = ts_sum(a.iloc[2000:], vec = old.vec)
    >>> assert new == ts_sum(a)
    """
    state = state or {}
    return first_(_ts_sum(a, axis = axis, **state))

def ts_sum_(a, axis = 0, data = None, instate = None):
    """
    ts_sum_(a) is equivalent to ts_sum(a) except vec is also returned.
    See ts_sum for full documentation        
    """
    state = instate or {}
    return _zip(_ts_sum(a, axis = axis, **state))

@loop_all
def _ts_mean(a, vec = None):
    vec = _vec(vec,2,0.)
    vec = _moments(a, vec)
    return vec[1]/vec[0], vec

def ts_mean(a, axis = 0, data = None, state = None):
    """
    ts_mean(a) is equivalent to a.mean()
    
    - supports numpy arrays 
    - handles nan
    - supports state management
    - pandas is actually faster on count
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
        
    axis : int, optional
        0/1/-1. The default is 0.

    data: None
        unused at the moment. Allow code such as func(live, **func_(history)) to work

    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 

    :Example: pandas matching
    -----------------------------------
    >>> # create sample data:
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999)); a[a>0] = np.nan
    >>> assert ts_mean(a) == a.mean()

    :Example: numpy 
    -----------------------------------
    >>> assert ts_mean(a.values) == ts_mean(a)

    :Example: state management
    -------------------------------------------
    >>> old = ts_mean_(a.iloc[:2000])
    >>> new = ts_mean(a.iloc[2000:], vec = old.vec)
    >>> assert new == ts_mean(a)
    """
    state = state or {}
    return first_(_ts_mean(a, axis = axis, **state))

def ts_mean_(a, axis = 0, data = None, instate = None):
    """
    ts_mean_(a) is equivalent to ts_mean(a) except vec is also returned.
    See ts_mean for full documentation        
    """
    state = instate or {}
    return _zip(_ts_mean(a, axis = axis, **state))

@loop_all
def _ts_std(a, vec = None):
    vec = _vec(vec,3,0.)
    vec = _moments(a, vec)
    return stdev_calculation(*vec), vec


def ts_std(a, axis = 0, data = None, state = None):
    """
    ts_std(a) is equivalent to a.std()
    
    - supports numpy arrays 
    - handles nan
    - supports state management
    
    >>> # create sample data:
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999)); a[a>0] = np.nan

    :Example: pandas matching
    -----------------------------------
    >>> assert abs(ts_std(a) - a.std())<1e-13

    :Example: numpy 
    -----------------------------------
    >>> assert ts_std(a.values) == ts_std(a)

    :Example: state management
    -------------------------------------------
    >>> old = ts_std_(a.iloc[:2000])
    >>> new = ts_std(a.iloc[2000:], vec = old.vec)
    >>> assert new == ts_std(a)

    """
    state = state or {}
    return first_(_ts_std(a, axis = axis, **state))

def ts_std_(a, axis = 0, data = None, instate = None):
    """
    ts_std_(a) is equivalent to ts_std(a) except vec is also returned.
    See ts_std for full documentation    
    """
    state = instate or {}
    return _zip(_ts_std(a, axis = axis, **state))

@loop_all
def _ts_rms(a, vec = None):
    vec = _vec(vec,3,0.)
    vec = _moments(a, vec)
    return np.sqrt(vec[2]/vec[0]), vec
    

def ts_rms(a, axis = 0, data = None, state = None):
    """
    ts_rms(a) is equivalent to (a**2).mean()**0.5
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
        
    axis : int, optional
        0/1/-1. The default is 0.

    data: None
        unused at the moment. Allow code such as func(live, **func_(history)) to work

    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 

    - supports numpy arrays 
    - handles nan
    - supports state management
    
    >>> # create sample data:
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999)); a[a>0] = np.nan

    :Example: pandas matching
    -----------------------------------
    >>> assert abs(ts_std(a) - a.std())<1e-13

    :Example: numpy 
    -----------------------------------
    >>> assert ts_std(a.values) == ts_std(a)

    :Example: state management
    -------------------------------------------
    >>> old = ts_rms_(a.iloc[:2000])
    >>> new = ts_rms(a.iloc[2000:], vec = old.vec)
    >>> assert new == ts_rms(a)

    """
    state = state or {}
    return first_(_ts_rms(a, axis = axis, **state))

def ts_rms_(a, axis = 0, data = None, instate = None):
    """
    ts_rms_(a) is equivalent to ts_rms(a) except it also returns vec
    see ts_rms for full documentation
    """
    state = instate or {}
    return _zip(_ts_rms(a, axis = axis, **state))


@loop_all
def _ts_skew(a, bias = False, min_sample = 0.25, vec = None):
    vec = _vec(vec,4,0.)
    vec = _moments(a, vec)
    return skew_calculation(t0 = vec[0], t1 = vec[1], t2 = vec[2], t3 = vec[3], bias = bias, min_sample = min_sample), vec

def ts_skew(a, bias = False, min_sample = 0.25, axis = 0, data = None, state = None):
    """
    ts_skew(a,0) is equivalent to a.skew()
    
    - supports numpy arrays 
    - handles nan
    - faster than pandas
    - supports state management
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
        
    axis : int, optional
        0/1/-1. The default is 0.
        
    min_sample: float, optional
        This refers to the denominator when we calculate the skew. Over time, the deonimator converges to 1 but initially, it is small. 
        Also, if there is a gap in the data, older datapoints weight may have decayed while there are not enough "new point". 
        min_sample ensures that in both cases, if denominator<0.25 )(default value) we return nan.

    data: None
        unused at the moment. Allow code such as func(live, **func_(history)) to work

    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 

    :Example: pandas matching
    -----------------------------------
    >>> # create sample data:
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999)); a[a>0] = np.nan
    >>> assert abs(ts_skew(a, 0) - a.skew())<1e-13

    :Example: numpy 
    -----------------------------------
    >>> assert ts_skew(a.values) == ts_skew(a)

    :Example: state management
    -------------------------------------------
    >>> old = ts_skew_(a.iloc[:2000])
    >>> new = ts_skew(a.iloc[2000:], vec = old.vec)
    >>> assert new == ts_skew(a)
    """
    state = state or {}
    return first_(_ts_skew(a, bias = bias, min_sample = min_sample, axis = axis, **state))
    
def ts_skew_(a, bias = False, min_sample = 0.25, axis = 0, data = None, instate = None):
    """
    ts_skew_(a) is equivalent to ts_skew except vec is also returned.
    See ts_skew for full details
    """
    state = instate or {}
    return _zip(_ts_skew(a, bias = bias, min_sample = min_sample, axis = axis, **state))

@loop(dict, list, tuple)
def _nona(df, value = np.nan):
    if np.isnan(value):
        mask = np.isnan(df)
    elif np.isinf(value):
        mask = np.isinf(df)
    else:
        mask = df == value
    if len(mask.shape) == 2:
        mask = mask.min(axis=1) == 1
    return df[~mask]

def nona(a, value = np.nan):
    """
    removes rows that are entirely nan (or a specific other value)

    :Parameters:
    ----------------
    a : dataframe/ndarray
        
    value : float, optional
        value to be removed. The default is np.nan.
        
    :Example:
    ----------
    >>> from pyg import *
    >>> a = np.array([1,np.nan,2,3])
    >>> assert eq(nona(a), np.array([1,2,3]))

    :Example: multiple columns
    ---------------------------
    >>> a = np.array([[1,np.nan,2,np.nan], [np.nan, np.nan, np.nan, 3]]).T 
    >>> b = np.array([[1,2,np.nan], [np.nan, np.nan, 3]]).T ## 2nd row has nans across
    >>> assert eq(nona(a), b)


    """
    return _nona(a)


ts_min_.output = ['data', 'state']
ts_max_.output = ['data', 'state']
ts_count_.output = ['data', 'state']
ts_sum_.output = ['data', 'state']
ts_mean_.output = ['data', 'state']
ts_rms_.output = ['data', 'state']
ts_std_.output = ['data', 'state']
ts_skew_.output = ['data', 'state']


