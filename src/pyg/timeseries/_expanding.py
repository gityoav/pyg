import numpy as np
from pyg.timeseries._math import stdev_calculation, skew_calculation
from pyg.timeseries._decorators import compiled, first_, _data_state
from pyg.base import pd2np, loop_all

__all__ = ['cumsum', 'cumprod', ]
__all__ += ['expanding_mean', 'expanding_sum', 'expanding_rms', 'expanding_std', 'expanding_skew']

__all__ = ['cumsum_', 'cumprod_']
__all__ += ['expanding_mean_', 'expanding_sum_', 'expanding_rms_', 'expanding_std_', 'expanding_skew_']



@loop_all
@pd2np
@compiled
def _cumsum(a, t1 = 0):
    res = np.empty_like(a)
    for i in range(a.shape[0]):
        if np.isnan(a[i]):
            res[i] = np.nan
        else:
            t1 += a[i]
            res[i] = t1
    return res, t1

@loop_all
@pd2np
@compiled
def _cumprod(a, t1 = 1.):
    res = np.empty_like(a)
    for i in range(a.shape[0]):
        if np.isnan(a[i]):
            res[i] = np.nan
        else:
            t1 *= a[i]
            res[i] = t1
    return res, t1

@loop_all
@pd2np
@compiled
def _expanding_sum(a, t1 = 0.):
    res = np.empty_like(a)
    for j in range(a.shape[0]):
        if np.isnan(a[j]):
            res[j] = np.nan
        else:
            t1 += a[j]
            res[j] = t1
    return res, t1

@loop_all
@pd2np
@compiled
def _expanding_mean(a, t0 = 0., t1 = 0.):
    res = np.empty_like(a)
    for j in range(a.shape[0]):
        if np.isnan(a[j]):
            res[j] = np.nan
        else:
            t0 +=1.
            t1 += a[j]
            res[j] = t1/t0
    return res, t0, t1

@loop_all
@pd2np
@compiled
def _expanding_rms(a, t0 = 0., t2 = 0.):
    res = np.empty_like(a)
    for j in range(a.shape[0]):
        if np.isnan(a[j]):
            res[j] = np.nan
        else:
            t0 +=1.
            t2 += a[j]**2
            res[j] = np.sqrt(t2/t0)
    return res, t0, t2

@loop_all
@pd2np
@compiled
def _expanding_std(a, t0 = 0., t1 = 0., t2 = 0.):
    res = np.empty_like(a)
    for j in range(a.shape[0]):
        if np.isnan(a[j]):
            res[j] = np.nan
        else:
            t0 +=1.
            t1 += a[j]
            t2 += a[j]**2
            res[j] = stdev_calculation(t0 = t0, t1 = t1, t2 = t2)
    return res, t0, t1, t2

@loop_all
@pd2np
@compiled
def _expanding_skew(a, bias, t0 = 0., t1 = 0., t2 = 0., t3 = 0.):
    res = np.empty_like(a)
    for j in range(a.shape[0]):
        if np.isnan(a[j]):
            res[j] = np.nan
        else:
            t0 +=1
            t1 += a[j]
            t2 += a[j]**2
            t3 += a[j]**3
            res[j] = np.nan if t0<3 else skew_calculation(t0 = t0, t1 = t1, t2 = t2, t3 = t3, bias = bias, min_sample = 1)
    return res, t0, t1, t2, t3


############################################
##
## functions
##
###########################################



def cumprod(a, axis = 0, data = None, state = None):
    """
    equivalent to pandas np.exp(np.log(a).expanding().sum()).
    
    - works with np.arrays
    - handles nan without forward filling.
    - supports state parameters
    
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
        
    :Example: agreement with pandas
    --------------------------------
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = 1 + pd.Series(np.random.normal(0.001,0.05,10000), drange(-9999))
    >>> panda = np.exp(np.log(a).expanding().sum()); ts = cumprod(a)
    >>> assert abs(ts-panda).max() < 1e-10

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not forward fill the nans.    

    >>> a = 1 + pd.Series(np.random.normal(-0.01,0.05,100), drange(-99, 2020))
    >>> a[a<0.975] = np.nan
    >>> panda = np.exp(np.log(a).expanding().sum()); ts = cumprod(a)
    
    >>> pd.concat([panda,ts], axis=1)
    >>> 2019-09-24  1.037161  1.037161
    >>> 2019-09-25  1.050378  1.050378
    >>> 2019-09-26  1.158734  1.158734
    >>> 2019-09-27  1.158734       NaN
    >>> 2019-09-28  1.219402  1.219402
    >>>              ...       ...
    >>> 2019-12-28  4.032919  4.032919
    >>> 2019-12-29  4.032919       NaN
    >>> 2019-12-30  4.180120  4.180120
    >>> 2019-12-31  4.180120       NaN
    >>> 2020-01-01  4.244261  4.244261
    
    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.

    >>> old = a.iloc[:50]        
    >>> new = a.iloc[50:]    
    >>> ts = cumprod(a)
    >>> old_ts = cumprod_(old)
    >>> new_ts = cumprod(new, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[50:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(cumprod(dict(x = a, y = a**2)), dict(x = cumprod(a), y = cumprod(a**2)))
    >>> assert eq(cumprod([a,a**2]), [cumprod(a), cumprod(a**2)])

    """
    state = state or {}
    return first_(_cumprod(a, axis = axis, **state))

def cumprod_(a, axis = 0, data = None, instate = None):
    """
    Equivalent to cumprod(a) but returns also the state variable. 
    For full documentation, look at cumprod.__doc__
    """
    state = instate or {}
    return _data_state(['data','t1'], _cumprod(a, axis = axis, **state))

cumprod_.output = ['data', 'state']



############# expanding

def expanding_mean(a, axis = 0, data = None, state = None):
    """
    equivalent to pandas a.expanding().mean().
    
    - works with np.arrays
    - handles nan without forward filling.
    - supports state parameters
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
        
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
    >>> panda = a.expanding().mean(); ts = expanding_mean(a)
    >>> assert eq(ts,panda)    

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not forward fill the nans.    

    >>> a[a<0.1] = np.nan
    >>> panda = a.expanding().mean(); ts = expanding_mean(a)
    
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
    
    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.
    
    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = expanding_mean(a)
    >>> old_ts = expanding_mean_(old)
    >>> new_ts = expanding_mean(new, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(expanding_mean(dict(x = a, y = a**2)), dict(x = expanding_mean(a), y = expanding_mean(a**2)))
    >>> assert eq(expanding_mean([a,a**2]), [expanding_mean(a), expanding_mean(a**2)])

    """
    state = state or {}
    return first_(_expanding_mean(a, axis = axis, **state))

def expanding_rms(a, axis = 0, data = None, state = None):
    """
    equivalent to pandas (a**2).expanding().mean()**0.5).
    - works with np.arrays
    - handles nan without forward filling.
    - supports state parameters
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame, list/dict of these
        timeseries
        
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
    >>> panda = (a**2).expanding().mean()**0.5; ts = expanding_rms(a)
    >>> assert eq(ts,panda)    

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not forward fill the nans.    

    >>> a[a<0.1] = np.nan
    >>> panda = (a**2).expanding().mean()**0.5; ts = expanding_rms(a)
    
    >>> pd.concat([panda,ts], axis=1)
    >>>                    0         1
    >>> 1993-09-23  0.160462  0.160462
    >>> 1993-09-24  0.160462       NaN
    >>> 1993-09-25  0.160462       NaN
    >>> 1993-09-26  0.160462       NaN
    >>> 1993-09-27  0.160462       NaN
    >>>                  ...       ...
    >>> 2021-02-03  1.040346  1.040346
    >>> 2021-02-04  1.040346       NaN
    >>> 2021-02-05  1.040338  1.040338
    >>> 2021-02-06  1.040337  1.040337
    >>> 2021-02-07  1.040473  1.040473
        
    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.

    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = expanding_rms(a)
    >>> old_ts = expanding_rms_(old)
    >>> new_ts = expanding_rms(new, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])
    
    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(expanding_rms(dict(x = a, y = a**2)), dict(x = expanding_rms(a), y = expanding_rms(a**2)))
    >>> assert eq(expanding_rms([a,a**2]), [expanding_rms(a), expanding_rms(a**2)])

    """
    state = state or {}
    return first_(_expanding_rms(a, axis = axis, **state))

def expanding_sum(a, axis = 0, data = None, state = None):
    """
    equivalent to pandas a.expanding().sum().
    
    - works with np.arrays
    - handles nan without forward filling.
    - supports state parameters
    
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
        
    :Example: agreement with pandas
    --------------------------------
    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> panda = a.expanding().sum(); ts = expanding_sum(a)
    >>> assert eq(ts,panda)    

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not forward fill the nans.    

    >>> a[a<0.1] = np.nan
    >>> panda = a.expanding().sum(); ts = expanding_sum(a)
    
    >>> pd.concat([panda,ts], axis=1)
    >>>                    0         1
    >>> 1993-09-23          NaN          NaN
    >>> 1993-09-24          NaN          NaN
    >>> 1993-09-25     0.645944     0.645944
    >>> 1993-09-26     2.816321     2.816321
    >>> 1993-09-27     2.816321          NaN
    >>>                 ...          ...
    >>> 2021-02-03  3976.911348  3976.911348
    >>> 2021-02-04  3976.911348          NaN
    >>> 2021-02-05  3976.911348          NaN
    >>> 2021-02-06  3976.911348          NaN
    >>> 2021-02-07  3976.911348          NaN
    
    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.

    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = expanding_sum(a)
    >>> old_ts = expanding_sum_(old)
    >>> new_ts = expanding_sum(new, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(expanding_sum(dict(x = a, y = a**2)), dict(x = expanding_sum(a), y = expanding_sum(a**2)))
    >>> assert eq(expanding_sum([a,a**2]), [expanding_sum(a), expanding_sum(a**2)])

    """
    state = state or {}
    return first_(_expanding_sum(a, axis = axis, **state))

def expanding_std(a, axis = 0, data = None, state = None):
    """
    equivalent to pandas a.expanding().std().
    
    - works with np.arrays
    - handles nan without forward filling.
    - supports state parameters
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
        
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
    >>> panda = a.expanding().std(); ts = expanding_std(a)
    >>> assert abs(ts-panda).max()<1e-10   

    :Example: nan handling
    ----------------------
    Unlike pandas, timeseries does not forward fill the nans.    

    >>> a[a<0.1] = np.nan
    >>> panda = a.expanding().std(); ts = expanding_std(a)
    
    >>> pd.concat([panda,ts], axis=1)
    >>>                    0         1
    >>> 1993-09-23       NaN       NaN
    >>> 1993-09-24       NaN       NaN
    >>> 1993-09-25       NaN       NaN
    >>> 1993-09-26       NaN       NaN
    >>> 1993-09-27       NaN       NaN
    >>>              ...       ...
    >>> 2021-02-03  0.590448  0.590448
    >>> 2021-02-04  0.590448       NaN
    >>> 2021-02-05  0.590475  0.590475
    >>> 2021-02-06  0.590475       NaN
    >>> 2021-02-07  0.590411  0.590411

    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.

    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = expanding_std(a)
    >>> old_ts = expanding_std_(old)
    >>> new_ts = expanding_std(new, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(expanding_std(dict(x = a, y = a**2)), dict(x = expanding_std(a), y = expanding_std(a**2)))
    >>> assert eq(expanding_std([a,a**2]), [expanding_std(a), expanding_std(a**2)])

    """
    state = state or {}
    return first_(_expanding_std(a, axis = axis, **state))

def expanding_skew(a, bias = False, axis = 0, data = None, state = None):
    """
    equivalent to pandas a.expanding().skew() which doesn't exist
    
    - works with np.arrays
    - handles nan without forward filling.
    - supports state parameters
    
    :Parameters:
    ------------
    a : array, pd.Series, pd.DataFrame or list/dict of these
        timeseries
        
    axis : int, optional
        0/1/-1. The default is 0.
    
    data: None.
        unused at the moment. Allow code such as func(live, **func_(history)) to work
    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 
            
    :Example: state management
    --------------------------
    One can split the calculation and run old and new data separately.

    >>> from pyg import *; import pandas as pd; import numpy as np
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))

    >>> old = a.iloc[:5000]        
    >>> new = a.iloc[5000:]    
    >>> ts = expanding_skew(a)
    >>> old_ts = expanding_skew_(old)
    >>> new_ts = expanding_skew(new, **old_ts)    
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: dict/list inputs
    ---------------------------
    >>> assert eq(expanding_skew(dict(x = a, y = a**2)), dict(x = expanding_skew(a), y = expanding_skew(a**2)))
    >>> assert eq(expanding_skew([a,a**2]), [expanding_skew(a), expanding_skew(a**2)])
    """
    state = state or {}
    return first_(_expanding_skew(a, bias = bias, axis = axis, **state))


def expanding_mean_(a, axis = 0, data = None, instate = None):
    """
    Equivalent to expanding_mean(a) but returns also the state variables. 
    For full documentation, look at expanding_mean.__doc__
    """
    state = instate or {}
    return _data_state(['data','t0','t1'],_expanding_mean(a, axis = axis, **state))

expanding_mean_.output = ['data','state']

def expanding_rms_(a, axis = 0, data = None, instate = None):
    """
    Equivalent to expanding_rms(a) but returns also the state variables.
    For full documentation, look at expanding_rms.__doc__
    """
    state = instate or {}
    return _data_state(['data','t0','t2'],_expanding_rms(a, axis = axis, **state))

expanding_rms_.output = ['data','state']

def expanding_sum_(a, axis = 0, data = None, instate = None):
    """
    Equivalent to expanding_sum(a) but returns also the state variables.
    For full documentation, look at expanding_sum.__doc__
    """
    state = instate or {}
    return _data_state(['data','t1'], _expanding_sum(a, axis = axis, **state))

expanding_sum_.output = ['data','state']

def expanding_std_(a, axis = 0, data = None, instate = None):
    """
    Equivalent to expanding_mean(a) but returns also the state variables. 
    For full documentation, look at expanding_std.__doc__
    """
    state = instate or {}
    return _data_state(['data','t0', 't1', 't2'],_expanding_std(a, axis = axis, **state))

expanding_std_.output = ['data','state']

def expanding_skew_(a, bias = False, axis = 0, data = None, instate = None):
    """
    Equivalent to expanding_mean(a) but returns also the state variables. 
    For full documentation, look at expanding_skew.__doc__
    """    
    state = instate or {}
    return _data_state(['data','t0', 't1', 't2', 't3'],_expanding_skew(a, bias = bias, axis = axis, **state))

expanding_skew_.output = ['data','state']

cumsum = expanding_sum
cumsum_ = expanding_sum_

