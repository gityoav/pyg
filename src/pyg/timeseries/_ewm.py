import numpy as np; import pandas as pd
from pyg.timeseries._math import stdev_calculation_ewm, skew_calculation, cor_calculation_ewm, variance_calculation_ewm
from pyg.timeseries._decorators import compiled, first_, _data_state
from pyg.timeseries._index import presync
from pyg.base import pd2np, clock, loop_all, loop

__all__ = ['ewma', 'ewmstd', 'ewmvar', 'ewmskew', 'ewmrms',  'ewmcor', 'ewma_', 'ewmstd_', 'ewmskew_', 'ewmrms_', 'ewmcor_', 'ewmvar_',]

############################################
##
## resolving parameters
##
###########################################

@compiled
def _w(n):
    if n > 1:
        return n/(n+1)
    elif n < 1:
        return 1. * n
    else:
        raise ValueError('n can either be days>1 or fraction 1-1/(n+1) but nothing else')        
    

############################################
##
## compiled functions
##
###########################################

# import numba
# from numba import int32, float32    # import the types
# from numba.experimental import jitclass

# spec = [
#     ('t0',  float32),               # a simple scalar field
#     ('t1',  float32),               # a simple scalar field
#     ('t',   float32),               # a simple scalar field
#     ('a',   float32),               # a simple scalar field
#     ('w',   float32),               # a simple scalar field
# ]

# @jitclass(spec)
# class c_ewma(object):
#     def __init__(self, a, t, t0, t1, w):
#         self.a = a
#         self.t = t
#         self.t0 = t0
#         self.t1 = t1
#         self.w = w

#     def push(self, a, t):
#         if np.isnan(a):
#             return np.nan
#         if t == self.t:
#             self.t1 = self.t1 + (1-self.w) * (a - self.a)
#             return self.t1/self.t0
#         else:
#             p = self.w if np.isnan(t) else self.w**(t-self.t)
#             self.t0 = self.t0 * p + (1-self.w)
#             self.t1 = self.t1 * p + (1-self.w) * a
#             return self.t1/self.t0


# @compiled
# def _ewma_(ai, ti, ai0, ti0, t0, t1, w):
#     """
#     we receive 
    
#     - current values, (ai, ti)
#     - previous values (ai0, ti0)
#     - current state of the moments t0, t1
#     - parameters, w
    
#     We return:
#     result, current values, updated moments
#     res, a, t, t0, t1
#     """
#     if np.isnan(ai):
#         res = np.nan
#         return res, ai0, ti0, t0, t1
#     else:
#         if ti == ti0:
#             t1 = t1 + (1-w) * (ai - ai0)
#             res = t1/t0
#             return res, ai, ti, t0, t1 
#         else:
#             p = w**(ti-ti0)
#             t0 = t0 * p + (1-w)
#             t1 = t1 * p + (1-w) * ai
#             res = t1/t0
#             return res, ai, ti, t0, t1

@pd2np
@compiled
def _ewma(a, n, time, t = np.nan, t0 = 0, t1 = 0):
    w = _w(n)
    res = np.empty_like(a)
    i0 = 0
    for i in range(a.shape[0]):
        if np.isnan(a[i]):
            res[i] = np.nan
        else:
            if time[i] == t:
                t1 = t1 + (1-w) * (a[i] - a[i0])
            else:
                p = w if np.isnan(time[i]) else w**(time[i]-t)
                t0 = t0 * p + (1-w)
                t1 = t1 * p + (1-w) * a[i]
                t = time[i]
            i0 = i
            res[i] = np.nan if t0 == 0 else t1/t0
    return res, t, t0, t1


@pd2np
@compiled
def _ewmrms(a, n, time, t = np.nan, t0 = 0., t2 = 0.):
    w = _w(n)
    res = np.empty_like(a)
    i0 = 0
    for i in range(a.shape[0]):
        if np.isnan(a[i]):
            res[i] = np.nan
        else:
            if time[i] == t:
                t2 = t2 + (1-w) * (a[i]**2 - a[i0]**2)
            else:
                p = w if np.isnan(time[i]) else w**(time[i]-t)
                v = a[i]
                t0 = t0 * p + (1-w)
                t2 = t2 * p + (1-w) * v**2
                t = time[i]
            i0 = i
            res[i] = np.nan if t0 == 0 else np.sqrt(t2/t0)
    return res, t, t0, t2


@pd2np
@compiled
def _ewmstd(a, n, time, t = np.nan, t0 = 0, t1 = 0, t2 = 0, w2 = 0, min_sample = 0.25, bias = False, calculator = stdev_calculation_ewm):
    w = _w(n)
    res = np.empty_like(a)
    i0 = 0
    for i in range(a.shape[0]):
        if np.isnan(a[i]):
            res[i] = np.nan
        else:
            if time[i] == t:
                t1 = t1 + (1-w) * (a[i] - a[i0])
                t2 = t2 + (1-w) * (a[i]**2 - a[i0]**2)
            else:
                p = w if np.isnan(time[i]) else w**(time[i]-t)
                v = a[i]
                t0 = t0 * p + (1-w)
                w2 = w2 * p**2 + (1-w)**2
                t1 = t1 * p + (1-w) * v
                t2 = t2 * p + (1-w) * v**2
                t = time[i]
            i0 = i
            res[i] = calculator(t0, t1, t2, w2 = w2, min_sample = min_sample, bias = bias)
    return res, t, t0, t1, t2, w2
    

@pd2np
@compiled
def _ewmcor(a, b, ba, n, time, t = np.nan, t0 = 0, a1 = 0, a2 = 0, b1 = 0, b2 = 0, ab = 0, w2 = 0, min_sample = 0.25, bias = False):
    """
    _ewmcor(a, b, ba, n, time, t)
    n = 50
    t = np.nan; t0 = 0; a1 = 0; a2 = 0; b1 = 0; b2 = 0; ab = 0; w2 = 0; min_sample = 0.25; bias = False    
    data, t, t0, a1, a2, b1, b2, ab, w2 = _ewmcor(a, b, ba, 200, time, t = np.nan, t0 = 0, a1 = 0, a2 = 0, b1 = 0, b2 = 0, ab = 0, w2 = 0, min_sample = 0.25, bias = False)
    pd.Series(data, drange(-9999)).plot()

    """
    w = _w(n)
    res = np.empty_like(a)
    i0 = 0
    for i in range(a.shape[0]):
        if np.isnan(a[i]) or np.isnan(b[i]):
            res[i] = np.nan
        else:
            if time[i] == t:
                a1 = a1 + (1-w) * (a[i] - a[i0])
                a2 = a2 + (1-w) * (a[i]**2 - a[i0]**2)
                b1 = b1 + (1-w) * (b[i] - b[i0])
                b2 = b2 + (1-w) * (b[i]**2 - b[i0]**2)
                ab = ab + (1-w) * (ba[i] - ba[i0])
            else:
                p = w if np.isnan(time[i]) else w**(time[i]-t)
                t0 = t0 * p + (1-w)
                w2 = w2 * p**2 + (1-w)**2
                a1 = a1 * p + (1-w) * a[i]
                a2 = a2 * p + (1-w) * a[i]**2
                b1 = b1 * p + (1-w) * b[i]
                b2 = b2 * p + (1-w) * b[i]**2
                ab = ab * p + (1-w) * ba[i]
                t = time[i]
            i0 = i
            res[i0] = cor_calculation_ewm(t0 = t0, a1 = a1, a2 = a2, b1 = b1, b2 = b2, ab = ab, w2 = w2, min_sample = min_sample, bias = bias)
    return res, t, t0, a1, a2, b1, b2, ab, w2




@pd2np
@compiled
def _ewmskew(a, n, time, bias = 0, t = np.nan, t0 = 0, t1 = 0, t2 = 0, t3 = 0, min_sample = 0.25):
    w = _w(n)
    days = n if n>1 else w/(1-w)
    d = 1 + days
    res = np.empty_like(a)
    i0 = 0
    for i in range(a.shape[0]):
        if np.isnan(a[i]):
            res[i] = np.nan
        else:
            if time[i] == t:
                t1 = t1 + (1-w) * (a[i] - a[i0])
                t2 = t2 + (1-w) * (a[i]**2 - a[i0]**2)
                t3 = t3 + (1-w) * (a[i]**3 - a[i0]**3)
            else:
                p = w if np.isnan(time[i]) else w**(time[i]-t)
                v = a[i]
                t0 = t0 * p + (1-w)
                t1 = t1 * p + (1-w) * v
                t2 = t2 * p + (1-w) * v**2
                t3 = t3 * p + (1-w) * v**3
                t = time[i]
            i0 = i
            res[i] = skew_calculation(t0 * d, t1 * d, t2 * d, t3 * d, bias, min_sample)
    return res, t, t0, t1, t2, t3

##### clock management functions  ############################################

@loop_all
def _ewmat(a, n, time = None, t = None, t0 = 0, t1 = 0):
    time = clock(a, time, t)
    t = 0 if t is None or np.isnan(t) else t
    return _ewma(a = a, n = n, time = time, t = t, t0 = t0, t1 = t1)

@loop_all
def _ewmrmst(a, n, time = None, t = None, t0 = 0, t2 = 0):
    time = clock(a, time, t)
    t = 0 if t is None or np.isnan(t) else t
    return _ewmrms(a, n, time = time, t = t, t0 = t0, t2 = t2)

@loop_all
def _ewmstdt(a, n, time = None, t = None, t0 = 0, t1 = 0, t2 = 0, w2 = 0, min_sample = 0.25, bias = False, calculator = stdev_calculation_ewm):
    time = clock(a, time, t)
    t = 0 if t is None or np.isnan(t) else t
    return _ewmstd(a, n, time = time, t = t, t0 = t0, t1 = t1, t2 = t2, w2 = w2, min_sample=min_sample, bias = bias, calculator = calculator)

@loop_all
@presync
def _ewmcort(a, b, n, time = None, t = None, t0 = 0, a1 = 0, a2 = 0, b1 = 0, b2 = 0, ab = 0, w2 = 0, min_sample = 0.25, bias = False):
    ba = b * a
    time = clock(ba, time, t)
    t = 0 if t is None or np.isnan(t) else t
    return _ewmcor(a = a, b = b, ba = ba, n = n, time = time, t = t, t0 = t0, a1 = a1, a2 = a2, b1 = b1, b2 = b2, ab = ab, w2 = w2, min_sample=min_sample, bias = bias)

@loop_all
def _ewmskewt(a, n, time = None, t = None, bias = False, t0 = 0, t1 = 0, t2 = 0, t3 = 0, min_sample = 0.25):
    time = clock(a, time, t)
    t = 0 if t is None or np.isnan(t) else t
    return _ewmskew(a, n, time = time, bias = int(bias), t = t, t0 = t0, t1 = t1, t2 = t2, t3 = t3, min_sample=min_sample)    


#####   exposed API functions ############################################

def ewma_(a, n, time = None, data = None, instate = None):
    """
    Equivalent to ewma but returns a state parameter for instantiation of later calculations.
    See ewma documentation for more details
    """
    state = instate or {}
    return _data_state(['data', 't', 't0', 't1'], _ewmat(a = a, n = n, time = time, **state))

ewma_.output = ['data', 'state']


def ewma(a, n, time = None, axis = 0, data = None, state = None):
    """
    ewma is equivalent to a.ewm(n).mean() but with...
    - supports np.ndarrays as well as timeseries
    - handles nan by skipping them
    - allows state-management
    - ability to supply a 'clock' to the calculation
    
    :Parameters:
    ------------
    a : array/timeseries
    n : int/fraction
        The number or days (or a ratio) to scale the history
    time : Calendar, 'b/d/y/m' or a timeseries of time (use clock(a) to see output)
        If time parameter is provided, we allow multiple observations per unit of time. i.e., converging to the last observation in time unit. 
            - if we have intraday data, and set time = 'd', then 
            - the ewm calculation on last observations per day is what is retained. 
            - the ewm calculation on each intraday observation is same as an ewm(past EOD + current intraday observation)
    
    data: None.
        unused at the moment. Allow code such as func(live, **func_(history)) to work
    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 
    
    :Example: matching pandas
    -------------------------
    >>> import pandas as pd; import numpy as np; from pyg import *
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> ts = ewma(a,10); df = a.ewm(10).mean()
    >>> assert abs(ts-df).max()<1e-10

    :Example: numpy arrays support
    ------------------------------
    >>> assert eq(ewma(a.values, 10), ewma(a,10).values)

    :Example: nan handling
    ----------------------
    >>> a[a.values<0.1] = np.nan
    >>> ts = ewma(a,10, time = 'i'); df = a.ewm(10).mean() # note: pandas assumes, 'time' pass per index entry, even if value is nan
    >>> assert abs(ts-df).max()<1e-10
    
    >>> pd.concat([ts,df], axis=1)
    >>>                        0         1
    >>> 1993-09-24  0.263875  0.263875
    >>> 1993-09-25       NaN  0.263875
    >>> 1993-09-26       NaN  0.263875
    >>> 1993-09-27       NaN  0.263875
    >>> 1993-09-28       NaN  0.263875
    >>>                  ...       ...
    >>> 2021-02-04       NaN  0.786506
    >>> 2021-02-05  0.928817  0.928817
    >>> 2021-02-06       NaN  0.928817
    >>> 2021-02-07  0.839168  0.839168
    >>> 2021-02-08  0.831109  0.831109

    :Example: state management
    --------------------------
    >>> old = a.iloc[:5000]
    >>> new = a.iloc[5000:]
    >>> old_ts = ewma_(old, 10)
    >>> new_ts = ewma(new, 10, **old_ts) # instantiation with previous ewma
    >>> ts = ewma(a,10)
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: Support for time & clock
    ------------------------------------
    >>> daily = a
    >>> monthly = daily.resample('M').last()
    >>> m_ts = ewma(monthly, 3) ## 3-month ewma run on monthly data
    >>> d_ts = ewma(daily, 3, 'm') ## 3-month ewma run on daily data
    >>> daily_resampled_to_month = d_ts.resample('M').last()
    >>> assert abs(daily_resampled_to_month - m_ts).max() < 1e-10

    So you can run a 3-monthly ewma on daily, where within month, most recent value is used with the EOM history.
    
    :Example: Support for dict/list of arrays
    --------------------------------------
    >>> x = pd.Series(np.random.normal(0,1,1000), drange(-999)); y = pd.Series(np.random.normal(0,1,1000), drange(-999))
    >>> a = dict(x = x, y = y)    
    >>> assert eq(ewma(dict(x=x, y=y),10), dict(x=ewma(x,10), y=ewma(y,10)))
    >>> assert eq(ewma([x,y],10), [ewma(x,10), ewma(y,10)])
    
             
    :Returns:
    -------
    an array/timeseries of ewma
    
    """
    state = state or {}
    return first_(_ewmat(a=a, n=n, time = time, axis=axis, **state))


def ewmrms_(a, n, time = None, axis = 0, data = None, instate = None):
    """
    Equivalent to ewmrms but returns a state parameter for instantiation of later calculations.
    See ewmrms documentation for more details
    """
    state = instate or {}
    return _data_state(['data', 't', 't0', 't2'],_ewmrmst(a, n, time = time, axis=axis, **state))

ewmrms_.output = ['data', 'state']

def ewmrms(a, n, time=None, axis=0, data = None, state = None):
    """
    ewmrms is equivalent to (a**2).ewm(n).mean()**0.5  but with...
    - supports np.ndarrays as well as timeseries
    - handles nan by skipping them
    - allows state-management
    - ability to supply a 'clock' to the calculation
    
    :Parameters:
    ------------
    a : array/timeseries
    n : int/fraction
        The number or days (or a ratio) to scale the history
    time : Calendar, 'b/d/y/m' or a timeseries of time (use clock(a) to see output)
        If time parameter is provided, we allow multiple observations per unit of time. i.e., converging to the last observation in time unit. 
            - if we have intraday data, and set time = 'd', then 
            - the ewm calculation on last observations per day is what is retained. 
            - the ewm calculation on each intraday observation is same as an ewm(past EOD + current intraday observation)
    
    data: None.
        unused at the moment. Allow code such as func(live, **func_(history)) to work
    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 
    
    :Example: matching pandas
    -------------------------
    >>> import pandas as pd; import numpy as np; from pyg import *
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> ts = ewmrms(a,10); df = (a**2).ewm(10).mean()**0.5
    >>> assert abs(ts-df).max()<1e-10

    :Example: numpy arrays support
    ------------------------------
    >>> assert eq(ewmrms(a.values, 10), ewmrms(a,10).values)

    :Example: nan handling
    ----------------------
    >>> a[a.values<0.1] = np.nan
    >>> ts = ewmrms(a,10, time = 'i'); df = (a**2).ewm(10).mean()**0.5 # note: pandas assumes, 'time' pass per index entry, even if value is nan
    >>> assert abs(ts-df).max()<1e-10
    
    >>> pd.concat([ts,df], axis=1)
    >>>                        0         1
    >>> 1993-09-24  0.263875  0.263875
    >>> 1993-09-25       NaN  0.263875
    >>> 1993-09-26       NaN  0.263875
    >>> 1993-09-27       NaN  0.263875
    >>> 1993-09-28       NaN  0.263875
    >>>                  ...       ...
    >>> 2021-02-04       NaN  0.786506
    >>> 2021-02-05  0.928817  0.928817
    >>> 2021-02-06       NaN  0.928817
    >>> 2021-02-07  0.839168  0.839168
    >>> 2021-02-08  0.831109  0.831109
        
    :Example: state management
    --------------------------
    >>> old = a.iloc[:5000]
    >>> new = a.iloc[5000:]
    >>> old_ts = ewmrms_(old, 10)
    >>> new_ts = ewmrms(new, 10, **old_ts) # instantiation with previous ewma
    >>> ts = ewmrms(a,10)
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: Support for time & clock
    ------------------------------------
    >>> daily = a
    >>> monthly = daily.resample('M').last()
    >>> m_ts = ewmrms(monthly, 3) ## 3-month ewma run on monthly data
    >>> d_ts = ewmrms(daily, 3, 'm') ## 3-month ewma run on daily data
    >>> daily_resampled_to_month = d_ts.resample('M').last()
    >>> assert abs(daily_resampled_to_month - m_ts).max() < 1e-10

    So you can run a 3-monthly ewma on daily, where within month, most recent value is used with the EOM history.
    
    :Example: Support for dict/list of arrays
    --------------------------------------
    >>> x = pd.Series(np.random.normal(0,1,1000), drange(-999)); y = pd.Series(np.random.normal(0,1,1000), drange(-999))
    >>> a = dict(x = x, y = y)    
    >>> assert eq(ewmrms(dict(x=x, y=y),10), dict(x=ewmrms(x,10), y=ewmrms(y,10)))
    >>> assert eq(ewmrms([x,y],10), [ewmrms(x,10), ewmrms(y,10)])
    
             
    :Returns:
    -------
    an array/timeseries of ewma
    
    """
    state = state or {}
    return first_(_ewmrmst(a, n, time = time, axis=axis, **state))

def ewmstd_(a, n, time = None, min_sample=0.25, bias = False, axis=0, data = None, instate = None):
    """
    Equivalent to ewmstd but returns a state parameter for instantiation of later calculations.
    See ewmstd documentation for more details
    """
    state = instate or {}
    return _data_state(['data', 't', 't0', 't1', 't2', 'w2'],_ewmstdt(a, n, time = time, min_sample=min_sample, axis=axis, calculator = stdev_calculation_ewm, **state))

ewmstd_.output = ['data', 'state']

def ewmstd(a, n, time = None, min_sample=0.25, bias = False, axis=0, data = None, state = None):
    """
    ewmstd is equivalent to a.ewm(n).std() but with...
    - supports np.ndarrays as well as timeseries
    - handles nan by skipping them
    - allows state-management
    - ability to supply a 'clock' to the calculation
    
    :Parameters:
    ------------
    a : array/timeseries
    n : int/fraction
        The number or days (or a ratio) to scale the history
    time : Calendar, 'b/d/y/m' or a timeseries of time (use clock(a) to see output)
        If time parameter is provided, we allow multiple observations per unit of time. i.e., converging to the last observation in time unit. 
            - if we have intraday data, and set time = 'd', then 
            - the ewm calculation on last observations per day is what is retained. 
            - the ewm calculation on each intraday observation is same as an ewm(past EOD + current intraday observation)
    
    data: None.
        unused at the moment. Allow code such as func(live, **func_(history)) to work
    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 
    
    :Example: matching pandas
    -------------------------
    >>> import pandas as pd; import numpy as np; from pyg import *
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> ts = ewmstd(a,10); df = a.ewm(10).std()
    >>> assert abs(ts-df).max()<1e-10
    >>> ts = ewmstd(a,10, bias = True); df = a.ewm(10).std(bias = True)
    >>> assert abs(ts-df).max()<1e-10


    :Example: numpy arrays support
    ------------------------------
    >>> assert eq(ewmstd(a.values, 10), ewmstd(a,10).values)

    :Example: nan handling
    ----------------------
    >>> a[a.values<-0.1] = np.nan
    >>> ts = ewmstd(a,10, time = 'i'); df = a.ewm(10).std() # note: pandas assumes, 'time' pass per index entry, even if value is nan
    >>> assert abs(ts-df).max()<1e-10
    >>> ts = ewmstd(a,10, time = 'i', bias = True); df = a.ewm(10).std(bias = True) # note: pandas assumes, 'time' pass per index entry, even if value is nan
    >>> assert abs(ts-df).max()<1e-10
    
        
    :Example: state management
    --------------------------
    >>> old = a.iloc[:5000]
    >>> new = a.iloc[5000:]
    >>> old_ts = ewmstd_(old, 10)
    >>> new_ts = ewmstd(new, 10, **old_ts) # instantiation with previous ewma
    >>> ts = ewmstd(a,10)
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: Support for time & clock
    ------------------------------------
    >>> daily = a
    >>> monthly = daily.resample('M').last()
    >>> m_ts = ewmstd(monthly, 3) ## 3-month ewma run on monthly data
    >>> d_ts = ewmstd(daily, 3, 'm') ## 3-month ewma run on daily data
    >>> daily_resampled_to_month = d_ts.resample('M').last()
    >>> assert abs(daily_resampled_to_month - m_ts).max() < 1e-10

    So you can run a 3-monthly ewma on daily, where within month, most recent value is used with the EOM history.
    
    :Example: Support for dict/list of arrays
    --------------------------------------
    >>> x = pd.Series(np.random.normal(0,1,1000), drange(-999)); y = pd.Series(np.random.normal(0,1,1000), drange(-999))
    >>> a = dict(x = x, y = y)    
    >>> assert eq(ewmstd(dict(x=x, y=y),10), dict(x=ewmstd(x,10), y=ewmstd(y,10)))
    >>> assert eq(ewmstd([x,y],10), [ewmstd(x,10), ewmstd(y,10)])
    
             
    :Returns:
    -------
    an array/timeseries of ewma
    
    """
    state = state or {}
    return first_(_ewmstdt(a, n, time = time, min_sample=min_sample, bias = bias, axis=axis,  calculator = stdev_calculation_ewm, **state))


def ewmvar_(a, n, time = None, min_sample=0.25, bias = False, axis=0, data = None, instate = None):
    """
    Equivalent to ewmvar but returns a state parameter for instantiation of later calculations.
    See ewmvar documentation for more details
    """
    state = instate or {}
    return _data_state(['data', 't', 't0', 't1', 't2', 'w2'],_ewmstdt(a, n, time = time, min_sample=min_sample, axis=axis, calculator = variance_calculation_ewm, **state))

def ewmvar(a, n, time = None, min_sample=0.25, bias = False, axis=0, data = None, state = None):
    """
    ewmstd is equivalent to a.ewm(n).var() but with...
    - supports np.ndarrays as well as timeseries
    - handles nan by skipping them
    - allows state-management
    - ability to supply a 'clock' to the calculation
    
    :Parameters:
    ------------
    a : array/timeseries
    n : int/fraction
        The number or days (or a ratio) to scale the history
    time : Calendar, 'b/d/y/m' or a timeseries of time (use clock(a) to see output)
        If time parameter is provided, we allow multiple observations per unit of time. i.e., converging to the last observation in time unit. 
            - if we have intraday data, and set time = 'd', then 
            - the ewm calculation on last observations per day is what is retained. 
            - the ewm calculation on each intraday observation is same as an ewm(past EOD + current intraday observation)
    
    data: None.
        unused at the moment. Allow code such as func(live, **func_(history)) to work
    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 
    
    :Example: matching pandas
    -------------------------
    >>> import pandas as pd; import numpy as np; from pyg import *
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> ts = ewmvar(a,10); df = a.ewm(10).var()
    >>> assert abs(ts-df).max()<1e-10
    >>> ts = ewmvar(a,10, bias = True); df = a.ewm(10).var(bias = True)
    >>> assert abs(ts-df).max()<1e-10


    :Example: numpy arrays support
    ------------------------------
    >>> assert eq(ewmvar(a.values, 10), ewmvar(a,10).values)

    :Example: nan handling
    ----------------------
    >>> a[a.values<-0.1] = np.nan
    >>> ts = ewmvar(a,10, time = 'i'); df = a.ewm(10).var() # note: pandas assumes, 'time' pass per index entry, even if value is nan
    >>> assert abs(ts-df).max()<1e-10
    >>> ts = ewmvar(a,10, time = 'i', bias = True); df = a.ewm(10).var(bias = True) # note: pandas assumes, 'time' pass per index entry, even if value is nan
    >>> assert abs(ts-df).max()<1e-10
    
        
    :Example: state management
    --------------------------
    >>> old = a.iloc[:5000]
    >>> new = a.iloc[5000:]
    >>> old_ts = ewmvar_(old, 10)
    >>> new_ts = ewmvar(new, 10, **old_ts) # instantiation with previous ewma
    >>> ts = ewmvar(a,10)
    >>> assert eq(new_ts, ts.iloc[5000:])

    :Example: Support for time & clock
    ------------------------------------
    >>> daily = a
    >>> monthly = daily.resample('M').last()
    >>> m_ts = ewmvar(monthly, 3) ## 3-month ewma run on monthly data
    >>> d_ts = ewmvar(daily, 3, 'm') ## 3-month ewma run on daily data
    >>> daily_resampled_to_month = d_ts.resample('M').last()
    >>> assert abs(daily_resampled_to_month - m_ts).max() < 1e-10

    So you can run a 3-monthly ewma on daily, where within month, most recent value is used with the EOM history.
    
    :Example: Support for dict/list of arrays
    --------------------------------------
    >>> x = pd.Series(np.random.normal(0,1,1000), drange(-999)); y = pd.Series(np.random.normal(0,1,1000), drange(-999))
    >>> a = dict(x = x, y = y)    
    >>> assert eq(ewmvar(dict(x=x, y=y),10), dict(x=ewmvar(x,10), y=ewmvar(y,10)))
    >>> assert eq(ewmvar([x,y],10), [ewmvar(x,10), ewmvar(y,10)])
    
             
    :Returns:
    -------
    an array/timeseries of ewma
    
    """
    state = state or {}
    return first_(_ewmstdt(a, n, time = time, min_sample=min_sample, bias = bias, axis=axis, calculator = variance_calculation_ewm, **state))


ewmstd_.output = ['data', 'state']


def ewmcor_(a, b, n, time = None, min_sample = 0.25, bias = True, axis = 0, data = None, instate = None):
    """
    Equivalent to ewmcor but returns a state parameter for instantiation of later calculations.
    See ewmcor documentation for more details
    """
    state = instate or {}    
    return _data_state(['data', 't', 't0', 'a1', 'a2', 'b1', 'b2', 'ab', 'w2'], _ewmcort(a = a, b = b, n = n, time = time, min_sample=min_sample, bias = bias, axis=axis, **state))

def ewmcor(a, b, n, time = None, min_sample = 0.25, bias = True, axis = 0, data = None, state = None):
    """
    calculates pair-wise correlation between a and b.
    
    Parameters
    ----------
    a : array/timeseries
    b : array/timeseries
    n : int/fraction
        The number or days (or a ratio) to scale the history
    time : Calendar, 'b/d/y/m' or a timeseries of time (use clock(a) to see output)
        If time parameter is provided, we allow multiple observations per unit of time. i.e., converging to the last observation in time unit. 
            - if we have intraday data, and set time = 'd', then 
            - the ewm calculation on last observations per day is what is retained. 
            - the ewm calculation on each intraday observation is same as an ewm(past EOD + current intraday observation)
    min_sample : floar, optional
        minimum weight of observations before we return a reading. The default is 0.25. This ensures that we don't get silly numbers due to small population.
    bias : book, optional
        vol estimation for a and b should really by unbiased. Nevertheless, we track pandas and set bias = True as a default.
    axis : int, optional
        axis of calculation. The default is 0.
    data : place holder, ignore, optional
        ignore. The default is None.
    state : dict, optional
        Output from a previous run of ewmcor_. The default is None.
        
    :Example: matching pandas
    -------------------------
    >>> import pandas as pd; import numpy as np; from pyg import *
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> b = pd.Series(np.random.normal(0,1,9000), drange(-8999))
    >>> ts = ewmcor(a, b, n = 10); df = a.ewm(10).corr(b)
    >>> assert abs(ts-df).max()<1e-10

    :Example: numpy arrays support
    ------------------------------
    >>> assert eq(ewmcor(a.values, b.values, 10), ewmcor(a, b, 10).values)

    :Example: nan handling
    ----------------------
    >>> a[a.values<-0.1] = np.nan
    >>> ts = ewmcor(a, b, 10, time = 'i'); df = a.ewm(10).corr(b) # note: pandas assumes, 'time' pass per index entry, even if value is nan
    >>> assert abs(ts-df).max()<1e-10

    :Example: state management
    --------------------------
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> b = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> old_a = a.iloc[:5000]; old_b = b.iloc[:5000]
    >>> new_a = a.iloc[5000:]; new_b = b.iloc[5000:]
    >>> old_ts = ewmcor_(old_a, old_b, 10)
    >>> new_ts = ewmcor(new_a, new_b, 10, **old_ts) # instantiation with previous ewma
    >>> ts = ewmcor(a,b,10)
    >>> assert eq(new_ts, ts.iloc[5000:])
    
    """
    state = state or {}    
    return first_(_ewmcort(a, b = b, n = n, time = time, min_sample=min_sample, bias = bias, axis=axis, **state))

def ewmskew_(a, n, time = None, bias = False, min_sample = 0.25, axis=0, data = None, instate = None):
    """
    Equivalent to ewmskew but returns a state parameter for instantiation of later calculations.
    See ewmskew documentation for more details
    """
    state = instate or {}
    return _data_state(['data', 't', 't0', 't1', 't2', 't3'],_ewmskewt(a, n, time = time, bias = bias, min_sample = min_sample, axis = axis, **state))

ewmskew_.output = ['data', 'state']

def ewmskew(a, n, time = None, bias = False, min_sample = 0.25, axis=0, data = None, state = None):
    """
    Equivalent to a.ewm(n).skew() but with...
    - supports np.ndarrays as well as timeseries
    - handles nan by skipping them
    - allows state-management
    - ability to supply a 'clock' to the calculation
    
    :Parameters:
    ------------
    a : array/timeseries
    n : int/fraction
        The number or days (or a ratio) to scale the history
    time : Calendar, 'b/d/y/m' or a timeseries of time (use clock(a) to see output)
        If time parameter is provided, we allow multiple observations per unit of time. i.e., converging to the last observation in time unit. 
            - if we have intraday data, and set time = 'd', then 
            - the ewm calculation on last observations per day is what is retained. 
            - the ewm calculation on each intraday observation is same as an ewm(past EOD + current intraday observation)
    
    data: None.
        unused at the moment. Allow code such as func(live, **func_(history)) to work
    state: dict, optional
        state parameters used to instantiate the internal calculations, based on history prior to 'a' provided. 
    
    :Example: matching pandas
    -------------------------
    >>> import pandas as pd; import numpy as np; from pyg import *
    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> x = a.ewm(10).skew()

    >>> old = a.iloc[:10]
    >>> new = a.iloc[10:]
    f = ewmskew_
    for f in [ewma_, ewmstd_, ewmrms_, ewmskew_, ]:
        both = f(a, 3)
        o = f(old, 3)
        n = f(new, 3, **o)
        assert eq(o.data, both.data.iloc[:10]) 
        assert eq(n.data, both.data.iloc[10:]) 
        assert both - 'data' == n - 'data'

    >>> assert abs(a.ewm(10).mean() - ewma(a,10)).max() < 1e-14
    >>> assert abs(a.ewm(10).std() - ewmstd(a,10)).max() < 1e-14

    :Example: numpy arrays support
    -----------------------------------------------
    >>> assert eq(ewma(a.values, 10), ewma(a,10).values)

    :Example: nan handling
    ---------------------------------
    while panadas ffill values, timeseries skips nans:

    >>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999))
    >>> a[a.values>0.1] = np.nan
    >>> ts = ewma(a,10)
    >>> assert eq(ts[~np.isnan(ts)], ewma(a[~np.isnan(a)], 10))
    
    :Example: initiating the ewma with past state
    --------------------------------------------------------------------------
    >>> old = np.random.normal(0,1,100)
    >>> new = np.random.normal(0,1,100)
    >>> old_ = ewma_(old, 10)
    >>> new_ = ewma(new, 10, t0 = old_ewma.t0, t1 = old_ewma.t1) # instantiation with previous ewma
    >>> new_2 = ewma(np.concatenate([old,new]), 10)[-100:]
    >>> assert eq(new_ewma, new_ewma2)

    :Example: Support for time & clock
    --------------------------------------------------------------------------
    >>> daily = pd.Series(np.random.normal(0,1,10000), drange(-9999)).cumsum()
    >>> monthly = daily.resample('M').last()
    >>> m = ewma(monthly, 3) ## 3-month ewma run on monthly data
    >>> d = ewma(daily, 3, 'm') ## 3-month ewma run on daily data
    >>> daily_resampled_to_month = d.resample('M').last()
    >>> assert abs(daily_resampled_to_month - m).max() < 1e-10

    So you can run a 3-monthly ewma on daily, where within month, most recent value is used with the EOM history.

    
    :Returns:
    -------
    an array/timeseries of ewma
    
    """
    state = state or {}
    return first_(_ewmskewt(a, n, time = time, bias = bias, min_sample=min_sample, axis=axis, **state))

