from pyg.base import getargspec, first, Dict, loop, zipper, as_list
from numba import njit

__all__ = ['compiled']

def compiled(function):
    res = njit(nogil = True)(function)
    res.fullargspec = getargspec(function)
    return res

#from pyg.base import passthru; compiled = passthru ## use this to get valid code-coverage for compiled functions

first_ = loop(dict, list)(first)


def _data_state(keys, values, output = 'data'):
    if isinstance(values, dict):
        return type(values)({k: _data_state(keys, v, output) for k, v in values.items()})
    elif isinstance(values, list):
        return type(values)([_data_state(keys, v, output) for v in values])
    output = as_list(output)
    assert keys[:len(output)] == output
    res = Dict(zip(output,values))
    if len(keys) > len(output):
        res['state'] = dict(zipper(keys[len(output):], values[len(output):]))
    return res


def _ignore(ignore, data, state):
    state = {} if state is None else state
    if ignore is True:
        return None, {}
    elif ignore is False:
        return data, state
    ignore = as_list(ignore)
    if 'data' in ignore:
        data = None
    if 'state' in ignore:
        state = {}
    return data, state

# import pandas as pd
# from pyg.base import wrapper, is_ts, is_dict, is_tuple, getargspec, loop, first, Dict, zipper

# class persist_data(wrapper):
#     """
#     We work with state-persisting function that return a variable called 'data'
    
#     :Example:
#     -------
#     >>> from pyg import *
#     >>> ts = pd.Series(np.random.normal(0,1,(1000)), drange(-999))
#     >>> ts[ts<0.1] = np.nan

#     We can run ts in one go:
        
#     >>> both = ffill_(ts)

#     Or split it into two parts:

#     >>> old = ffill_(ts.iloc[:500])
    
#     old.data is the data for first 500 entries. We can use this data to speed up later calculations...
    
#     >>> new = ffill_(ts.iloc[500:], **(old-'data')) 
    
#     We note: 
#     1) the 'data' variable must be remove
#     2) We need to glue together old.data and new.data
    
#     However... persist_data does the work for you:
        
#     >>> persist_ffill_ = persist_data(ffill_)
#     >>> glued_with_old = persist_ffill_(ts, **old)  # Note that we pass on data and the full ts, but calculation is done only on the 'new' bit of ts
#     >>> glued_just_new = persist_ffill_(ts.iloc[500:], **old)  

#     >>> assert eq(glued_with_old, both)    
#     >>> assert eq(glued_just_new, both)    
    
#     """
#     def wrapped(self, *args, **kwargs):
#         data = kwargs.pop('data', None)
#         if data is not None and len(data)>0 and is_ts(data):
#             cutoff = data.index[-1]
#             args_ = [v[v.index>cutoff] if is_ts(v) and len(v) else v for v in args]
#             kwargs_ = {k : v[v.index>cutoff] if is_ts(v) and len(v) else v for k, v in kwargs.items()}
#             res = self.function(*args_, **kwargs_)
#             if res is None:
#                 return data
#             if is_ts(res):
#                 new = res
#             elif is_dict(res):
#                 new = res.get('data')
#             elif is_tuple(res):
#                 new = res[0]
#             else:
#                 raise ValueError('result of persistence data must be a timeseries or a dict \n%s '%res)
#             if new is None:
#                 both = data
#             elif is_ts(new):
#                 both = data if len(new) == 0 else pd.concat([data[data.index<new.index[0]], new])
#             else:
#                 raise ValueError('data must be None or a timeseries')                
#             if is_ts(res):
#                 return both
#             elif is_dict(res):
#                 res['data'] = both
#                 return res
#             elif is_tuple(res):
#                 return (both,) + res[1:]
#         else:
#             return self.function(*args, **kwargs)

