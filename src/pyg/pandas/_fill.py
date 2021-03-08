# import numpy as np
# from pyg.base._as_list import as_list
# from pyg.base._types import is_pd, is_str, is_num, is_array
# from pyg.base._inspect import getargspec
# from pyg.base._loop import loop
# from numba import jit

# __all__ = ['fill']

# def _as_index(ts):
#     return ts.index if is_pd(ts) else ts
    
# def _as_method(method):
#     if is_str(method):
#         if method.lower().startswith('f'):
#             return 'ffill'
#         elif method.lower().startswith('b'):
#             return 'bfill'
#         else:
#             return method
#     else:
#         return method


# @loop(np.ndarray)
# @compiled
# def _ffill(a, limit = -1):
#     """
#     _ffill(np.array([np.nan, 1., np.nan])) 
#     """
#     res = a.copy()
#     prev = np.nan
#     n = limit
#     for i in range(a.shape[0]):
#         if np.isnan(a[i]):
#             if n!=0:
#                 n-=1
#                 res[i] = prev
#         else:
#             n = limit
#             prev = res[i]
#     return res

# @loop(np.ndarray)
# @compiled
# def _bfill(a, limit = -1):
#     """
#     _bfill(np.array([np.nan, 1., np.nan])) 
#     """
#     res = a.copy()
#     prev = np.nan
#     n = limit
#     for i in range(a.shape[0]-1, -1, -1):
#         if np.isnan(a[i]):
#             if n!=0:
#                 n-=1
#                 res[i] = prev
#         else:
#             n = limit
#             prev = res[i]
#     return res


# @loop(list, tuple, dict)
# def _fill(value, method = None, limit = None, **kwargs):
#     if method is None:
#         return value
#     if is_pd(value):
#         method = as_list(method)
#         res = value.copy()
#         for m in method:
#             m = _as_method(m)
#             if is_num(m):
#                 return res.fillna(value = m)
#                 return res
#             else:
#                 res = res.fillna(method = m, limit = limit, **kwargs)
#         return res
#     elif is_array(value) and len(value.shape):
#         method = as_list(method)
#         res = value.copy()
#         for m in method:
#             m = _as_method(m)
#             limit = -1 if limit is None else limit
#             if is_num(m):
#                 res[np.isnan(res)] = m
#                 return res
#             elif m == 'ffill':
#                 res = _ffill(res, limit = limit)
#             elif m == 'bfill':
#                 res = _bfill(res, limit = limit)
#             else:
#                 raise ValueError('unsupported fill method "%s"'%m)
#         return res
#     else:
#         return value
              
# def fill(value, method = None, limit = None, **kwargs):
#     return _fill(value, method = method, limit = limit, **kwargs)
