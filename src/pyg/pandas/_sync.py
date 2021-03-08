# import pandas as pd; import numpy as np

# from pyg.base._types import is_ts, is_arr, is_pd, is_num, is_series, is_df
# from pyg.base._as_list import as_list
# from pyg.base._loop import loop
# from pyg.base._dict import dictattr
# from pyg.base._decorators import wrapper

# from pyg.pandas._bi import _np_reindex, _as_how
# from pyg.pandas._fill import fill
# from functools import reduce
# from operator import __and__, __or__
# from copy import copy

# __all__ = ['joint_index', 'joint_columns', 'sync', 'presync', 'reindex', 'recolumn']


# def _columns(value):
#     if is_series(value):
#         if is_ts(value):
#             return None
#         else:
#             return value.index
#     elif isinstance(value, dict):
#         return value.keys()
#     elif isinstance(value, pd.DataFrame):
#         return None if value.shape[1]<=1 else value.columns
#     else:
#         return None

# def joint_columns(dfs, how = 'ij'):
#     """
#     :Example:
#     -------
#     >>> dfs = [dict(a = 1, b=2), dict(a=1, c=2)]
#     >>> assert joint_columns(dfs) == ['a']
#     >>> assert joint_columns(dfs, 'ij') == ['a']
#     >>> assert sorted(joint_columns(dfs, 'oj')) == ['a', 'b', 'c']
#     """
#     cols = [_columns(df) for df in dfs]
#     cols = [c for c in cols if c is not None]
#     how = _as_how(how)
#     if len(cols) == 0:
#         return None
#     elif how == 'left':
#         return list(cols[0])
#     elif how == 'right':
#         return list(cols[-1])
#     elif len(cols) == 1 or len(set([tuple(c) for c in cols]))==1:
#         return list(cols[0])
#     scols = [set(c) for c in cols]
#     if how == 'inner':
#         return list(reduce(__and__, scols))
#     elif how == 'outer':
#         return list(reduce(__or__, scols))
#     else:
#         raise ValueError('%s not supported for columns'%how)



# def recolumn(df, columns, default = np.nan):
#     columns = as_list(columns)
#     mycols = _columns(df)
#     if mycols is None:
#         return df
#     res = copy(df)
#     for col in columns:
#         if col not in mycols:
#             res[col] = default
#     if is_pd(res) or isinstance(res, dictattr):
#         return res[columns]
#     elif isinstance(df, dict):
#         return type(df)({key : res[key] for key in columns})
#     else:
#         return df 

# @loop(list, dict, tuple)
# def _reindex(df, index, method = None):
#     if is_pd(index):
#         index = index.index
#     if is_num(index):
#         if is_arr(df):
#             return fill(_np_reindex(df, index), method = method)
#     else:
#         if is_df(df) or (is_series(df) and is_ts(df)):
#             return fill(df.reindex(index), method = method)
#     return df

# def reindex(df, index, method = None):
#     return _reindex(df, index = index, method = method)


# def _df_joint_index(tss, how):
#     if how == 'left':
#         return tss[0].index
#     elif how == 'right':
#         return tss[-1].index
#     elif how == 'equal':
#         assert len(set([len(ts) for ts in tss])) == 1, 'asserted all indices are equal but length mismatch %s'%set([len(ts) for ts in tss])
#         res = pd.concat(tss, axis=1, join = 'inner')
#         assert len(res) == len(tss[0]), 'asserted all indices are identical but only %s out of %s are common'%(len(res), len(tss[0]))
#     else:
#         return pd.concat(tss, axis=1, join = how).index

    
# def joint_index(dfs, how = 'ij'):
#     """
#     calculates the joint index for a collection of arrays/timeseries objects

#     :Example:
#     -------
#     >>> ts1 = pd.Series(range(10), drange(-9))
#     >>> ts2 = pd.Series(range(10), drange('-9b', 0, '1b'))
#     >>> ts3 = pd.DataFrame(dict(a=range(10), b = range(10,20)), drange('-14b', '-5b', '1b'))
#     >>> dfs = [ts1, ts2, ts3, 4]

#     :Parameters:
#     ----------------
#     dfs : list of dfs or arrays
#         DESCRIPTION.
#     how : str, optional
#         method for joining the dataframes/arrays. The default is inner join 'ij'.

#     Raises
#     ------
#     ValueError
#         DESCRIPTION.

#     :Returns:
#     -------
#     TYPE
#         DESCRIPTION.

#     """
#     how = _as_how(how)
#     dfs = as_list(dfs)
#     if len(dfs) == 1:
#         if is_pd(dfs[0]):
#             return dfs[0].index
#         elif is_arr(dfs[0]):
#             return len(dfs[0])
#         else:
#             return None
#     tss = [df for df in dfs if is_ts(df) and len(df)]
#     if len(tss) > 0:
#         return _df_joint_index(tss, how)
#     tss = [df for df in dfs if is_pd(df) and len(df)]
#     if len(tss) > 0:
#         return _df_joint_index(tss, how)
#     arrs = [len(df) for df in dfs if is_arr(df) and len(df.shape)]
#     if len(arrs) == 0:
#         return None
#     else:
#         if how == 'left':
#             return arrs[0]
#         elif how == 'right':
#             return arrs[-1]
#         elif how == 'inner':
#             return min(arrs)
#         elif how == 'outer':
#             return max(arrs)
#         elif how == 'equal':
#             n,x = min(arrs), max(arrs)
#             if n == x:
#                 return n
#             else:
#                 raise ValueError('join was specified as equal but provided lengths are unequal %s'%(set(arrs)))
#         else:
#             raise ValueError('inner/outer/equal only valid for arrays how %s'%how)


# def sync(dfs, how = 'ij', method = None, chow = 'ij', default = np.nan):
#     """
#     takes a list of dfs (or non dfs) and ensures they are aligned correctly

#     :Parameters:
#     ----------------
#     dfs : TYPE
#         DESCRIPTION.
#     how : TYPE, optional
#         DESCRIPTION. The default is 'ij'.
#     method : TYPE, optional
#         DESCRIPTION. The default is None.
#     chow : TYPE, optional
#         DESCRIPTION. The default is 'ij'.
#     default : TYPE, optional
#         DESCRIPTION. The default is np.nan.
    
#     :Example:
#     -------
#     >>> from pyg import *
#     >>> a = pd.Series(range(3), drange(2000,2))
#     >>> b = pd.Series(range(3), drange(2000,4,2))
#     >>> c = pd.Series(range(2), drange(2000,1))
#     >>> dfs = [a,b,c]
#     >>> how = 'ij'; method = None; chow = 'ij'; default = np.nan
#     >>> sync(a, 'oj', 3)
#     >>> df1 = pd.DataFrame(dict(a=a, b=a))
#     >>> df2 = pd.DataFrame(dict(a=a, c=a))
#     >>> s = pd.Series([1,2,3,4], list('abcd'))
#     >>> dfs = [df1, df2, s]


#     :Returns:
#     -------
#     TYPE
#         DESCRIPTION.

#     """
#     if isinstance(dfs, dict):
#         index = joint_index(dfs.values(), how = how)
#         res = {key : reindex(df, index, method = method) for key, df in dfs.items()}
#         columns = joint_columns(res.values(), chow)
#         return type(dfs)({key: recolumn(df, columns, default) for key, df in res.items()})
#     elif isinstance(dfs, (list, tuple)):
#         index = joint_index(dfs, how = how)
#         res = [reindex(df, index, method = method) for df in dfs]
#         columns = joint_columns(res, chow)
#         return type(dfs)([recolumn(df, columns, default) for df in res])
#     elif is_pd(dfs) or is_arr(dfs):
#         return fill(dfs, method = method)
#     else:
#         return dfs        
    

# class presync(wrapper):
#     """
#     allows presynching of paramters
    
#     :Example:
#     -------
    
#     @presync
#     def f(x,y):
#         return x+y
        
#     >>> x = pd.Series([1,2,3,4], drange(-3))
#     >>> y = pd.Series([1,2,3,4], drange(-4,-1))    

#     >>> assert eq(f(x,y), pd.Series([3,5,7], drange(-3,-1)))
    
#     @presync
#     def fsum(xs, initial_value = 0):
#         return sum(xs, initial_value)
    
#     >>> assert eq(fsum([x,y]), pd.Series([3,5,7], drange(-3,-1)))
    
#     """
    
#     def __init__(self, function = None, how = 'ij', method = None, chow = 'ij', default = np.nan):
#         super(presync, self).__init__(function = function, how = how, method = method, chow = chow, default = default)
#     def wrapped(self, *args, **kwargs):
#         values = list(args) + list(kwargs.values())
#         listed = sum([df if isinstance(df, list) else list(df.values()) if isinstance(df, dict) else [df] for df in values], [])
#         synced = sync(listed, how = self.how, method = self.method, chow = self.chow, default = self.default)
#         args_ = []
#         for arg in args:
#             if isinstance(arg, list):
#                 args_.append(synced[:len(arg)])
#                 synced = synced[len(arg):]
#             elif isinstance(arg, dict):
#                 args_.append(type(arg)(zip(arg, synced[:len(arg)])))
#                 synced = synced[len(arg):]
#             else:
#                 args_.append(synced[0])
#                 synced = synced[1:]
#         kwargs_ = {}
#         for key, arg in kwargs.items():
#             if isinstance(arg, list):
#                 kwargs_[key] = synced[:len(arg)]
#                 synced = synced[len(arg):]
#             elif isinstance(arg, dict):
#                 kwargs_[key] = type(arg)(zip(arg, synced[:len(arg)]))
#                 synced = synced[len(arg):]
#             else:
#                 kwargs_[key] = synced[0]
#                 synced = synced[1:]
#         return self.function(*args_, **kwargs_)




