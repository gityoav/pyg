# from pyg.base._loop import loop
# from pyg.base._types import is_df
# from pyg.base._dict import Dict
# from copy import copy 

# __all__ = ['df_update', 'getitem']

# @loop(list, tuple)
# def _getitem(df, item):
#     if is_df(df):
#         res = Dict({df.index.name or 'index' : df.index})
#         res.update({key: df[key] for key in df.columns})
#         return res[item]
#     elif isinstance(df, Dict):
#         return df[item]
#     else:
#         return Dict(df)[item]


# def getitem(df, item):
#     """
#     :Example:
#     -------
#     >>> from pyg import *
#     >>> df = pd.DataFrame(dict(a=[1,2], b=[2,4]))
#     >>> assert eq(getitem(df, 'a'), df.a)
#     >>> assert eq(getitem(df, ['a']), Dict(a = df.a))
#     >>> assert eq(getitem(df, ['a','b']), Dict(a = df.a, b=df.b))
#     >>> assert eq(getitem(df, lambda a, b: a+b), pd.Series([3,6]))
#     >>> assert eq(getitem(dictable(df), lambda a, b: a+b), [3,6])
#     >>> assert eq(getitem(dict(a=1,b=2), lambda a, b: a+b), 3)
    

#     :Parameters:
#     ----------------
#     df : dict, pd.Series, pd.DataFrame or dictable
#         stuff we want to calculate items for
#     item : callable or item
#         a key, or a function of existing keys.


#     """
#     return _getitem(df, item)

# @loop(list, tuple)
# def _update(df, updates = None, **kwargs):
#     """
#     Extends dictable arithmetic of a function to DataFrames or Series or just a normal dict

#     :Example:
#     -------
#     >>> from pyg import *
#     >>> df = pd.DataFrame(dict(a=[1,2], b=[2,4]))
#     >>> res = update(df, c = lambda a, b: a+b)
#     >>> df['c'] = df.a + df.b    
#     >>> assert eq(df, res)
    
    
#     >>> df = pd.DataFrame(dict(a = [5,6], d = [7,8]), index = drange(1))
#     >>> kwargs = dict(b = 0); updates = None
#     >>> update(df, b = 0)

#     >>> assert call(dict(a=1,b=2), c = lambda a, b: a+b) == dict(a=1,b=2,c=3)


#     :Parameters:
#     ----------------
#     df : TYPE
#         DESCRIPTION.
#     **kwargs : TYPE
#         DESCRIPTION.

#     :Returns:
#     -------
#     TYPE
#         DESCRIPTION.

#     """
#     if updates is None:
#         updates = {}
#     if not isinstance(updates, dict):
#         updates = dict(updates = updates)
#     updates.update(kwargs)
#     if len(updates) ==0:
#         return df
#     if isinstance(df, Dict):
#         return df(**updates)
#     elif is_df(df):
#         res = copy(df)
#         d = Dict({df.index.name or 'index' : df.index.values})
#         d.update({key: df[key].values for key in df.columns})
#         d = d(**updates)
#         for key, value in updates.items():
#             res[key] = d[key]
#         return res
#     else:
#         res = copy(df)
#         d = Dict(df)(**updates)        
#         for key in updates:
#             res[key] = d[key]
#         return res                

# def df_update(df, updates = None, **kwargs):
#     """
#     Extends dictable arithmetic of a function to DataFrames or Series or just a normal dict

#     :Example:
#     -------
#     >>> from pyg import *
#     >>> df = pd.DataFrame(dict(a=[1,2], b=[2,4]))
#     >>> res = update(df, c = lambda a, b: a+b)
#     >>> df['c'] = df.a + df.b    
#     >>> assert eq(df, res)
    
    
#     >>> df = pd.DataFrame(dict(a = [5,6], d = [7,8]), index = drange(1))
#     >>> kwargs = dict(b = 0); updates = None
#     >>> update(df, b = 0)

#     >>> assert call(dict(a=1,b=2), c = lambda a, b: a+b) == dict(a=1,b=2,c=3)


#     :Parameters:
#     ----------------
#     df : TYPE
#         DESCRIPTION.
#     **kwargs : TYPE
#         DESCRIPTION.

#     :Returns:
#     -------
#     TYPE
#         DESCRIPTION.

#     """
#     return _update(df, updates, **kwargs)