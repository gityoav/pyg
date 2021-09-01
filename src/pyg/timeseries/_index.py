"""
We want to simplify the operations for pandas dataframes assuming we are using timeseries as the main objects.

When we have multiple timeseries, we will:
    
    1) calculate joint index using df_index()
    2) reindex each timeseries to the joint index
    
We then need to worry about multiple columns if there are. If none, each timeseries will be considered as pd.Series

If there are multiple columns, we will perform the calculations columns by columns. 

"""
from pyg.base._types import is_df, is_str, is_num, is_tss, is_int, is_arr, is_pd, is_ts, is_arrs, is_tuples
from pyg.base import zipper, wrapper, loop, as_list, reducing
from pyg.timeseries._ts import _nona

import pandas as pd
import numpy as np
from copy import copy
import inspect


__all__ = ['df_fillna', 'df_index', 'df_reindex', 'df_columns', 'presync', 'np_reindex']

def _list(values):
    """
    >>> assert _list([1,2,[3,4,5,[6,7]],dict(a =[8,9], b=[10,[11,12]])])  == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]  
    >>> assert _list(1)  == [1]  
    >>> assert _list(dict(a=1, b=2))  == [1,2]  

    """
    if isinstance(values, list):
        return sum([_list(df) for df in values], [])
    elif isinstance(values, dict):
        return _list(list(values.values()))
    else:
        return [values]


@loop(list, tuple, dict)
def _index(ts):
    if isinstance(ts, pd.Index):
        return ts
    elif is_pd(ts):
        return ts.index
    elif is_arr(ts):
        return len(ts)
    else:
        raise ValueError('did not provide an index')
    

def _df_index(indexes, index):
    if len(indexes) > 0:
        if is_str(index):
            if index[0].lower() == 'i':#nner
                return reducing('intersection')(indexes)        
            elif index[0].lower() == 'o':#uter
                return reducing('union')(indexes)        
            elif index[0].lower() == 'l':#uter
                return indexes[0]
            elif index[0].lower() == 'r':#uter
                return indexes[-1]
        else:
            return _index(index)
    else:
        return None


def _np_index(indexes, index):
    if len(indexes) > 0:
        if index[0].lower() == 'i':#nner
            return min(indexes)        
        elif index[0].lower() == 'o':#uter
            return max(indexes)        
        elif index[0].lower() == 'l':#uter
            return indexes[0]
        elif index[0].lower() == 'r':#uter
            return indexes[-1]
    else:
        return None


def df_index(seq, index = 'inner'):
    """
    Determines a joint index of multiple timeseries objects.

    :Parameters:
    ----------------
    seq : sequence whose index needs to be determined
        a (possible nested) sequence of timeseries/non-timeseries object within lists/dicts
    index : str, optional
        method to determine the index. The default is 'inner'.

    :Returns:
    -------
    pd.Index
        The joint index.
        
    :Example:
    ---------
    >>> tss = [pd.Series(np.random.normal(0,1,10), drange(-i, 9-i)) for i in range(5)]
    >>> more_tss_as_dict = dict(zip('abcde',[pd.Series(np.random.normal(0,1,10), drange(-i, 9-i)) for i in range(5)]))
    >>> res = df_index(tss + [more_tss_as_dict], 'inner')
    >>> assert len(res) == 6
    >>> res = df_index(more_tss_as_dict, 'outer')
    >>> assert len(res) == 14
    """
    listed = _list(seq)
    indexes = [ts.index for ts in listed if is_pd(ts)]
    if len(indexes):
        return _df_index(indexes, index)
    arrs = [len(ts) for ts in listed if is_arr(ts)]
    if len(arrs):
        return _np_index(arrs, index)
    else:
        return None
    

def df_columns(seq, index = 'inner'):
    listed = _list(seq)
    indexes= [ts.columns for ts in listed if is_df(ts) and ts.shape[1]>1 and len(set(ts.columns)) == ts.shape[1]] #dataframe with non-unique columns are treated like arrays
    if len(indexes):
        return _df_index(indexes, index)
    arrs = [ts.shape[1] for ts in listed if (is_arr(ts) or is_df(ts)) and len(ts.shape)>1 and ts.shape[1]>1]
    if len(arrs):
        return _np_index(arrs, index)
    return None

@loop(list, tuple, dict)
def _df_fillna(df, method = None, axis = 0, limit = None):
    methods = as_list(method)
    if len(methods) == 0:
        return df
    if is_arr(df):
        return df_fillna(pd.DataFrame(df) if len(df.shape)==2 else pd.Series(df), method, axis, limit).values
    res = df
    for m in methods:
        if is_num(m):
            res = res.fillna(value = m, axis = axis, limit = limit)
        elif m in ['backfill', 'bfill', 'pad', 'ffill']:
            res = res.fillna(method = m, axis = axis, limit = limit)
        elif m in ('fnna', 'nona'):
            nonan = ~np.isnan(res)
            if len(res.shape)==2:
                nonan = nonan.max(axis=1)
            if m == 'fnna':
                nonan = nonan[nonan.values]
                if len(nonan):
                    res = res[nonan.index[0]:]
                else:
                    res = res.iloc[:0]
            elif m == 'nona':
                res = res[nonan.values]
        else:
            if is_num(limit) and limit<0:
                res = res.interpolate(method = m, axis = axis, limit = abs(limit), 
                                      limit_direction = 'backward')
            else:
                res = res.interpolate(method = m, axis = axis, limit = limit)
    return res

def df_fillna(df, method = None, axis = 0, limit = None):
    """
    Equivelent to df.fillna() except:

    - support np.ndarray as well as dataframes
    - support multiple methods of filling/interpolation
    - supports removal of nan from the start/all of the timeseries
    - supports action on multiple timeseries
    
    :Parameters:
    ----------------
    df : dataframe/numpy array
        
    method : string, list of strings or None, optional
        Either a fill method (bfill, ffill, pad)
        Or an interplation method: 'linear', 'time', 'index', 'values', 'nearest', 'zero', 'slinear', 'quadratic', 'cubic', 'barycentric', 'krogh', 'spline', 'polynomial', 'from_derivatives', 'piecewise_polynomial', 'pchip', 'akima', 'cubicspline'
        Or 'fnna': removes all to the first non nan
        Or 'nona': removes all nans
    axis : int, optional
        axis. The default is 0.
    limit : TYPE, optional
        when filling, how many nan get filled. The default is None (indefinite)
    
    :Example: method ffill or bfill
    -----------------------------------------------
    >>> from pyg import *; import numpy as np
    >>> df = np.array([np.nan, 1., np.nan, 9, np.nan, 25])    
    >>> assert eq(df_fillna(df, 'ffill'), np.array([ np.nan, 1.,  1.,  9.,  9., 25.]))
    >>> assert eq(df_fillna(df, ['ffill','bfill']), np.array([ 1., 1.,  1.,  9.,  9., 25.]))
    >>> assert eq(df_fillna(df, ['ffill','bfill']), np.array([ 1., 1.,  1.,  9.,  9., 25.]))

    >>> df = np.array([np.nan, 1., np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 9, np.nan, 25])    
    >>> assert eq(df_fillna(df, 'ffill', limit = 2), np.array([np.nan,  1.,  1.,  1., np.nan, np.nan, np.nan, np.nan,  9.,  9., 25.]))

    df_fillna does not maintain state of latest 'prev' value: use ffill_ for that.

    :Example: interpolation methods
    -----------------------------------------------
    >>> from pyg import *; import numpy as np
    >>> df = np.array([np.nan, 1., np.nan, 9, np.nan, 25])    
    >>> assert eq(df_fillna(df, 'linear'), np.array([ np.nan, 1.,  5.,  9.,  17., 25.]))
    >>> assert eq(df_fillna(df, 'quadratic'), np.array([ np.nan, 1.,  4.,  9.,  16., 25.]))


    :Example: method = fnna and nona
    ---------------------------------------------
    >>> from pyg import *; import numpy as np
    >>> ts = np.array([np.nan] * 10 + [1.] * 10 + [np.nan])
    >>> assert eq(df_fillna(ts, 'fnna'), np.array([1.]*10 + [np.nan]))
    >>> assert eq(df_fillna(ts, 'nona'), np.array([1.]*10))

    >>> assert len(df_fillna(np.array([np.nan]), 'nona')) == 0
    >>> assert len(df_fillna(np.array([np.nan]), 'fnna')) == 0

    :Returns:
    -------
    array/dataframe with nans removed/filled

    """
    return _df_fillna(df, method = method, axis = axis, limit = limit)

@loop(list, tuple, dict)
def _df_reindex(ts, index, method = None, limit = None):
    methods = as_list(method)
    if is_pd(ts):
        if is_int(index):
            raise ValueError('trying to reindex dataframe %s using numpy interval length %i'%(ts, index))
        if len(methods) and methods[0] in ['backfill', 'bfill', 'pad', 'ffill']:
            res = _nona(ts).reindex(index, method = methods[0], limit = limit)
            res = _df_fillna(res, method = methods[1:], limit = limit)
        else:
            res = ts.reindex(index)
            res = _df_fillna(res, method = method, limit = limit)
        return res
    elif is_arr(ts):
        if isinstance(index, pd.Index):
            if len(index) == len(ts):
                return ts
            else:
                raise ValueError('trying to reindex numpy array %s using pandas index %s'%(ts, index))
        elif is_int(index):
            if index<len(ts):
                res = ts[-index:]
            elif index>len(ts):
                shape = (index - len(ts),) + ts.shape[1:]
                res = np.concatenate([np.full(shape, np.nan),ts])
            else:
                res = ts
            return df_fillna(res, method = methods, limit = limit)
        else:
            return ts
    else:
        return ts

def np_reindex(ts, index, columns = None):
    """
    pyg assumes that when working with numpy arrays representing timeseries, you:
        - determine a global timestamp
        - resample all timeseries to that one, and then covert to numpy.array, possibly truncating leading nan's.
        - do the maths you need to do
        - having worked with numpy arrays, if we want to reindex them back into dataframe, use np_reindex
    
    :Example:
    -------
    >>> from pyg import *
    >>> ts = np.array(np.random.normal(0,1,1000))
    >>> index = pd.Index(drange(-1999))
    >>> np_reindex(ts, index)

    :Parameters:
    ----------------
    ts : numpy array

    index : pandas.Index

    columns: list/array of columns names

    :Returns:
    ----------
    pd.DataFrame/pd.Series

    """
    if is_pd(index):
        index = index.index
    if len(index)>len(ts):
        index = index[-len(ts):]
    elif len(index)<len(ts):
        ts = ts[-len(index):]
    res = pd.Series(ts, index) if len(ts.shape)<2 else pd.DataFrame(ts, index)
    if columns is not None:
        if is_df(columns):
            columns = columns.columns
        res.columns = columns
    return res

def df_reindex(ts, index = None, method = None, limit = None):
    """
    A slightly more general version of df.reindex(index)

    :Parameters:
    ----------------
    ts : dataframe or numpy array (or list/dict of theses)
        timeseries to be reindexed
    index : str, timeseries, pd.Index.
        The new index
    method : str, list of str, float, optional
        various methods of handling nans are available. The default is None.
        See df_fillna for a full list.

    :Returns:
    -------
    timeseries/np.ndarray (or list/dict of theses)
        timeseries reindex.
        
    :Example: index = inner/outer
    -----------------------------
    >>> tss = [pd.Series(np.random.normal(0,1,10), drange(-i, 9-i)) for i in range(5)]
    >>> res = df_reindex(tss, 'inner')
    >>> assert len(res[0]) == 6
    >>> res = df_reindex(tss, 'outer')
    >>> assert len(res[0]) == 14

    :Example: index provided
    -----------------------------
    >>> tss = [pd.Series(np.random.normal(0,1,10), drange(-i, 9-i)) for i in range(5)]
    >>> res = df_reindex(tss, tss[0])
    >>> assert eq(res[0], tss[0])
    >>> res = df_reindex(tss, tss[0].index)
    >>> assert eq(res[0], tss[0])

    """
    if index is None:
        return ts
    elif is_str(index):
        index = df_index(ts, index)
    elif is_ts(index):
        index = index.index
    elif is_arr(index):
        index = pd.Index(index)
    return _df_reindex(ts, index = index, method = method, limit = limit)


def df_concat(objs, columns = None, axis=1, join = 'outer'):
    """
    simple concatenator, 
    - defaults to to concatenating by date (for timeseries)
    - supports columns renaming

    :Parameters:
    ----------
    objs : list/dict
        collection of timeseries
    columns : str/list
        Names of new columns. The default is None.
    axis : int, optional
        axis to merge. The default is 1.
    join : str, optional
        join method inner/outer, see pd.concat. The default is 'outer'.

    :Returns:
    -------
    res : pd.DataFrame
        joined dataframe
        
    :Example:
    ---------
    >>> objs = [pd.Series([1,2,3], [4,5,6]), pd.Series([3,4,5], [1,2,4])]
    >>> columns = ['a', 'b']; 
    >>> axis = 1; join = 'outer'
    >>> res = df_concat(objs, columns)

    >>> res
    >>>      a    b
    >>> 1  NaN  3.0
    >>> 2  NaN  4.0
    >>> 4  1.0  5.0
    >>> 5  2.0  NaN
    >>> 6  3.0  NaN    

    >>> df_concat(res, dict(a = 'x', b = 'y'))
    >>> res
    >>>      x    y
    >>> 1  NaN  3.0
    >>> 2  NaN  4.0
    >>> 4  1.0  5.0
    >>> 5  2.0  NaN
    >>> 6  3.0  NaN    

    """
    if isinstance(objs, dict):
        columns = list(objs.keys())
        objs = list(objs.values())
    if isinstance(objs, list):
        res = pd.concat(objs, axis = axis, join = join)
    elif isinstance(objs, pd.DataFrame):
        res = objs.copy() if columns is not None else objs
    if columns is not None:
        if isinstance(columns, list):
            res.columns = columns 
        else:
            res = res.rename(columns = columns)
    return res


@loop(list, dict, tuple)
def df_column(ts, column, i = None, n = None):
    if is_df(ts):
        if ts.shape[1] == 1:
            return ts[ts.columns[0]]
        elif column in ts.columns:
            return ts[column]
        elif column is None and i is not None:
            if len(set(ts.columns)) == ts.shape[1]: #unique columns, don't call me using i
                raise ValueError('trying to grab %ith column from a dataframe with proper columns: %s'%(i, ts.columns))
            elif n is not None and ts.shape[1]!=n:
                raise ValueError('trying to grab %ith column and asserting must have %i columns but have %i'%(i, n, ts.shape[1]))
            else:
                if i<ts.shape[1]:
                    return ts.iloc[:,i]
                else:
                    return np.nan
        else:
            return np.nan
    elif is_arr(ts) and len(ts.shape) == 2:
        if ts.shape[1] == 1:
            return ts.T[0]
        elif i is not None:
            if n is not None and ts.shape[1]!=n:
                raise ValueError('trying to grab %ith column and asserting must have %i columns but have %i'%(i, n, ts.shape[1]))
            elif i<ts.shape[1]:
                return ts.T[i]
            else:
                return np.nan
        else:
            return ts
    else:
        return ts


def _convert(res, columns):
    """
    We run a result per each column, now we want to convert it back to objects
    ----------
    res : dict
        results run per each column.
    """
    values = list(res.values())
    if is_tss(values):
        return pd.DataFrame(res)
    elif is_arrs(values) and is_int(columns):
        return np.array(values).T
    elif is_tuples(values):
        return tuple([_convert(dict(zip(res.keys(), row)), columns) for row in zipper(*values)])
    else:    
        return np.array(values) if is_int(columns) else pd.Series(res)



class presync(wrapper):
    """
    Much of timeseries analysis in Pandas is spent aligning multiple timeseries before feeding them into a function.
    presync allows easy presynching of all paramters of a function.
    
    :Parameters:
    ----------
    function : callable, optional
        function to be presynched. The default is None.
    index : str, optional
        index join policy. The default is 'inner'.
    method : str/int/list of these, optional
        method of nan handling. The default is None.
    columns : str, optional
        columns join policy. The default is 'inner'.
    default : float, optional
        value when no data is available. The default is np.nan.

    :Returns:
    -------
    presynch-decorated function

    
    :Example:
    -------    
    >>> from pyg import *
    >>> x = pd.Series([1,2,3,4], drange(-3))
    >>> y = pd.Series([1,2,3,4], drange(-4,-1))    
    >>> z = pd.DataFrame([[1,2],[3,4]], drange(-3,-2), ['a','b'])
    >>> addition = lambda a, b: a+b    

    #We get some nonsensical results:

    >>> assert list(addition(x,z).columns) ==  list(x.index) + ['a', 'b']
    
    #But:
        
    >>> assert list(presync(addition)(x,z).columns) == ['a', 'b']
    >>> res = presync(addition, index='outer', method = 'ffill')(x,z)
    >>> assert eq(res.a.values, np.array([2,5,6,7]))
    
    
    :Example 2: alignment works for parameters 'buried' within...
    -------------------------------------------------------
    >>> function = lambda a, b: a['x'] + a['y'] + b    
    >>> f = presync(function, 'outer', method = 'ffill')
    >>> res = f(dict(x = x, y = y), b = z)
    >>> assert eq(res, pd.DataFrame(dict(a = [np.nan, 4, 8, 10, 11], b = [np.nan, 5, 9, 11, 12]), index = drange(-4)))
    
    
    :Example 3: alignment of numpy arrays
    -------------------------------------
    >>> addition = lambda a, b: a+b
    >>> a = presync(addition)
    >>> assert eq(a(pd.Series([1,2,3,4], drange(-3)), np.array([[1,2,3,4]]).T),  pd.Series([2,4,6,8], drange(-3)))
    >>> assert eq(a(pd.Series([1,2,3,4], drange(-3)), np.array([1,2,3,4])),  pd.Series([2,4,6,8], drange(-3)))
    >>> assert eq(a(pd.Series([1,2,3,4], drange(-3)), np.array([[1,2,3,4],[5,6,7,8]]).T),  pd.DataFrame({0:[2,4,6,8], 1:[6,8,10,12]}, drange(-3)))
    >>> assert eq(a(np.array([1,2,3,4]), np.array([[1,2,3,4]]).T),  np.array([2,4,6,8]))


    :Example 4: inner join alignment of columns in dataframes by default
    ---------------------------------------------------------------------
    >>> x = pd.DataFrame({'a':[2,4,6,8], 'b':[6,8,10,12.]}, drange(-3))
    >>> y = pd.DataFrame({'wrong':[2,4,6,8], 'columns':[6,8,10,12]}, drange(-3))
    >>> assert len(a(x,y)) == 0    
    >>> y = pd.DataFrame({'a':[2,4,6,8], 'other':[6,8,10,12.]}, drange(-3))
    >>> assert eq(a(x,y),x[['a']]*2)
    >>> y = pd.DataFrame({'a':[2,4,6,8], 'b':[6,8,10,12.]}, drange(-3))
    >>> assert eq(a(x,y),x*2)
    >>> y = pd.DataFrame({'column name for a single column dataframe is ignored':[1,1,1,1]}, drange(-3)) 
    >>> assert eq(a(x,y),x+1)
    
    >>> a = presync(addition, columns = 'outer')
    >>> y = pd.DataFrame({'other':[2,4,6,8], 'a':[6,8,10,12]}, drange(-3))
    >>> assert sorted(a(x,y).columns) == ['a','b','other']    

    :Example 4: ffilling, bfilling
    ------------------------------
    >>> x = pd.Series([1.,np.nan,3.,4.], drange(-3))    
    >>> y = pd.Series([1.,np.nan,3.,4.], drange(-4,-1))    
    >>> assert eq(a(x,y), pd.Series([np.nan, np.nan,7], drange(-3,-1)))

    but, we provide easy conversion of internal parameters of presync:

    >>> assert eq(a.ffill(x,y), pd.Series([2,4,7], drange(-3,-1)))
    >>> assert eq(a.bfill(x,y), pd.Series([4,6,7], drange(-3,-1)))
    >>> assert eq(a.oj(x,y), pd.Series([np.nan, np.nan, np.nan, 7, np.nan], drange(-4)))
    >>> assert eq(a.oj.ffill(x,y), pd.Series([np.nan, 2, 4, 7, 8], drange(-4)))
    
    :Example 5: indexing to a specific index
    ----------------------------------------
    >>> index = pd.Index([dt(-3), dt(-1)])
    >>> a = presync(addition, index = index)
    >>> x = pd.Series([1.,np.nan,3.,4.], drange(-3))    
    >>> y = pd.Series([1.,np.nan,3.,4.], drange(-4,-1))    
    >>> assert eq(a(x,y), pd.Series([np.nan, 7], index))
    
    
    :Example 6: returning complicated stuff
    ----------------------------------------
    >>> from pyg import * 
    >>> a = pd.DataFrame(np.random.normal(0,1,(100,10)), drange(-99))
    >>> b = pd.DataFrame(np.random.normal(0,1,(100,10)), drange(-99))

    >>> def f(a, b):
    >>>     return (a*b, ts_sum(a), ts_sum(b))

    >>> old = f(a,b)    
    >>> self = presync(f)
    >>> args = (); kwargs = dict(a = a, b = b)
    >>> new = self(*args, **kwargs)
    >>> assert eq(new, old)
    """
    
    def __init__(self, function = None, index = 'inner', method = None, columns = 'inner', default = np.nan):
        super(presync, self).__init__(function = function, index = index, method = method, columns = columns , default = default)
    
    @property
    def ij(self):
        return copy(self) + dict(index = 'inner')

    @property
    def oj(self):
        return self + dict(index = 'outer')

    @property
    def lj(self):
        return self + dict(index = 'left')

    @property
    def rj(self):
        return self + dict(index = 'right')

    @property
    def ffill(self):
        return copy(self) + dict(method = 'ffill')

    @property
    def bfill(self):
        return self + dict(method = 'bfill')


    def wrapped(self, *args, **kwargs):
        values = list(args) + list(kwargs.values())
        listed = _list(values)
        tss = [ts for ts in listed if is_ts(ts)]
        callargs = inspect.getcallargs(self.function, *args, **kwargs)
        if is_str(self.index) and self.index in callargs:
            index = _index(callargs[self.index])
        else:
            index = df_index(listed, self.index)
        args_= df_reindex(args, index, method = self.method)
        kwargs_= df_reindex(kwargs, index, method = self.method)
        ### now we do the columns
        if self.columns is False:
            return self.function(*args_, **kwargs_)
        else:
            cols = [tuple(ts.columns) for ts in tss if is_df(ts) and ts.shape[1]>1]
            if len(set(cols))==1: # special case where all 2-d dataframes have same column headers
                columns = cols[0]
                n = len(columns)
                res = {column: self.function(*df_column(args_,column = column, i = i, n = n), **df_column(kwargs_, column=column, i = i, n = n)) for i, column in enumerate(columns)}
            else:
                columns = df_columns(listed, self.columns)
                if is_int(columns):
                    res = {i: self.function(*df_column(args_, column = None, i = i), **df_column(kwargs_, column=None, i = i)) for i in range(columns)}
                elif columns is None:
                    return self.function(*df_column(args_, column = None), **df_column(kwargs_, column = None))
                else:
                    columns = list(columns) if isinstance(columns, pd.Index) else as_list(columns)
                    columns = sorted(columns)
                    res = {column: self.function(*df_column(args_,column = column), **df_column(kwargs_, column=column)) for column in columns}                
            converted = _convert(res, columns)
            return converted 

@presync
def _add_(a, b):
    """
    addition of a and b supporting presynching (inner join) of timeseries
    """
    return a+b

@presync
def _mul_(a, b):
    """
    multiplication of a and b supporting presynching (inner join) of timeseries
    """
    return a*b

@presync
def _div_(a, b):
    """
    division of a by b supporting presynching (inner join) of timeseries
    """
    return a/b

@presync
def _sub_(a, b):
    """
    subtraction of b from a supporting presynching (inner join) of timeseries
    """
    return a-b

@presync
def _pow_(a, b):
    """
    equivalent to a**b supporting presynching (inner join) of timeseries
    """
    return a**b


def add_(a, b):
    """
    addition of a and b supporting presynching (inner join) of timeseries
    """
    return _add_(a,b)

def mul_(a, b):
    """
    multiplication of a and b supporting presynching (inner join) of timeseries
    """
    return _mul_(a,b)

def div_(a, b):
    """
    division of a by b supporting presynching (inner join) of timeseries
    """
    return _div_(a,b)

def sub_(a, b):
    """
    subtraction of b from a supporting presynching (inner join) of timeseries
    """
    return _sub_(a,b)

def pow_(a, b):
    """
    equivalent to a**b supporting presynching (inner join) of timeseries
    """
    return _pow_(a,b)
