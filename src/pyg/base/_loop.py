import pandas as pd; import numpy as np
from pyg.base._inspect import getargs
from pyg.base._types import is_ts, is_str, is_df, is_pd, is_series, is_array, is_tuple, is_dict
from pyg.base._decorators import wrapper
from pyg.base._as_list import as_tuple

__all__ = ['loop', 'loops', 'len0', 'pd2np', 'shape', 'loop_all']


def _zero():
    return 0

def len0(value):
    """
    returns the len of an object or 0 if no len(value) exists
    
    :Example:
    ---------
    >>> assert len0(5) == 0
    >>> assert len0('a string is dimensionless') == 0
    >>> assert len0([]) == 0
    >>> assert len0([1,2]) == 2

    Parameters
    ----------
    value : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    try:
        return 0 if is_str(value) else getattr(value, '__len__', _zero)()
    except Exception:
        return 0


def shape(value):
    return getattr(value, 'shape', ())
 

def _item_by_key(value, key, keys, i = None):
    if isinstance(value, dict) and sorted(value.keys()) == keys:
        return value[key] 
    elif isinstance(value, pd.Series) and sorted(value.index.values) == keys:
        return value[key] 
    elif isinstance(value, pd.DataFrame) and sorted(value.columns) == keys:
        return value[key]
    elif is_array(value) and len(value.shape):
        if len(value.shape) == 2 and value.shape[1] == len(keys) and i is not None:
            return value.T[i]
        elif len(value.shape) == 1 and value.shape[0] == len(keys) and i is not None:
            return value[i]
        else:
            return value
    else:
        if i is not None and len0(value) == len(keys):
            return value[i]
        else:
            return value

def _item_by_i(value, i, n):
    if is_df(value) and value.shape[1] == 1:
        value = value.iloc[:,0]
    elif is_array(value) and len(value.shape) == 2 and value.shape[1] == 1:
        value = value.T[0]
    if isinstance(value, (list, tuple)) and len(value) == n:
        return value[i]
    elif is_array(value):
        if len(value.shape) == 2 and value.shape[-1] == n:
            return value.T[i]
        if len(value.shape) == 1 and value.shape[-1] == n:
            return value[i]
    elif is_series(value) and len(value) == n and not is_ts(value):
        return value.iloc[i]
    elif is_df(value) and value.shape[1] == n:
        return value.iloc[:, i]
    return value           

# add_index_and_columns = _np2pd

def add_index_and_columns(res, arg):
    if isinstance(res, tuple):
        return tuple([add_index_and_columns(r, arg) for r in res])
    elif isinstance(res, dict):
        return type(res)({k: add_index_and_columns(r, arg) for k,r in res.items()})
    if isinstance(res, (pd.Series, pd.DataFrame)) and len(res) == len(arg):
        res.index = arg.index
    if isinstance(res, (pd.DataFrame)) and res.shape[1] == arg.shape[1]:
        res.columns = arg.columns
    return res

def axis0_to_dataframe(res, arg):
    if min([is_tuple(r) for r in res], default = False) and len(set([len(r) for r in res])) == 1:
        return tuple([axis0_to_dataframe(r, arg) for r in list(zip(*res))])
    elif min([is_dict(r) for r in res], default = False) and len(set([len(r) for r in res])) == 1:
        keys = res[0].keys()
        tp = type(res[0])
        return tp({key : axis0_to_dataframe([r[key] for r in res], arg) for key in keys})
    try:
        res = type(arg)(dict(zip(arg.columns, res)))
        return add_index_and_columns(res, arg)
    except ValueError:
        res = pd.Series(res)
        res.index = arg.columns
        return res

def axis0_to_array(res, arg):
    if min([is_tuple(r) for r in res], default = False) and len(set([len(r) for r in res])) == 1:
        return tuple([axis0_to_array(r, arg) for r in list(zip(*res))])
    elif min([is_dict(r) for r in res], default = False) and len(set([len(r) for r in res])) == 1:
        keys = res[0].keys()
        tp = type(res[0])
        return tp({key : axis0_to_array([r[key] for r in res], arg) for key in keys})
    rtn = np.array(res)
    if len(rtn.shape)>1:
        rtn = rtn.T
    return rtn

class loops(wrapper):

    """
    
    converts a function to loop over the arguments, depending on the type of the first argument

    :Examples:
    --------------
    >>> @loop(dict, list, pd.DataFrame, pd.Series)
    >>> def f(a,b):
    >>>     return a+b
    
    >>> assert f(1,2) == 3
    >>> assert f([1,2,3],2) == [3,4,5]
    >>> assert f([1,2,3], [4,5,6]) == [5,7,9]    
    
    >>> assert f(dict(x=1,y=2), 3) == dict(x = 4, y = 5)
    >>> assert f(dict(x=1,y=2), dict(x = 3, y = 4)) == dict(x = 4, y = 6)
    

    >>> a = pd.Series(dict(x=1,y=2))
    >>> b = dict(x=3,y=4)
    >>> assert np.all(f(a,b) == pd.Series(dict(x=4,y=6)))    

    >>> a = pd.DataFrame(dict(x=[1,1],y=[2,2])); a.index = [5,10]
    >>> b = dict(x=3,y=4)
    >>> res =  f(a,b)
    >>> assert np.all(res == pd.DataFrame(dict(x=[4,4],y=[6,6]), index = [5,10]))    
    
    >>> a = pd.DataFrame(dict(x=[1,1],y=[2,2])); a.index = [5,10]
    >>> res =  f(a,[3,4])
    >>> assert np.all( res == pd.DataFrame(dict(x=[4,4],y=[6,6]), index = [5,10]))    
    
    
    """

    def __init__(self, function = None, types = None):
        super(loops, self).__init__(function = function, types = as_tuple(types))

    @property
    def first(self):
        args = getargs(self)
        return args[0] if len(args) else None
    
    def T(self, arg, args, kwargs):
        arg_ = _T(arg)
        args_ = _T(args)
        kwargs_ = _T(kwargs)
        res = self._wrapped(arg_, args_, kwargs_)
        return _T(res)

    def wrapped(self, *args, **kwargs):
        top = self.first
        if len(args) == 0 and not top in kwargs:
            return self.function(*args, **kwargs)
        if len(args):
            arg, args_, kwargs_ = args[0], args[1:], kwargs
        else:
            arg = kwargs.pop(top)
            args_, kwargs_ = args, kwargs
        if isinstance(arg, pd.Series) and pd.Series in self.types and not is_ts(arg):
            keys = sorted(arg.index)
            res = {key : self._wrapped(arg[key], (_item_by_key(a,key,keys) for a in args_), {k : _item_by_key(v,key,keys) for k,v in kwargs_.items()}) for key in arg.index}
            return type(arg)(res)          
        else:
            return self._wrapped(arg, args_, kwargs_)
    
    def _wrapped(self, arg, args, kwargs):
        axis = kwargs.pop('axis', 0)
        if isinstance(arg, dict) and dict in self.types:
            keys = sorted(arg.keys())
            res = {key : self._wrapped(arg[key], (_item_by_key(a,key,keys) for a in args), {k : _item_by_key(v,key,keys) for k,v in kwargs.items()}) for key in arg.keys()}
            return type(arg)(res)
        elif isinstance(arg, pd.DataFrame) and pd.DataFrame in self.types:
            if axis in (1,-1):
                res = self.T(arg, args, kwargs)
                if isinstance(res, (pd.DataFrame, pd.Series)) and len(res) == len(arg):
                    res.index = arg.index
                return add_index_and_columns(res, arg)
            else:
                keys = sorted(arg.columns)
                res = [self._wrapped(arg[key], (_item_by_key(a,key,keys,i) for a in args), {k : _item_by_key(v,key,keys,i) for k,v in kwargs.items()}) for i, key in enumerate(arg.columns)]
                rtn = axis0_to_dataframe(res, arg)
                return rtn
        elif isinstance(arg, pd.Series):
            return self.function(arg, *args, **kwargs)
        elif isinstance(arg, np.ndarray) and np.ndarray in self.types:
            if len(arg.shape) <= 1:
                return self.function(arg, *args, **kwargs) 
            else:
                if axis in (1,-1):
                    return self.T(arg, args, kwargs)
                else:
                    n = arg.shape[-1]
                    res = [self._wrapped(_item_by_i(arg,i,n), (_item_by_i(a,i,n) for a in args), {k: _item_by_i(v,i,n) for k, v in kwargs.items()}) for i in range(n)]
                    return axis0_to_array(res, arg)
        elif isinstance(arg, self.types):
            n = len(arg)
            res = [self._wrapped(arg[i], (_item_by_i(a,i,n) for a in args), {k: _item_by_i(v,i,n) for k, v in kwargs.items()}) for i in range(n)]                            
            return type(arg)(res)
        else:
            return self.function(arg, *args, **kwargs)

def loop(*types):
    """
    returns an instance of loops(types = types)
    """
    return loops(types = as_tuple(types))


def _T(arg):
    if isinstance(arg, tuple):
        return type(arg)([_T(a) for a in arg])
    elif isinstance(arg, list):
        return type(arg)([_T(a) for a in arg])
    elif isinstance(arg, dict):
        return type(arg)({k :_T(a) for k,a in arg.items()})
    else:
        return arg.T if isinstance(arg, (np.ndarray, pd.DataFrame)) and len(arg.shape) == 2 else arg

def _values(a):
    if is_series(a):
        return a.values
    elif is_df(a):
        if a.shape[-1] == 1:
            return a.values.T[0]
        else:
            return a.values
    else:
        return a

def _np2pd(res, arg):
    if isinstance(res, tuple):
        return tuple([_np2pd(r, arg) for r in res])
    elif isinstance(res, tuple):
        return [_np2pd(r, arg) for r in res]
    elif isinstance(res, dict):
        return type(res)({k: _np2pd(r, arg) for k,r in res.items()})
    n = len0(res)
    if n == len0(arg):
        if len(res.shape) == 2:
            res = pd.DataFrame(res, arg.index)
            if len(arg.shape) == 2 and res.shape[1] == arg.shape[1]:
                res.columns = arg.columns
            return res
        else:
            return pd.Series(res, arg.index)
    elif is_df(arg) and n == arg.shape[1]:
        return pd.Series(arg.columns, res)
    else:
        return res

class pd2np(wrapper):
    """
    converts a numpy-based function to work on pandas by converting to a numpy arrage and then converting back to panadas dataframe
    """
    def wrapped(self, *args, **kwargs):
        if len(args):
            arg = args[0]
        else:
            arg = kwargs[getargs(self.function)[0]]
        if not is_pd(arg):
            return self.function(*args, **kwargs)
        args_ = [_values(a) for a in args]
        kwargs_ = {k : _values(v) for k, v in kwargs.items()}
        res = self.function(*args_, **kwargs_)
        return _np2pd(res, arg)


loop_all = loops(types = (pd.Series, pd.DataFrame, np.ndarray, list, tuple, dict))

