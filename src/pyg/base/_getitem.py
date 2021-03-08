from pyg.base._zip import zipper
from pyg.base._types import is_tuples, is_tuple
from pyg.base._as_list import as_list

__all__ = ['getitem', 'callitem', 'callattr']

def getitem(value, key, *default):
    """
    gets an item, like getattr
    
    :Example:
    ---------
    >>> a = dict(a = 1)
    >>> assert getitem(a, 'a') == 1
    >>> assert getitem(a, 'b', 2) == 2
    
    >>> import pytest
    >>> with pytest.raises(KeyError):
    >>>     getitem(a, 'b') 

    """    
    if len(default):
        try:
            return value[key]
        except Exception:
            return default[0]
    else:
        return value[key]

def _args(args):
    if args is None:
        args = [()]
    if is_tuple(args):
        args = [args]
    assert is_tuples(args)
    return args
    
def callitem(value, key, args = None, kwargs = None):
    """
    gets an item and calls it

    :Example:
    ---------
    >>> c = dict(function = lambda a, b: a + b)
    >>> assert callitem(c, 'function', kwargs = dict(a = 1, b = 2)) == 3
    >>> assert callitem(c, 'function', args = (1, 2)) == 3
    
    Parameters
    ----------
    value : obj
        object that contrains an item.
    key : string
        key within object.
    args : tuple, optional
        tuple of values to be fed to function. The default is None.
    kwargs : dict, optional
        kwargs to be fed to the method. The default is None.

    """
    res = value
    args = _args(args)
    kwargs = kwargs or [{}]
    for m, a, k in zipper(as_list(key), as_list(args), as_list(kwargs)):        
        method  = res[m]
        a = a or ()
        k = k or {}
        res = method(*a, **k)
    return res

def callattr(value, attr, args = None, kwargs = None):
    """
    gets the attribute(s) from a value and calls its

    :Example:
    ---------
    >>> from pyg import *
    >>> value = Dict(function = lambda a, b: a + b)
    >>> assert callattr(value, 'function', kwargs = dict(a = 1, b = 2)) == 3
    >>> assert callattr(value, attr = 'function', args = (1, 2), kwargs = None) == 3

    >>> ts = pd.Series(np.random.normal(0,1,1000))    
    >>> assert ts.std() == callattr(ts, 'std')
    >>> assert eq(ts.ewm(com = 10).mean(), callattr(ts, ['ewm','mean'], kwargs = [{'com':10}, {}]))

    >>> d = dictable(a = [1,2,3,4,1,2], b = list('abcdef'))
    >>> assert callattr(d, ['inc', 'exc'], kwargs = [dict(a = 2), dict(b = 'f')]) == d.inc(a = 2).exc(b = 'f')

    Parameters
    ----------
    value : obj
        object that contrains an item.
    attr : string(s)
        key within object.
    args : tuple, optional
        tuple of values to be fed to function. The default is None.
    kwargs : dict, optional
        kwargs to be fed to the method. The default is None.

    """
    res = value
    args = _args(args)
    kwargs = kwargs or [{}]
    for m, a, k in zipper(as_list(attr), as_list(args), as_list(kwargs)):
        method  = getattr(res, m)
        a = a or ()
        k = k or {}
        res = method(*a, **k)
    return res
