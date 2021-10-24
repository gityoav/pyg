# -*- coding: utf-8 -*-
import numpy as np
from pyg.base._as_list import as_list
from pyg.base._encode import as_primitive
from pyg.base._types import is_nan, is_iterable, is_float, is_int, is_bool, is_str
from pyg.base._loop import len0
from pyg.base._eq import veq


__all__ = ['Cmp', 'cmp', 'sort',]

def cmparr(x,y):
    c = 0
    for pair in zip(x,y):
        c = cmp(*pair)
        if c!=0:
            return c
    return c
                

def cmp(x,y):
    """
    Implements lexcompare while allowing for comparison of different types.
    First compares by type, then by length, then by keys and finally on values

    :Parameters:
    ----------------
    x : obj
        1st object to be compared.
    y : obj
        2nd object to be compared.

    :Returns:
    -------
    int
        returns -1 if x<y else 1 if x>y else 0
    
    :Examples:
    --------------
    >>> assert cmp('2', 2) == 1
    >>> assert cmp(np.int64(2), 2) == 0
    >>> assert cmp(None, 2.0) == -1 # None is smallest
    >>> assert cmp([1,2,3], [4,5]) == 1 # [1,2,3] is longer
    >>> assert cmp([1,2,3], [1,2,0]) == 1 # lexical sorting 
    >>> assert cmp(dict(a = 1, b = 2), dict(a = 1, c = 2)) == -1 # lexical sorting on keys
    >>> assert cmp(dict(a = 1, b = 2), dict(b = 2, a = 1)) == 0 # order does not matter

    """
    if x is y:
        return 0
    x,y = as_primitive([x,y])
    tx = str(type(x))
    ty = str(type(y))
    if tx<ty:
        return -1
    elif ty<tx:
        return 1
    lx = len0(x)
    ly = len0(y)
    if lx<ly:
        return -1
    elif ly<lx:
        return 1
    if isinstance(x, dict):
        xk, xv = zip(*sorted(x.items()))
        yk, yv = zip(*sorted(y.items()))
        c = cmparr(xk, yk)
        if c!=0:
            return c
        else:
            return cmparr(xv, yv)
    if is_nan(x):
        x = np.inf
    if is_nan(y):
        y = np.inf
    if is_iterable(x):
        return cmparr(x,y)
    else:
        return -1 if x<y else 1 if x>y else 0
    
class Cmp(object):
    """
    class wrapper of cmp, allowing us to compare objects of different types

    :Example:
    ---------
    >>> with pytest.raises(TypeError):
    >>>     sorted([1,2,3,None])

    >>> # but this is fine:
    >>> assert sorted([1,3,2,None], key = Cmp) == [None, 1, 2, 3]
        
    """
    def __init__(self, x):
        self.x = x

    def cmp(self, y):
        if isinstance(y, Cmp):
            y = y.x
        return cmp(self.x, y)

    def __lt__(self, y):
        return self.cmp(y)==-1

    def __gt__(self, y):
        return self.cmp(y)==1

    def __str__(self):
        return 'Compare(%s)'%self.x 

vcmp = np.vectorize(Cmp)

def sort(iterable):
    """
    implements sorting allowing for comparing of not-same-type objects

    :Parameters:
    ----------
    iterable : iterable
        values to be sorted

    :Returns:
    -------
    list
        sorted list.

    :Example:
    ---------
    >>> with pytest.raises(TypeError):
    >>>     sorted([1,2,3,None])
    >>> # but this is fine:
    >>> sort([1,3,2,None]) == [None, 1, 2, 3]

    """
    try:
        return sorted(iterable)
    except TypeError:
        return sorted(iterable, key = Cmp)


# def _type(x):
#     return str(float if is_float(x) else int if is_int(x) else bool if is_bool(x) else str if is_str(x) else type(x))

# def _len(x):
#     return len(x) if hasattr(x, '__len__') else 0

# def _type_length(x):
#     return (_type(x), _len(x))


# vtype = np.vectorize(_type)
# vlen  = np.vectorize(_len)
# vtl = np.vectorize(_type_length)

# def as_1d_arrays(values):
#     """
#     if the user provided a columns of same-size np.arrays, we split it into its constituents
#     """
#     if isinstance(values, np.ndarray): 
#         if len(values.shape)==2:
#             values = list(values.T)
#         else:
#             values = [values]
#     res = []
#     for value in values:
#         value = np.array(value if _len(value) else [value])
#         if len(value.shape)==2:
#             res.extend(as_1d_arrays(value))
#         else:
#             res.append(value)
#     return res

# def as_cmp(values) :
#     return [vcmp(value) if value.dtype == np.dtype('O') else value for value in values]


# class Sort(object):
#     """
#     This class is the main sorting class 
    
#     `parameters`
#     :values are provided as lists
#     :key is used to transform the values into comparable keys
#     :sorter: The sorting itself is done using self.sorter. This is np.lexsort by default, can use panda_sorter as an alternative

#     self.keys is then what is actually sorted and compared, derived as self.key applied to self.values
#     self.argsort is what produces the indexing, applied to self.keys
#     self.sort is then the indexing from argsort applied to any list of values
#     self.sorted is applying self.sort to the original self.values
    
#     self.grouped uses the actual values to return a list of lists, each element has the same original value.
#     """
#     def __init__(self, values, sorter=np.lexsort, transform=None, key = [as_1d_arrays, as_cmp]):
#         self.values = tuple(np.array(v) for v in values)
#         if transform: 
#             self.values = tuple(transform(v) for v in self.values)
#         self.sorter = sorter
#         self.transform = transform
#         self.key = key
    
#     @property
#     def keys(self):
#         values = self.values
#         for k in as_list(self.key):
#             values = k(values)
#         return values[::-1]

#     @property
#     def argsort(self):
#         return self.sorter(self.keys)
    
#     @property
#     def sorted(self):
#         """
#         sorting myself based on argsort, returns a transposed matrix!
#         """
#         return self.sort(self.values)
    
#     def sort(self, values):
#         """
#         sorting values based on argsort
#         """
#         if isinstance(values, list):
#             return [values[i] for i in self.argsort]
#         elif isinstance(values, np.ndarray):
#             return values[self.argsort]
#         elif isinstance(values, tuple):
#             return type(values)(self.sort(v) for v in values)
#         elif isinstance(values, dict):
#             return type(values)({key: self.sort(value) for key, value in values.items()})
    
#     def __len__(self):
#         return len(self.keys[0])
    
#     @property
#     def shape(self):
#         return (len(self), len(self.keys))

#     @property
#     def _edges(self):
#         """
#         find the points, in the sorted data, where we have a change in value
#         we know the sorted data is indexed by argsort so we then return the coordinates in the original data
#         """
#         changes = ~np.min(np.array([veq(col[1:], col[:-1]) for col in self.sorted]), axis=0)
#         return np.arange(1, len(self))[changes]

#     @property
#     def grouped(self):
#         return np.split(self.argsort, self._edges)
        
#     def group(self, values):
#         """
#         based on the keys, group all values
#         """
#         if isinstance(values, np.ndarray):
#             return np.split(self.sort(values), self._edges)
#         elif isinstance(values, list):
#             return [[values[i] for i in grp] for grp in self.grouped]
#         elif isinstance(values, tuple):
#             return type(values)(self.group(v) for v in values)
#         elif isinstance(values, dict):
#             return type(values)({key: self.group(value) for key, value in values.items()})

#     @property
#     def unique(self):
#         mask = np.concatenate([[0], self._edges])
#         return [col[mask] for col in self.sorted]
    
#     def __str__(self):
#         return '%s of %s' % (type(self).__name__, self.values.__str__())
    
#     def __repr__(self):
#         return '%s of %s' % (type(self).__name__, self.values.__repr__())
