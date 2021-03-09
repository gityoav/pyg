from pyg.base._as_list import as_list, is_rng
from pyg.base._types import is_str
from pyg.base._ulist import ulist
from pyg.base._logger import logger

__all__ = ['dictattr', 'relabel']

class dictattr(dict):
    '''
    A simple dict with extended member manipulation
        1) access using d.key
        2) access multiple elements using d[key1, key2]
    
    
    :Example: members access
    ------------------------
    >>> from pyg import *
    >>> d = dictattr(a = 1, b = 2, c = 3)
    >>> assert isinstance(d, dict)
    >>> assert d.a == 1
    >>> assert d['a','b'] == [1,2]
    >>> assert d[['a','b']] == dictattr(a = 1, b = 2)


    In addition, it has extended key selection/subsetting
    
    :Example: subsetting
    -------------------
    >>> d = dictattr(a = 1, b = 2, c = 3)
    >>> assert d - 'a' == dictattr(b = 2, c = 3)
    >>> assert d & ['b', 'c', 'not in keys'] == dictattr(b = 2, c = 3)


    dictattr supports not in-place 'update':

    :Example: updating via adding another dict
    ------------------------------------------
    >>> d = dictattr(a = 1, b = 2) + dict(b = 'replacing old value', c = 'new key')
    >>> assert d == dictattr(a = 1, b = 'replacing old value', c = 'new key')

        
    '''
    def __sub__(self, key, copy = True):
        """
        deletes an item but does not throw an exception if not there
        dictattr uses subtraction to remove key(s)

        :Returns:
        -------
        updated dictattr
        
        
        :Example:
        -------
        >>> from pyg import *
        >>> d = dictattr(a = 1, b = 2, c = 3)
        >>> assert d - ['b','c'] == dictattr(a = 1)
        >>> assert d - 'c' == dictattr(a = 1, b = 2)
        >>> assert d - 'key not there' == d
        >>> #commutative
        >>> assert (d - 'c').keys() == d.keys() - 'c'
        """
        res = self.copy() if copy else self
        if isinstance(key, tuple):
            branch = res
            for k in key[:-1]:
                if k in branch:
                    branch = branch[k]
                else:
                    return res
            if key[-1] in branch:
                del branch[key[-1]]
        elif isinstance(key, list):
            for k in key:
                res = res.__sub__(k, copy = False)
        else:
            if key in res:
                del res[key]               
        return res

    def __and__(self, other):
        """
        dictattr uses & as a set operator for key filtering

        :Returns:
        -------
        updated dictattr
        
        :Example:
        -------
        >>> from pyg import *
        >>> d = dictattr(a = 1, b = 2, c = 3)
        >>> assert d & ['a', 'b', 'not_there'] == dictattr(a = 1, b = 2)
        >>> #commutative
        >>> assert (d & ['a', 'b', 'x']).keys() == d.keys() & ['a', 'b', 'x']
        """
        other = set(as_list(other))
        return type(self)(**{key : value for key, value in self.items() if key in set(self.keys()) & other}) 

    def __add__(self, other):
        """
        dictattr uses add as a copy + update. Similar to the latest python |=        
        
        :Example:
        ---------
        >>> from pyg import *
        >>> d = dictattr(a = 1, b = 2)
        >>> assert d + dict(b = 3, c = 5) == dictattr(a = 1, b = 3, c = 5)
        
        :Parameters:
        ------------
        other: dict
            a dict used to update current dict.
        
        """
        res = self.copy()
        res.update(other)
        return res
    
    def __dir__(self):
        return list(self.keys()) + super(dictattr, self).__dir__()

    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError as e: ## must be raised as an AttributeError so that getattr() works
            if attr.startswith('_'):
                return getattr(dict(self), attr)
            else:
                raise AttributeError(e)
    
    def __setattr__(self, attr, value):
        if attr.startswith('_'):
            super(dictattr, self).__setattr__(attr, value)
        else:
            self[attr] = value
    
    def __delattr__(self, attr):
        try:
            del self[attr]
        except KeyError as e:
            if attr.startswith('_'):
                super(dictattr, self).__delattr__(attr)
            else:
                raise AttributeError(str(e))

    def __delitem__(self, key):
        if isinstance(key, tuple):
            res = self
            for k in key[:-1]:
                res = res[k]
            del res[key[-1]]
        elif isinstance(key, list):
            for k in key:
                del self[k]
        else:
            try:
                super(dictattr, self).__delitem__(key)               
            except KeyError as e:
                if is_str(key) and '.' in key:
                    res = self
                    keys = key.split('.')
                    for k in keys[:-1]:
                        res = res[k]
                    del res[keys[-1]]
                else:
                    raise KeyError(e)
    
    def __getitem__(self, value):
        if isinstance(value, tuple):
            return [self[v] for v in value]
        elif is_rng(value):
            return type(self)(**{k : self[k] for k in value})
        res = self
        if value in res or not is_str(value):
            return super(dictattr, self).__getitem__(value)
        else:
            for k in value.split('.'):
                res = dict(res)[k]
            return res
    
    def copy(self):
        return type(self)(**self)
    
    def __getstate__(self):
        return dict(self)
    
    def __setstate__(self, state):
        self.update(state)
    
    def __dict__(self):
        return dict(self)
    
    def keys(self):
        """
        dictattr returns an actual list rather than a generator. 
        Further, this recognises that the keys are necessarily unique so it returns a ulist which is also a set

        :Returns:
        -------
        ulist
            list of keys of dictattr.

        :Example:
        -------
        >>> from pyg import *
        >>> d = dictattr(a = 1, b = 2)
        >>> assert d.keys() == ulist(['a', 'b'])
        >>> assert d.keys() & ['a', 'c', 'd'] == ['a']
        """
        return ulist(super(dictattr, self).keys(), unique = True)
    
    def values(self):
        return list(super(dictattr, self).values())
    
    def relabel(self, *args, **relabels):
        """
        easy relabel/rename of keys

        :Parameters:
        ----------------
        *args : str or callable
            - a string ending/starting with _ will trigger a prefix/suffix to all keys
            - callable function will be applied to the keys to update them
            
        **relabels : strings
            individual relabeling of keys

        :Returns:
        -------
        dictattr
            new dict with renamed keys.
        
        :Example: suffix/prefix
        ------------------------
        >>> from pyg import *
        >>> d = dictattr(a = 1, b = 2, c = 3)
        >>> assert d.relabel('x_') == dictattr(x_a = 1, x_b = 2, x_c = 3) # prefixing
        >>> assert d.relabel('_x') == dictattr(a_x = 1, b_x = 2, c_x = 3) # suffixing

        :Example: callable 
        -------------------
        >>> assert d.rename(upper) == dictattr(A = 1, B = 2, C = 3)

        :Example: individual relabelling
        -------------------
        >>> assert d.rename(a = 'A') == dictattr(A = 1, b = 2, c = 3)
        >>> assert d.rename(['A', 'B', 'C']) == d.relabel(upper)
        """
        keys = relabel(list(self.keys()), *args, **relabels)
        return type(self)(**{keys.get(k,k) : v for k, v in self.items()})

    def rename(self, *args, **relabels):
        """
        Identical to relabel. See relabel for full docs
        """
        return self.relabel(*args, **relabels)
    
def relabel(keys, *args, **relabels):
    """
    returns a mapping from old keys to new keys
    
    :Example:
    --------------
    
    >>> assert relabel(['a', 'b', 'c'], a = 'A', b = 'B') == dict(a = 'A', b = 'B')
    >>> assert relabel(['a', 'b', 'c'], 'x_') == dict(a = 'x_a', b = 'x_b', c = 'x_c')
    >>> assert relabel(['a', 'b', 'c'], '_x') == dict(a = 'a_x', b = 'b_x', c = 'c_x')
    >>> assert relabel('a', lambda v: v * 2) == dict(a = 'aa')
    >>> assert relabel(['a', 'b'], lambda v: v * 2, b = 'other') == dict(a = 'aa', b = 'other')
    >>> assert relabel(['a', 'b', 'c'], 'A', 'B', 'C') == dict(a = 'A', b = 'B', c = 'C')
    >>> assert relabel(['a', 'b', 'c'], ['A', 'B', 'C']) == dict(a = 'A', b = 'B', c = 'C')
    
    :Parameters:
    ----------------
    keys : list
        a list of keys.
    *args : TYPE
        either 
        - a function to relabel keys.
        - a prefix/suffix
        - a list of new keys
    **relabels : TYPE
        DESCRIPTION.

    :Returns:
    -------
    res : dict
        a mapping from the old keys to the new keys. Note: result may not have all keys

    """
    keys = as_list(keys)
    args = as_list(args)
    res = {}
    if len(args) == 1:
        arg = args[0]
        if isinstance(arg , str):
            if arg.startswith('_'):
                res = {key : key + arg for key in keys}
            elif arg.endswith('_'):
                res = {key : arg + key for key in keys}
        elif callable(arg):
            res = {key : arg(key) for key in keys}
        elif isinstance(arg, dict):
            res.update(arg)
    elif len(args) == len(keys):
        res = {key : new_key  for key, new_key in zip(keys, args)}
    res.update(relabels)
    return res