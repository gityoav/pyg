from pyg.base._dictable import dictable, is_dictable
from pyg.base._as_list import as_list
from pyg.base._ulist import ulist
from pyg.base._types import is_str, is_dict, is_none
from pyg.base._logger import logger
from pyg.base._dates import dt
from pyg.base._reducer import reducer
from pyg.base._decorators import wrapper
from pyg.base._inspect import argspec_add, getargspec, argspec_defaults
import datetime
from operator import mul

_data = 'data'
_expiry = 'expiry'
_output = 'output'

__all__ = ['join', 'perdictable']

def _join_dictable_with_defaults(tbl_def1, tbl_def2):
    """
    performs a join while allowing for a default
    
    :Example:
    --------------
    >>> from pyg import *
    >>> tbl_def1 = dictable(a = [1,2,3], b = [4,5,6]), dict(b=0)
    >>> tbl_def2 = dictable(a = [2,3,4], c = [5,6,7]), dict(c=1)
    >>> res = _join_dictable_with_defaults(tbl_def1, tbl_def2) 
    >>> assert res[0] == dictable(a = [2,3,4,1], b = [5,6,0,4], c = [5,6,7,1])
    >>> assert res[1] == dict(b=0,c=1)


    :Parameters:
    -------------
    tbl_def1 : pair of a table and default value
    tbl_def2 : pair of a table and a default value

    :Returns:
    -------
    a pair of table and defaults

    """
    d1, def1 = tbl_def1
    d2, def2 = tbl_def2
    def1 = def1 or {}
    def2 = def2 or {}
    
    if d1 is None:
        d = d2
    elif d2 is None:
        d = d1
    else:
        d = d1 * d2
        if len(def1):
            d += (d2 / d1)(**def1)
        if len(def2):
            d += (d1 / d2)(**def2)
    defaults = {}
    defaults.update(def1)
    defaults.update(def2)
    return (d, defaults)

def _item(d, key, on = None, renames = None):
    """
    This re

    :Parameters:
    ----------------
    d : dictable or value
        value we want to extract item from
    key : TYPE
        DESCRIPTION.
    on : TYPE, optional
        DESCRIPTION. The default is None.
    renames : TYPE, optional
        DESCRIPTION. The default is None.

    :Returns:
    -------
    
    """
    on = as_list(on)
    if is_dictable(d):
        if isinstance(renames, list) and len(renames) == 1:
            renames = renames[0]
        if is_str(renames):
            d[key] = d[renames]
        elif is_dict(renames) and key in renames:
            d[key] = d[renames[key]]
        on = d.keys() & on
        if key in d.keys():
            d = d[on + [key]]
        elif _data in d.keys() and _data not in on:
            d = d.rename(**{_data: key})[on + [key]]
        elif len(d.keys()) == len(on) + 1:
            renames = (d.keys() - on)[0]
            d = d.rename(**{renames : key})[on + [key]]
        else:
            raise KeyError('Asked for key %s in %s but key not found'%(key, d.keys()))
        if len(on) == 0 or len(d) == 0:
            return d
        u = d.listby(on)
        if len(u) < len(d):
            logger.warning('WARNING: source for %s not unique per %s'%(key, on))
        return d    
    else:
        return d
        

def join(inputs, on = None, renames = None, defaults = None):
    """
    Suppose we have a function which is defined on simple numbers

    :Example:
    --------------
    >>> from pyg import *
    >>> profit = lambda amount, price: amount  * price    

    The amounts sold are available in one table and prices in another

    :Example:
    --------------
    >>> amounts = dictable(product = ['apple', 'orange', 'pear'], amount = [1,2,3])
    >>> prices = dictable(product = ['apple', 'orange', 'pear', 'banana'], price = [4,5,6,8])
    >>> join(dict(amount = amounts, price = prices), on = 'product')(profit = profit)    
    
    >>> dictable[3 x 4]
    >>> product|amount|price|profit
    >>> apple  |1     |4    |4     
    >>> orange |2     |5    |10    
    >>> pear   |3     |6    |18    
    
    :Parameters:
    ----------------
    inputs : dict
        a dict of input parameters, some of them may be dictables.
    on : str/list of str
        when we have dictables
    renames : dict, optional
        remapping. if the datasets contain multiple columns, you can say renames = dict(price = 'price_in_dollar') to tell the algo, this is the column to use
        The default is None.
    defaults : dict, optional
        Normally, an inner join is performed. However, if there is a default value/formula for when e.g. a price is missing, use this. The default is None.

    :Returns:
    -------
    dictable
        a dictable of an inner join.

    :Example: how column mapping is done
    ------------------------------------
    >>> on = 'a'
    >>> ## if there is only one column apart from keys, then it is selected:

    >>> assert join(dict(x = dictable(a = [1,2], data = [2,3])), on = on) == dictable(a = [1,2], x = [2,3])
    >>> assert join(dict(x = dictable(a = [1,2], random_name = [2,3])), on = on) == dictable(a = [1,2], x = [2,3])

    >>> ## if there are multiple columns, if variable name is there, we use it:
    >>> assert join(dict(x = dictable(a = [1,2], z = [2,3], x = [4,5])), on) == dictable(a = [1,2], x = [4,5])

    >>> ## if there are multiple columns, and 'data' is one of the columns, we use it:
    >>> assert join(dict(x = dictable(a = [1,2], z = [2,3], data = [4,5])), on) == dictable(a = [1,2], x = [4,5])

    :Example: how column mapping is done with rename
    ------------------------------------------------
    >>> with pytest.raises(KeyError):
    >>>     join(dict(x = dictable(a = [1,2], b = [2,3], c = [4,5])), on = 'a') ## pick b or c?
    >>> assert join(dict(x = dictable(a = [1,2], b = [2,3], c = [4,5])), on = 'a', renames = dict(x = 'c')) == dictable(a = [1,2,], x = [4, 5])
    
    
    :Example: joins with partial columns in some tables
    -----------------------------------------------------    
    >>> on = ['a', 'b', 'c']
    >>> a = dictable(a = [1,2,3,4], x = [1,2,3,4]) ## only column a here
    >>> b = dictable(b = [1,2,3,4], y = [1,2,3,4]) ## only column b here
    >>> c = dictable(a = [1,2,3,4], b = [1,2,3,4], c = [1,2,3,4], z = [1,2,3,4])
    >>> j = join(dict(x = a, y = b, z = c), on = ['a', 'b', 'c'])    
    >>> assert len(j) == 4 and sorted(j.keys()) == ['a', 'b', 'c', 'x', 'y', 'z']
    
    :Example: join with defaults
    ----------------------------
    If no defaults are provided, we need all variables to be present. 
    However, if we specify defaults, we left-join on that variable and insert the default value
    
    >>> x = dictable(a = [1,2,4], x = [1,2,4])
    >>> y = dictable(a = [1,2,3], x = [5,6,7])
    >>> on = 'a'
    >>> assert join(dict(x = x, y = y), on = on) == dictable(a = [1,2,], x = [1,2], y = [5,6])
    >>> assert join(dict(x = x, y = y), on = 'a', defaults = dict(x = None)) == dictable(a = [1,2,3], x = [1,2,None], y = [5,6,7])
    >>> assert join(dict(x = x, y = y), on = 'a', defaults = dict(y = 0)) == dictable(a = [1,2,4], x = [1,2,4], y = [5,6,0])
    >>> assert join(dict(x = x, y = y), on = 'a', defaults = dict(x = None, y = 0)) == dictable(a = [1,2,3,4], x = [1,2,None,4], y = [5,6,7,0])

    """
    
    defaults = {} if defaults is None else defaults
    defaults = {k:v for k,v in defaults.items() if k in inputs}
    renames = renames or {}
    seq = {key: _item(d, key, on = on.get(key) if is_dict(on) else on, renames = renames) for key, d in inputs.items()}
    non_dictables = {k : [v] for k, v in seq.items() if not is_dictable(v)}
    dictables = {k : v for k, v in seq.items() if is_dictable(v)}
    if len(dictables) == 0:
        return dictable(non_dictables)
    no_defaults = {k:v for k,v in dictables.items() if not k in defaults}
    tbl1 = reducer(mul, no_defaults.values())
    tbl_def1 = (tbl1, {})
    with_defaults = {k:v for k,v in dictables.items() if k in defaults}
    pairs = [(value, {key : defaults[key]}) for key, value in with_defaults.items()]
    tbl_def2 = reducer(_join_dictable_with_defaults, pairs, (None, None))
    tbl_def = _join_dictable_with_defaults(tbl_def1,tbl_def2) 
    res = tbl_def[0](**non_dictables)
    return res.sort(as_list(on))

class perdictable(wrapper):
    """
    A decorator that makes a function works per dictable and not just on original value

    :Example:
    -------    
    >>> f = lambda a, b: a+b
    >>> p = perdictable(f, on = ['key'])     
    
    The new modified function p now works the same on old values:

    :Paramaters:
    ------------
    function : callable
        A function
    
    on: str/list of str
        perform join based on these keys
    
    renames: dict 
        This tells us which column to grab from which table
    
    defaults: dict
        If a default is provided for a parameter, we will perform a left join, substituting missing values with the defaults
        
    if_none: bool, list of keys
        If historic data is None while the row has expired, should we force a recalculation? if True, will be done.
        
    output_is_input: bool, list of keys
        Some functions want their own outut to be presented to them. If you see to True, if cached values exist for these columns, these are provided to the function
        
    include_inputs:
        When we return the outputs, do you want the inputs to be included as well in the dictable.
        
    col: str
        the name of the variable output.

    :Example:
    -------
    >>> f = lambda a, b: a+b
    >>> p = perdictable(f, include_inputs = True)     
    >>> assert p(a = 1, b = 2) == 3
    >>> assert p(a = dictable(a = [1,2,3]), b = 3) == dictable(a = [1,2,3], b = 3, expiry = None, data = [4,5,6])



    # some parameters are constant, some are tables...
    
    >>> assert p(a = 1, b = dictable(key = ['a','b','c'], b = [1,2,3])) == dictable(key  = ['a', 'b', 'c'], data = [2,3,4])  

    # multiple tables... some unkeyed
    
    >>> assert p(a = dictable(a = [1,2]), b = dictable(key = ['a','b','c'], b = [1,2,3])) == dictable(key  = ['a','a', 'b', 'b', 'c','c'], data = [2,3,3,4,4,5])

    # multiple tables... all keyed
    
    >>> a = dictable(key = ['x', 'y'], data = [1,2])
    >>> b = dictable(key = ['y', 'z'], data = [3,4])
    >>> assert p(a = a, b = b) == dictable(key  = ['y'], data = [5])
    
    :Example: existing data provided using data and expiry
    -------------------------------------------------------
    >>> a = dictable(key = ['x', 'y', 'z'], data = [1,2,3])
    >>> b = dictable(key = ['x', 'y', 'z'], data = [1,3,4])
    >>> data = dictable(key = ['x', 'y'], data = ['we calculated this before', 'we calculated before but hasnt expired'])
    >>> expiry = dictable(key = ['x', 'y'], data = [dt(2000,1,1), dt(3000,1,1)])
    >>> inputs = dict(a = a, b = b)

    >>> res = p(a = a, b = b, data = data, expiry = expiry)
    >>> assert res.find_data(key = 'x').data == 'we calculated this before'
    >>> assert res.find_data(key = 'y').data == 5  # although calculated before, we recalculate as its expiry is in the future


    function = lambda a, b: dict(sum = a+b, prod = a*b); function.output = ['sum', 'prod']
    self = perdictable(function)
    self(a = 1, b = 2)
    inputs = dict(a = dictable(a = [1,2,3]), b = 2); expiry = None
    self(a = dictable(a = [1,2,3]), b = 2)
    """
    def __init__(self, function = None, on = None, renames = None, defaults = None, if_none = False, output_is_input = True, col = 'data', include_inputs = False):
        super(perdictable, self).__init__(function = function, on = on, renames = renames, defaults = defaults, if_none = if_none, output_is_input = output_is_input, col = col, include_inputs  = include_inputs)
    
    @property
    def output(self):
        return getattr(self.function, 'output', ['data'])

    @property
    def fullargspec(self):
        return argspec_add(getargspec(self.function), expiry = None, **{o : None for o in self.output})
                                
    def _value_output(self, expiry = None, **inputs):
        """
        this runs a normal function that is not defined to have a dict as an output

        """
        on = ulist(as_list(self.on))
        col = self.col
        cols = ulist(as_list(col))
        inputs[_expiry] = expiry
        defaults = argspec_defaults(self.function) if self.defaults is None else self.defaults
        defaults[col] = defaults.get(col, None)
        defaults[_expiry] = defaults.get(_expiry, None)
        ds = join(inputs, on = on, renames = self.renames, defaults = defaults)        
        missing_cols = cols - ds.keys()            
        provided_cols = cols - missing_cols
        
        if len(ds) == 0:
            return inputs.get(col, None)
        rows = ds if self.output_is_input is True else ds - [key for key in provided_cols if key not in as_list(self.output_is_input)]
        
        if len(ds) == 1 and len({k : v for k, v in inputs.items() if is_dictable(v)}) == 0:
            return rows[0][self.function]
        else:                
            missing_cols = cols - ds.keys()
            ### rows to be run because value is None
            if len(missing_cols): # if any of the cols is missing
                run_if_none = [True] * len(ds)
            elif self.if_none is False:
                run_if_none = [False] * len(ds)
            else:
                nones = ds[cols if self.if_none is True else ds.keys() & as_list(self.if_none)].do(is_none)
                run_if_none = [max(row.values()) for row in nones]

            ### rows to be run because has not expired
            today = dt(0)
            run_expiry = [value is None or value>=today for value in ds[_expiry]]
            
            ## default values if function is not run
            cache = ds[col] if col in ds.keys() else [None]*len(ds)            
            values = [row[self.function] if (rin or rex) else c for row, rin, rex, c in zip(rows, run_if_none, run_expiry, cache)]            
            if self.include_inputs:
                return rows(**{col : values})
            elif len(on)>0:
                return rows[on](**{col : values})                
            else:
                return dictable(**{col : values})

    def _dict_output(self, expiry = None, **inputs):
        """
        this runs a function that has function.output, meaning we want the function to have a dict as an output with its keys given by function.output

        """
        on = ulist(as_list(self.on))
        cols = ulist(self.output)
        inputs[_expiry] = expiry
        defaults = argspec_defaults(self.function) if self.defaults is None else self.defaults
        for key in cols:
            defaults[key] = defaults.get(key, None)
        defaults[_expiry] = defaults.get(_expiry, None)
        ds = join(inputs, on = on, renames = self.renames, defaults = defaults)        
        missing_cols = cols - ds.keys()            
        provided_cols = cols - missing_cols
        if len(ds) == 0:
            return {key : inputs.get(key , None) for key in cols}        ### different from single function
        rows = ds if self.output_is_input is True else ds - [key for key in provided_cols if key not in as_list(self.output_is_input)]
        
        if len(ds) == 1 and len({k : v for k, v in inputs.items() if is_dictable(v)}) == 0:
            return rows[0][self.function]
        else:                       
            ### rows to be run because value is None
            if len(missing_cols): # if any of the cols is missing
                run_if_none = [True] * len(ds)
            elif self.if_none is False:
                run_if_none = [False] * len(ds)
            else:
                nones = ds[cols if self.if_none is True else ds.keys() & as_list(self.if_none)].do(is_none)
                run_if_none = [max(row.values()) for row in nones]

            ### rows to be run because has not expired
            today = dt(0)
            run_expiry = [value is None or value>today for value in ds[_expiry]]
            
            ## default values if function is not run
            cache = ds(**{key : None for key in missing_cols})[cols]            

            values = dictable([row[self.function] if (rin or rex) else c for row, rin, rex, c in zip(rows, run_if_none, run_expiry, cache)])
            if self.include_inputs:
                return {key: rows(**{key : value}) for key, value in values.items()}                 
            elif len(on)>0:
                pks = rows[on]
                return {key: pks(**{key : value}) for key, value in values.items()} 
            else:
                return values

    def wrapped(self, expiry = None, **inputs):
        if getattr(self.function, _output, None) is None:
            return self._value_output(expiry = expiry, **inputs)
        else:
            return self._dict_output(expiry = expiry, **inputs)
            

        
    


    

