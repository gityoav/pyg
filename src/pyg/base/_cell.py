from pyg.base._as_list import as_list
from pyg.base._dictattr import dictattr
from pyg.base._dict import Dict 
from pyg.base._inspect import getargspec, getargs, getcallargs
from pyg.base._loop import loop
from pyg.base._tree_repr import tree_repr
from pyg.base._decorators import wrapper
from pyg.base._logger import logger
from copy import copy

_data = 'data'
_output = 'output'
_function = 'function'

__all__ = ['cell', 'cell_item', 'cell_go', 'cell_load', 'cell_func', 'cell_output', 'cell_clear']

def cell_output(c): 
    """
    returns the keys the cell output is stored at. equivalent to cell._output
    """
    res = c.get(_output)
    if res is None:
        res = getattr(c.get(_function), _output, None)
    if res is None:
        res = _data
    return as_list(res)


@loop(list, tuple)
def _cell_item(value, key = None):
    if not isinstance(value, cell):
        if isinstance(value, dict):
            return type(value)(**{k: cell_item(v, key) for k, v in value.items()})
        else:
            return value
    output = cell_output(value)
    if len(output) == 1:
        if key is None:
            return value[output[0]]
        else:
            try:
                return value[output[0]]
            except KeyError:
                return value[key]

    else:
        if key is not None:
            if key in output:
                return value[key]
            elif _data == output[0]:
                return value[_data]
            else:
                return Dict(value)[output]
        else:
            if _data == output[0]:
                return value[_data]
            else:            
                return Dict(value)[output]

def cell_clear(value):
    """
    cell_clear clears a cell of its output so that it contains only the essentil stuff to do its calculations.
    This will be used when we save the cell or we want to recalculate it.
    
    :Example:
    ---------
    >>> from pyg import *    
    >>> a = cell(add_, a = 1, b = 2)
    >>> b = cell(add_, a = 2, b = 3)
    >>> c = cell(add_, a = a, b = b)()
    >>> assert c.data == 8    
    >>> assert c.a.data == 3

    >>> bare = cell_clear(c)
    >>> assert 'data' not in bare and 'data' not in bare.a
    >>> assert bare() == c

    
    :Parameters:
    ------------
    value: obj
        cell (or list/dict of) to be cleared of output

    """
    if isinstance(value, cell):
        return value._clear()
    elif isinstance(value, (tuple, list)):
        return type(value)([cell_clear(v) for v in value])
    elif isinstance(value, dict):
        return type(value)(**{k : cell_clear(v) for k, v in value.items()})
    else:
        return value


def cell_item(value, key = None):
    """
    returns an item from a cell (if not cell, returns back the value).
    If no key is provided, will return the output of the cell
    
    :Parameters:
    ------------------
    value : cell or object or list of cells/objects
        cell
    key : str, optional
        The key within cell we are interested in. Note that key is treated as GUIDANCE only. 
        Our strong preference is to return valid output from cell_output(cell)

    :Example: non cells
    ---------------------------------
    >>> assert cell_item(1) == 1
    >>> assert cell_item(dict(a=1,b=2)) == dict(a=1,b=2)

    :Example: cells, simple
    ----------------------------
    >>> c = cell(lambda a, b: a+b, a = 1, b = 2)
    >>> assert cell_item(c) is None
    >>> assert cell_item(c.go()) == 3

    """
    return _cell_item(value, key = key)

@loop(list, tuple)
def _cell_go(value, go, mode = 0):
    if isinstance(value, cell):
        return value.go(go = go, mode = mode)
    else:
        if isinstance(value, dict):
            return type(value)(**{k: _cell_go(v, go = go, mode = mode) for k, v in value.items()})
        else:
            return value


def cell_go(value, go = 0, mode = 0):
    """
    cell_go makes a cell run (using cell.go(go)) and returns the calculated cell.
    If value is not a cell, value is returned.    

    :Parameters:
    ----------------
    value : cell
        The cell (or anything else).
    go : int
        same inputs as per cell.go(go).
        0: run if cell.run() is True
        1: run this cell regardless, run parent cells only if they need to calculate too
        n: run this cell & its nth parents regardless. 
        
    :Returns:
    -------
    The calculated cell
    
    :Example: calling non-cells
    ----------------------------------------------
    >>> assert cell_go(1) == 1
    >>> assert cell_go(dict(a=1,b=2)) == dict(a=1,b=2)

    :Example: calling cells
    ------------------------------------------
    >>> c = cell(lambda a, b: a+b, a = 1, b = 2)
    >>> assert cell_go(c) == c(data = 3)

    """
    return _cell_go(value, go = go, mode = mode)


@loop(list, tuple)
def _cell_load(value, mode):
    if isinstance(value, cell):
        return value.load(mode)
    else:
        if isinstance(value, dict):
            return type(value)(**{k: _cell_load(v, mode) for k, v in value.items()})
        else:
            return value

def cell_load(value, mode = 0):
    """
    loads a cell from a database or memory and return its updated values

    :Parameters:
    ----------------
    value : cell
        The cell (or anything else).
    mode: 1/0/-1
        Used by cell.load(mode) -1 = no loading, 0 = load if available, 1 = throw an exception in unable to load

    :Returns:
    -------
    A loaded cell
    
    """
    return _cell_load(value, mode)
        

class cell_func(wrapper):
    """
    cell_func is a wrapped and wraps a function to act on cells rather than just on values
    
    When called, it will returns not just the function, but also args, kwargs used to call it.
    
    :Example:
    -------
    >>> from pyg import *
    >>> a = cell(lambda x: x**2, x  = 3)
    >>> b = cell(lambda y: y**3, y  = 2)
    >>> function = lambda a, b: a+b
    >>> self = cell_func(function)
    >>> result, args, kwargs = self(a,b)

    >>> assert result == 8 + 9
    >>> assert args[0].data == 3 ** 2
    >>> assert args[1].data == 2 ** 3

    
    """
    def __init__(self, function = None, relabels = None, 
                 unitemized = None, 
                 uncalled = None, 
                 unloaded = None, **other_relabels):
        relabels = relabels or {}
        relabels.update(other_relabels)
        super(cell_func, self).__init__(function = function, 
                                        relabels = relabels, 
                                        unloaded = as_list(unloaded),
                                        unitemized = as_list(unitemized),
                                        uncalled = as_list(uncalled))

    def wrapped(self, *args, **kwargs):
        """
        function = lambda a: a+1
        self = cell_func(function)
        args = ()
        kwargs = dict(go = 0, a = 1)
        import inspect
        
        
        """
        go = kwargs.pop('go', 0)
        mode = kwargs.pop('mode', 0)
        function_ = cell_item(cell_go(self.function, go))        
        callargs = getcallargs(function_, *args, **kwargs)
        spec = getargspec(function_)
        arg_names = [] if spec.args is None else spec.args
        
        c = dict(callargs)
        varargs = c.pop(spec.varargs) if spec.varargs else []
        loaded_args = cell_load(varargs, mode)
        called_varargs = cell_go(loaded_args , go)
        itemized_varargs = cell_item(called_varargs, _data)

        varkw = c.pop(spec.varkw) if spec.varkw else {}
        loaded_varkw = {k : v if k in self.unloaded else cell_load(v, mode) for k, v in varkw.items()}
        called_varkw = {k : v if k in self.uncalled else cell_go(v, go) for k, v in loaded_varkw.items()}
        itemized_varkw = {k : v if k in self.unitemized else cell_item(v, self.relabels.get(k,k)) for k, v in called_varkw.items()}

        defs = spec.defaults if spec.defaults else []
        params = dict(zip(arg_names[-len(defs):], defs))
        params.update(c)
        loaded_params = {k : v if k in self.unloaded else cell_load(v, mode) for k, v in params.items()}
        called_params = {k : v if k in self.uncalled else cell_go(v, go) for k, v in loaded_params.items()}
        itemized_params = {k : v if k in self.unitemized else cell_item(v, self.relabels.get(k,k)) for k, v in called_params.items()}
        
        args_ = [itemized_params[arg] for arg in arg_names if arg in params] + list(itemized_varargs)
        res = function_(*args_, **itemized_varkw)
        called_params.update(called_varkw)
        return res, itemized_varargs, called_params

        # go = kwargs.pop('go', 0)
        # function_ = cell_item(cell_go(self.function, go))
        # called_args = cell_go(args, go)
        # itemized_args = cell_item(called_args, _data)
        # called_kwargs = {k : v if k in self.uncalled else cell_go(v, go) for k, v in kwargs.items()}
        # itemized_kwargs = {k : v if k in self.unitemized else cell_item(v, self.relabels.get(k,k)) for k, v in called_kwargs.items()}
        # return function_(*itemized_args, **itemized_kwargs), called_args, called_kwargs


class cell(dictattr):
    """
    cell is a Dict that can be though of as a node in a calculation graph. 
    The nearest parallel is actually an Excel cell:
    
    - cell contains both its function and its output. cell.output defines the keys where the output is supposed to be
    - cell contains reference to all the function outputs
    - cell contains its locations and the means to manage its own persistency


    :Parameters:
    ------------------
    - function is the function to be called
    - ** kwargs are the function named key value args. NOTE: NO SUPPORT for *args nor **kwargs in function
    - output: where should the function output go?
    
    :Example: simple construction
    -----------------------------------------------
    >>> from pyg import *
    >>> c = cell(lambda a, b: a+b, a = 1, b = 2)
    >>> assert c.a == 1
    >>> c = c.go()
    >>> assert c.output == ['data'] and c.data == 3

    :Example: make output go to 'value' key
    ---------------------------------------------------------------
    >>> c = cell(lambda a, b: a+b, a = 1, b = 2, output = 'value')
    >>> assert c.go().value == 3

    :Example: multiple outputs by function
    -------------------------------------------------------------
    >>> f = lambda a, b: dict(sum = a+b, prod = a*b)
    >>> c = cell(f, a = 1, b = 2, output  = ['sum', 'prod'])
    >>> c = c.go()
    >>> assert c.sum == 3 and c.prod == 2

    :Methods:
    ---------------    
    - cell.run() returns bool if cell needs to be run
    - cell.go() calculates the cell and returns the function with cell.output keys now populated.    
    - cell.load()/cell.save() interface for self load/save persistence
    
    
    """
    def __init__(self, function = None, output = None, **kwargs):
        if (len(kwargs) == 0 and isinstance(function, (Dict, cell))) or (isinstance(function,dict) and not callable(function)): 
            kwargs.update(function)
        else:
            kwargs[_function] = function
        if output is not None:
            kwargs[_output] = output
        super(cell, self).__init__(**kwargs)


    def run(self):
        """
        checks if the cell needs calculation. This depends on the nature of the cell. 
        By default (for cell and db_cell), if the cell is already calculated so that cell._output exists, then returns False. otherwise True

        Returns
        -------
        bool
            run cell?
            
        :Example:
        ---------
        >>> c = cell(lambda x: x+1, x = 1)
        >>> assert c.run()
        >>> c = c()
        >>> assert c.data == 2 and not c.run()
        """
        output = cell_output(self)
        for o in output:
            if self.get(o) is None:
                return True
        return False
    
    def save(self):
        """
        Saves the cell for persistency. Not implemented for simple cell. see db_cell

        :Returns:
        -------
        cell
            self, saved.

        """
        return self
    
    def load(self, mode = 0):
        """
        Loads the cell from the database based on primary keys of cell perhaps. Not implemented for simple cell. see db_cell

        :Returns:
        -------
        cell
            self, updated with values from database.
        """
        return self
            
    def __call__(self, go = 0, mode = 0, **kwargs):
        return (self + kwargs).load(mode = mode).go(go = go, mode = mode)


    @property
    def fullargspec(self):
        return getargspec(self.function)
    
    @property
    def _args(self):
        """
        returns the keyword arguments within the cell that will be presented to the cell.function
        """
        return getargs(self.function)
    
    
    @property
    def _output(self):
        """
        returns the keys within the cell where the output from the function will be stored.
        This can be set in two ways:
        
        :Example:
        ----------
        >>> from pyg import *
        >>> f = lambda a, b : a + b
        >>> c = cell(f, a = 2, b = 3)
        >>> assert c._output == ['data']
        
        >>> c = cell(f, a = 2, b = 3, output = 'x')
        >>> assert c._output == ['x'] and c().x == 5            

        :Example: output for functions that return dicts
        ---------

        >>> f = lambda a, b : dict(sum = a + b, prod = a*b) ## function returns a dict!
        >>> c = cell(f, a = 2, b = 3)()
        >>> assert c.data['sum'] == 5 and c.data['prod'] == 6 ## by default, the dict goes to 'data' again

        >>> c = cell(f, a = 2, b = 3, output = ['sum', 'prod'])() ## please, send the output to these keys...
        >>> assert c['sum'] == 5 and c['prod'] == 6 ## by default, the dict goes to 'data' again

        >>> f.output = ['sum', 'prod'] ## we make the function declare that it returns a dict with these keys        
        >>> c = cell(f, a = 2, b = 3)() ## now the cell does not need to set this
        >>> assert c.sum == 5 and c.prod == 6 
        """
        return cell_output(self)

    def _clear(self):
        res = self if self.function is None else self - self._output
        return type(self)(**cell_clear(dict(res)))
    
    def _go(self, go = 0, mode = 0):
        if not callable(self.function):
            return self
        elif go!=0 or self.run():
            if hasattr(self, '_address'):
                logger.info(str(self._address))
            kwargs = {arg: self[arg] for arg in self._args if arg in self}
            function = self.function if isinstance(self.function, cell_func) else cell_func(self.function)
            res, called_args, called_kwargs = function(go = go-1 if go>0 else go, mode = mode, **kwargs)
            c = self + called_kwargs
            output = cell_output(c)
            if output is None:
                c[_data] = res
            elif len(output)>1:
                for o in output:
                    c[o] = res[o]
            else:
                c[output[0]] = res
            return c
        else:
            return self

    def go(self, go = 1, mode = 0, **kwargs):
        """
        calculates the cell (if needed). By default, will then run cell.save() to save the cell. 
        If you don't want to save the output (perhaps you want to check it first), use cell._go()

        :Parameters:
        ------------
        go : int, optional
            a parameter that forces calculation. The default is 0.
            go = 0: calculate cell only if cell.run() is True
            go = 1: calculate THIS cell regardless. calculate the parents only if their cell.run() is True
            go = 2: calculate THIS cell and PARENTS cell regardless, calculate grandparents if cell.run() is True etc.
            go = -1: calculate the entire tree again.            
            
        **kwargs : parameters
            You can actually allocate the variables to the function at runtime

        Note that by default, cell.go() will default to go = 1 and force a calculation on cell while cell() is lazy and will default to assuming go = 0

        :Returns:
        -------
        cell
            the cell, calculated
        
        :Example: different values of go
        ---------------------
        >>> from pyg import *
        >>> f = lambda x=None,y=None: max([dt(x), dt(y)])
        >>> a = cell(f)()
        >>> b = cell(f, x = a)()
        >>> c = cell(f, x = b)()
        >>> d = cell(f, x = c)()

        >>> e = d.go()
        >>> e0 = d.go(0)
        >>> e1 = d.go(1)
        >>> e2 = d.go(2)
        >>> e_1 = d.go(-1)

        >>> assert not d.run() and e.data == d.data 
        >>> assert e0.data == d.data 
        >>> assert e1.data > d.data and e1.x.data == d.x.data
        >>> assert e2.data > d.data and e2.x.data > d.x.data and e2.x.x.data == d.x.x.data
        >>> assert e_1.data > d.data and e_1.x.data > d.x.data and e_1.x.x.data > d.x.x.data

        :Example: adding parameters on the run
        --------------------------------------
        >>> c = cell(lambda a, b: a+b)
        >>> d = c(a = 1, b =2)
        >>> assert d.data == 3


        """
        res = self._go(go = go, mode = mode, **kwargs)
        return res.save()
    
    def copy(self):
        return copy(self)

    def __repr__(self):
        return '%s\n%s'%(self.__class__.__name__,tree_repr(dict(self)))
        

    