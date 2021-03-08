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

__all__ = ['cell', 'cell_item', 'cell_go', 'cell_func', 'cell_output']

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

def cell_item(value, key = None):
    """
    returns an item from a cell (if not cell, returns back the value).
    If no key is provided, will return the output of the cell
    
    :Parameters:
    ------------------
    value : cell or object or list of cells/objects
        DESCRIPTION.
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
def _cell_go(value, go):
    if isinstance(value, cell):
        return value.go(go)
    else:
        if isinstance(value, dict):
            return type(value)(**{k: cell_go(v, go) for k, v in value.items()})
        else:
            return value


def cell_go(value, go = 1):
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
    return _cell_go(value, go = go)

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
    def __init__(self, function = None, relabels = None, unitemized = None, uncalled = None, **other_relabels):
        relabels = relabels or {}
        relabels.update(other_relabels)
        super(cell_func, self).__init__(function = function, 
                                        relabels = relabels, 
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
        function_ = cell_item(cell_go(self.function, go))        
        callargs = getcallargs(function_, *args, **kwargs)
        spec = getargspec(function_)
        arg_names = [] if spec.args is None else spec.args
        
        c = dict(callargs)
        varargs = c.pop(spec.varargs) if spec.varargs else []
        called_varargs = cell_go(varargs, go)
        itemized_varargs = cell_item(called_varargs, _data)

        varkw = c.pop(spec.varkw) if spec.varkw else {}
        called_varkw = {k : v if k in self.uncalled else cell_go(v, go) for k, v in varkw.items()}
        itemized_varkw = {k : v if k in self.unitemized else cell_item(v, self.relabels.get(k,k)) for k, v in called_varkw.items()}

        defs = spec.defaults if spec.defaults else []
        params = dict(zip(arg_names[-len(defs):], defs))
        params.update(c)
        called_params = {k : v if k in self.uncalled else cell_go(v, go) for k, v in params.items()}
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
        output = cell_output(self)
        for o in output:
            if self.get(o) is None:
                return True
        return False
    
    def save(self):
        return self
    
    def load(self):
        return self
            
    def __call__(self, go = 0, **kwargs):
        return (self + kwargs).load().go(go)

    @property
    def fullargspec(self):
        return getargspec(self.function)
    
    @property
    def _args(self):
        return getargs(self.function)
    
    @property
    def _output(self):
        return cell_output(self)
    
    def _go(self, go = 0):
        if not callable(self.function):
            return self
        elif go>0 or self.run():
            if hasattr(self, '_address'):
                logger.info(str(self._address))
            kwargs = {arg: self[arg] for arg in self._args if arg in self}
            function = self.function if isinstance(self.function, cell_func) else cell_func(self.function)
            res, called_args, called_kwargs = function(go = go-1 if go>1 else 0, **kwargs)
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

    def go(self, go = 0, **kwargs):
        res = self._go(go, **kwargs)
        return res.save()
    
    def copy(self):
        return copy(self)

    def __repr__(self):
        return '%s\n%s'%(self.__class__.__name__,tree_repr(dict(self)))
        

    