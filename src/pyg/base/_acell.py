from pyg.base._cell import cell, UPDATED, GRAPH, _updated, _data, cell_func, cell_output, cell_load, cell_go, _cell_item, cell_item
from pyg.base._inspect import getargspec, getcallargs
from pyg.base._waiter import waiter
from pyg.base._logger import logger
from pyg.base._dag import get_DAG, topological_sort
import datetime
from typing import Awaitable


__all__ = ['acell_func', 'acell', 'acell_load', 'acell_go']



async def acell_go(value, go = 0, mode = 0):
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
    if isinstance(value, cell):
        res = value.go(go = go, mode = mode)
        if isinstance(res, Awaitable):
            return await res
        else:        
            return res
    else:
        if isinstance(value, dict):
            res = await waiter({k: acell_go(v, go = go, mode = mode) for k, v in value.items()})
            return type(value)(res) 
        elif isinstance(value, (list, tuple)):
            res = await waiter([acell_go(v, go = go, mode = mode) for v in value])
            return type(value)(res)
        else:
            return await waiter(value)

async def acell_load(value, mode = 0):
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
    if isinstance(value, cell):
        result = value.load(mode)
        if isinstance(result, Awaitable):
            return await result
        else:
            return result
    elif isinstance(value, (list, tuple)):
        return await waiter([acell_load(v, mode) for v in value])
    elif isinstance(value, dict):
        return await waiter({k : acell_load(v, mode) for k, v in value.items()})
    elif isinstance(value, Awaitable):
        return await value
    else:
        return value


class acell_func(cell_func):
    """
    acell_func performs the same (async) function as cell_func, loading and ensuring data is there and then presenting parameters to self.function.
    
    :Example:
    --------
    >>> from pyg import * 
    >>> a = acell(add_, a = 1, b = 2)
    >>> self = acell_func(add_)
    >>> args = ();  kwargs = dict(a = a, b = a)    
    >>> res = await self(*args, **kwargs)
    >>> assert res[0] == 6    
    """
    async def __call__(self, *args, **kwargs):
        if self.function is None and len(args) == 1 and len(kwargs) == 0:
            return await type(self)(function = args[0], **self._kwargs)
        else:
            return await waiter(getattr(self, 'wrapped', self.function)(*args, **kwargs))

    async def wrapped(self, *args, **kwargs):
        """
        we load all variables together using the waiter
        then we call all loaded cells together
        """
        go = kwargs.pop('go', 0)
        mode = kwargs.pop('mode', 0)
        function_ = cell_item(await waiter(cell_go(self.function, go)))
        callargs = getcallargs(function_, *args, **kwargs)
        spec = getargspec(function_)
        arg_names = [] if spec.args is None else spec.args
        
        c = dict(callargs)
        varargs = c.pop(spec.varargs) if spec.varargs else []
        varkw = c.pop(spec.varkw) if spec.varkw else {}
        defs = spec.defaults if spec.defaults else []
        params = dict(zip(arg_names[-len(defs):], defs))
        params.update(c)

        
        loaded_args = acell_load(varargs, mode)
        loaded_varkw = {k : v if k in self.unloaded else acell_load(v, mode) for k, v in varkw.items()}
        loaded_params = {k : v if k in self.unloaded else acell_load(v, mode) for k, v in params.items()}

        loaded_args, loaded_varkw , loaded_params = await waiter((loaded_args, loaded_varkw , loaded_params))

        called_varargs = acell_go(loaded_args , go)
        called_varkw = {k : v if k in self.uncalled else acell_go(v, go) for k, v in loaded_varkw.items()}
        called_params = {k : v if k in self.uncalled else acell_go(v, go) for k, v in loaded_params.items()}

        called_varargs, called_varkw, called_params = await waiter((called_varargs, called_varkw, called_params))

        itemized_varargs = cell_item(called_varargs, _data)
        itemized_varkw = {k : v if k in self.unitemized else _cell_item(v, self.relabels[k], True) if k in self.relabels else _cell_item(v, k) for k, v in called_varkw.items()}
        itemized_params = {k : v if k in self.unitemized else _cell_item(v, self.relabels[k], True) if k in self.relabels else _cell_item(v, k) for k, v in called_params.items()}
        
        args_ = [itemized_params[arg] for arg in arg_names if arg in params] + list(itemized_varargs)
        res = await waiter(function_(*args_, **itemized_varkw))
        called_params.update(called_varkw)
        return res, itemized_varargs, called_params


class acell(cell):
    """
    asynchronous cell. 
    
    """
    _func = acell_func
    _awaitable = True

    async def __call__(self, go = 0, mode = 0, **kwargs):
        loaded = await (self + kwargs).load(mode = mode)
        return await loaded.go(go = go, mode = mode)

    async def go(self, go = 1, mode = 0, **kwargs):
        res = await (self + kwargs)._go(go = go, mode = mode)
        address = res._address
        if address in UPDATED:
            res[_updated] = UPDATED[address] 
        else: 
            res[_updated] = datetime.datetime.now()
        return await res.save()

    async def _go(self, go = 0, mode = 0):
        address = self._address
        if callable(self.function) and (go!=0 or self.run()):
            if address:
                pairs = ', '.join([("%s = '%s'" if isinstance(value, str) else "%s = %s")%(key, value) for key, value in address])
                msg = "get_cell(%s)()"%pairs
            else:
                msg = str(address)
            logger.info(msg)
            kwargs = {arg: self[arg] for arg in self._args if arg in self}
            function = self.function if isinstance(self.function, self._func) else self._func(self.function)
            mode = 0 if mode == -1 else mode
            res, called_args, called_kwargs = await function(go = go-1 if go>0 else go, mode = mode, **kwargs)
            c = self + called_kwargs
            output = cell_output(c)
            if output is None:
                c[_data] = res
            elif len(output)>1:
                for o in output:
                    c[o] = res[o]
            else:
                c[output[0]] = res
            if address:
                UPDATED[address] = datetime.datetime.now()
                GRAPH[address] = c
            return c
        else:
            if address:
                GRAPH[address] = self
            return self
    
    async def push(self):
        """
        same as push for a normal cell but done slightly differently. We want to async as many cells as possible
        but we do need to calculate parents before children
        We therefore perform a topological sort and calculate async (in parallel) within each generation while we go up the generations in a serial fashion 
        """
        me = self._address
        res = await self.go()
        generations = topological_sort(get_DAG(), [me])['gen2node']
        for i, children in sorted(generations.items())[1:]: # we skop the first generation... we just calculated it
            GRAPH.update(await waiter({child : GRAPH[child].go() for child in children}))            
            for child in children:
                if child in UPDATED:
                    del UPDATED[child]
        if me:
            del UPDATED[me]
        return res
    
    async def load(self, mode = 0):
        return super(acell, self).load(mode = mode)
    
    def _load(self, mode = 0):
        return super(acell, self).load(mode = mode)

    async def save(self):
        return super(acell, self).save()


