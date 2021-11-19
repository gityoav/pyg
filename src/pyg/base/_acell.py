from pyg.base._cell import cell, UPDATED, GRAPH, _updated, _data, cell_func, cell_output, cell_load, cell_go, _cell_item, cell_item
from pyg.base._inspect import getargspec, getcallargs
from pyg.base._waiter import waiter
from pyg.base._logger import logger
import datetime

__all__ = ['cell_async_func', 'acell']


class cell_async_func(cell_func):
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

        
        loaded_args = cell_load(varargs, mode)
        loaded_varkw = {k : v if k in self.unloaded else cell_load(v, mode) for k, v in varkw.items()}
        loaded_params = {k : v if k in self.unloaded else cell_load(v, mode) for k, v in params.items()}

        loaded_args, loaded_varkw , loaded_params = await waiter((loaded_args, loaded_varkw , loaded_params))

        called_varargs = cell_go(loaded_args , go)
        called_varkw = {k : v if k in self.uncalled else cell_go(v, go) for k, v in loaded_varkw.items()}
        called_params = {k : v if k in self.uncalled else cell_go(v, go) for k, v in loaded_params.items()}


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
    _func = cell_async_func

    async def __call__(self, go = 0, mode = 0, **kwargs):
        return await (self + kwargs).load(mode = mode).go(go = go, mode = mode)

    async def go(self, go = 1, mode = 0, **kwargs):
        res = await (self + kwargs)._go(go = go, mode = mode)
        address = res._address
        if address in UPDATED:
            res[_updated] = UPDATED[address] 
        else: 
            res[_updated] = datetime.datetime.now()
        return res.save()

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

