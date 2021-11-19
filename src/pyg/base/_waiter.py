from typing import Awaitable
from pyg.base._decorators import wrapper
import asyncio
__all__ = ['waiter', 'async_wrapper']




# args = [waiter(f(x) if x%2 ==0 else x) for x in range(10)]
# args = [waiter(f(x)) for x in range(10)]
# res = await asyncio.gather(*args)



async def waiter(value):
    """
    serves awaited functions coroutines on a plate...

    :Example:
    ---------
    
    >>> def f(x):
    >>>     return x * 2

    >>> values = await waiter([f(i) for i in range(10)])
    >>> assert values == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]    
    >>> 
    >>> values = {i : f(i) for i in range(5)}
    >>> assert await(waiter(values)) == {0: 0, 1: 2, 2: 4, 3: 6, 4: 8}

    but also for async functions...
    
    >>> from pyg import *
    >>> import numpy as np
    >>> async def f(x):
    >>>     n = np.random.randint(5)
    >>>     await asyncio.sleep(n)
    >>>     print(x,'sleeping concurrently for ', n, 'seconds')
    >>>     return x * 3
    
    >>> values = await waiter([f(i) for i in range(10)])
    >>> assert values == [0, 3, 6, 9, 12, 15, 18, 21, 24, 27]
    >>> values = {i : f(i) for i in range(5)}
    >>> assert await(waiter(values)) == {0: 0, 1: 3, 2: 6, 3: 9, 4: 12}
    
    
    >>> values = [{j: f(i*j) for j in range(i)} for i in range(10)]
    >>> values = await waiter(values)
    >>> assert values == [{j: i*j*3 for j in range(i)} for i in range(10)]

    """
    if isinstance(value, (list,tuple)):
        values = await asyncio.gather(*[waiter(v) for v in value])
        return type(value)(values)
    elif isinstance(value, dict):
        values = await asyncio.gather(*[waiter(v) for v in value.values()])
        return type(value)(dict(zip(value.keys(), values))) 
    elif isinstance(value, Awaitable):
        return await value
    else:
        return value


class async_wrapper(wrapper):
    """
    Create a generic wrapper framework for wrapping both outright and async functions.
    
    :Example:
    ---------
    >>> class times2(async_wrapper):
    >>>     async def wrapped(self, *args, **kwargs):
    >>>         original_value = await waiter(self.function(*args, **kwargs))
    >>>         return original_value * 2
    
    >>> @times2
    >>> def f(x):
    >>>     return x+1
    >>> assert await f(2) == 6

    but the same decorator can be used for async functions too:

    >>> @times2
    >>> async def async_f(x):
    >>>     return x+1
    >>> assert await async_f(2) == 6
    
    """
    async def __call__(self, *args, **kwargs):
        if self.function is None and len(args) == 1 and len(kwargs) == 0:
            return await type(self)(function = args[0], **self._kwargs)
        else:
            return await waiter(getattr(self, 'wrapped', self.function)(*args, **kwargs))

