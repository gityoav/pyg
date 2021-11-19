from pyg import *
from pyg import waiter
import pytest
import asyncio


@pytest.mark.asyncio
async def test_waiter():
    def f(x):
        return x * 2

    values = await waiter([f(i) for i in range(10)])
    assert values == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]    
    
    values = {i : f(i) for i in range(5)}
    assert await(waiter(values)) == {0: 0, 1: 2, 2: 4, 3: 6, 4: 8}

    import numpy as np
    async def f(x):
        n = np.random.randint(2)
        await asyncio.sleep(n)
        print(x,'sleeping concurrently for ', n, 'seconds')
        return x * 3
    
    values = await waiter([f(i) for i in range(10)])
    assert values == [0, 3, 6, 9, 12, 15, 18, 21, 24, 27]
    values = {i : f(i) for i in range(5)}
    assert await(waiter(values)) == {0: 0, 1: 3, 2: 6, 3: 9, 4: 12}
    
    
    values = [{j: f(i*j) for j in range(i)} for i in range(10)]
    values = await waiter(values)
    assert values == [{j: i*j*3 for j in range(i)} for i in range(10)]


