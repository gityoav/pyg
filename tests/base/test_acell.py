import pytest
from pyg import add_, cell, acell

@pytest.mark.asyncio
async def test_acell_basic():
    a = acell(add_, a = 1, b = 2)
    b = acell(add_, a = a, b = a)
    c = acell(add_, a = a, b = b)
    d = acell(add_, a = c, b = b)
    
    d = await d()
    
    aa = cell(add_, a = 1, b = 2)
    bb = cell(add_, a = aa, b = aa)
    cc = cell(add_, a = aa, b = bb)
    dd = cell(add_, a = cc, b = bb)
    
    dd = dd()
    assert dd.data == d.data

    
    

