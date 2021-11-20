import pytest
from pyg import add_, cell, acell, GRAPH, get_data
from pyg import *  

@pytest.mark.asyncio
async def test_acell_basic():
    """
    We build an async and a sync in-memory libraries and check they evaluate to the same value

    """
    print('this requires pytest-asyncio package to be installed')
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
    assert dd.data == d.data == 15


@pytest.mark.asyncio
async def test_acell_and_GRAPH():
    g = GRAPH.copy()

    a = acell(add_, a = 1, b = 2, pk = 'key', key = 'a')
    b = acell(add_, a = a, b = a, pk = 'key', key = 'b')
    c = acell(add_, a = a, b = b, pk = 'key', key = 'c')
    d = acell(add_, a = c, b = b, pk = 'key', key = 'd')    

    d = await d()

    assert d.data == 15
    assert a._address in GRAPH
    assert b._address in GRAPH
    assert c._address in GRAPH
    assert d._address in GRAPH

    assert get_data(key = 'd') == 15

    a = a.load(-1)
    b = b.load(-1)
    d = d.load(-1)
    c = c.load(-1)

    assert a._address not in GRAPH
    assert b._address not in GRAPH
    assert c._address not in GRAPH
    assert d._address not in GRAPH

    assert GRAPH == g

@pytest.mark.asyncio
async def test_acell_and_push():
    
    a = acell(add_, a = 1, b = 2, pk = 'key', key = 'a')
    b = acell(add_, a = a, b = a, pk = 'key', key = 'b')
    c = acell(add_, a = a, b = b, pk = 'key', key = 'c')
    d = acell(add_, a = c, b = b, pk = 'key', key = 'd')    
    
    d = await d()
    assert get_data(key = 'd') == 15

    # now we push...
    a.a = 3
    a = await a.push()
    assert get_data(key = 'd') == 25    
    assert a.data == 5
    assert d.load().data == 25    

    a = cell(add_, a = 3, b = 2, pk = 'key', key = 'a').load(-1)
    b = cell(add_, a = a, b = a, pk = 'key', key = 'b').load(-1)
    c = cell(add_, a = a, b = b, pk = 'key', key = 'c').load(-1)
    d = cell(add_, a = c, b = b, pk = 'key', key = 'd').load(-1)
    assert d().data == 25
    a.a = 3
    a = a.push()
