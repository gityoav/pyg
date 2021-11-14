from pyg import cell, cell_func, dictattr, dt, getargspec, passthru, add_
from pyg.base._cell import cell_output, cell_item, cell_inputs, UPDATED
import pytest
from pyg import get_cell, cell_push, cell_pull

def test_cell():
    c = cell(lambda a:a+1)
    assert cell_output(c) == ['data']
    with pytest.raises(TypeError):
        c.go()
    c.a = 1
    assert c.go().data == 2
    assert c().data == 2
    assert isinstance(c, dictattr)


def test_cell_go():
    c = cell(a = 1)
    assert c.go() == c    
    a = cell(lambda a: a +1 , a = 1, output = 'b')
    a = a.go()    
    assert a.b == 2
    f = lambda a: dict(a3 = a+3, a1 = a+1)
    f.output = ['a3', 'a1']
    b = cell(f, a = 1)
    assert cell_output(b) == f.output    
    b = b.go()
    assert b.a3 == 4 and b.a1 == 2
    
    
def test_cell_of_cell():
    a = cell(a = 1)
    b = cell(data = 2)    
    self = cell(lambda a,b:a+b, a = a, b=b)
    assert self.go().data == 3


def test_cell_fullargspec():
    function = lambda a, b = 1, **some_params: 1
    assert cell(function).fullargspec == getargspec(function)

def test_cell_func_cell():
    f = cell_func(lambda a, b: a+b, unitemized = ['a', 'b'])
    a = cell(a = 1)
    b = cell(b = 2)    
    c = cell(f, a = a, b = b)
    c = c.go()
    assert c.data == cell(a = 1) + cell(b=2)
    a = cell(lambda a: a * 3, a = 1)
    b = cell(lambda b: b * 3, b = 2)    
    c = cell(f, a = a, b = b)
    c = c.go()
    assert c.data == a.go() + b.go()
    f = cell_func(lambda a, b: a+b, unitemized = ['a', 'b'], uncalled = ['a', 'b'])
    c = cell(f, a = a, b = b)
    c = c.go()
    assert c.data == a + b
    f = cell_func(lambda a, b: a+b)
    c = cell(f, a = a, b = b)
    c = c.go()
    assert c.data == a.go().data + b.go().data
    f = cell_func(lambda a, b: a+b, uncalled = ['a', 'b'])
    c = cell(f, a = a, b = b)
    c = c.go()
    assert c.data == a.a + b.b


def test_cell_func_relabel():
    a = cell(passthru, data = 1, a = dict(b = 3), c = [1,2,3])
    res = cell_func(add_, a = 'a.b')(a, 1)
    assert res[0] == 4 # should pull a['a']['b'] from a
    res = cell_func(add_)(a, 1)
    assert res[0] == 2 # should pull data 
    res = cell_func(add_, a = ['c', 1])(a, 1)
    assert res[0] == 3 #should pick the '2' from c
    res = cell_func(add_, a = ['c', 1])(a, 0)
    assert res[0] == 2 #should pick the '1' from c

def test_cell_output():
    c = cell()
    assert cell_output(c) == ['data']
    function = lambda : dict(a = 1, b = 2)
    function.output = ['a','b']
    c.function = function
    assert cell_output(c) == ['a', 'b']
    c.function = lambda a: a
    assert cell_output(c) == ['data']
    
    
    
def test_cell_item():
    d = dict(a = 1)
    assert cell_item(d) == d
    d = cell(a = 1)
    with pytest.raises(KeyError):
        assert cell_item(d)

    d = cell(a = 1, data = 2)
    assert cell_item(d) == 2
    assert cell_item(d, 'whatever you put here') == 2

    d.output = ['data', 'b']
    assert cell_item(d, 'crap') == 2
    assert cell_item(d) == 2

    d.output = ['b', 'data']
    with pytest.raises(KeyError):
        cell_item(d, 'crap')
    with pytest.raises(KeyError):
        cell_item(d, 'b')
    with pytest.raises(KeyError):
        cell_item(d)

    d.output = ['data', 'a']
    assert cell_item(d, 'crap') == 2
    assert cell_item(d) == 2
    assert cell_item(d, 'a') == 1
    
    
def test_cell_init():
    c = cell(a = 1, b = 2)
    assert cell(c) == c
    c = cell(lambda a, b: a+ b, a = 1, b = 2)
    assert cell(c) == c    
    d = dict(c)
    assert cell(d, x = 5) == c + dict(x = 5)

    assert c().data == 3
    assert cell_item(c()) == 3    
    with pytest.raises(KeyError):
        cell_item(c)


def test_cell_item_tree():
    c = cell(a = dict(b = 1), output = ['a.b'])
    assert cell_item(c) == 1
    c = cell(a = 1, output = 'a')
    assert c._output == ['a']
    assert not c.run()
    c = cell(a = 1, output = ['a'])
    assert c._output == ['a']
    assert not c.run()
    assert c.__repr__() == "cell\n{'a': 1, 'function': None, 'output': ['a']}"


def test_cell_go_levels():
    def f(t1 = None, t2 = None):
        _ = [i for i in range(100000)]
        return max([dt(t1), dt(t2)])
    # f = lambda t1 = None, t2 = None: max([dt(t1), dt(t2)]) # constructing a function that goes deep recursively
    a = cell(f)()
    b = cell(f, t1 = a)()
    c = cell(f, t1 = b)()
    d = cell(f, t1 = c)()
    e = cell(f, t1 = d)()
    assert not e.run() and not e.t1.run() and not e.t1.t1.run()
    e0 = e()
    assert e0.data == e.data
    e1 = e.go(1) 
    assert e1.data > e.data and e1.t1.data == e.t1.data
    e2 = e.go(2) 
    assert e2.data > e.data and e2.t1.data > e.t1.data and e2.t1.t1.data == e.t1.t1.data
    g = e.go(-1)
    assert g.data > e.data and g.t1.data > e.t1.data and g.t1.t1.data > e.t1.t1.data and g.t1.t1.t1.data > e.t1.t1.t1.data
    
    
def test_cell_inputs():
    c = cell(lambda a, b: a*b , a = 'text', b = 2)
    assert cell_inputs(c) == []
    assert cell_inputs(c, int) == [2]
    assert cell_inputs(c, str) == ['text']
    assert cell_inputs(c, (str,int)) == ['text', 2]
    
    d = cell(lambda x, y: x +y, x = [c,c,3], y = [c,4])
    assert cell_inputs(d) == [c,c,c]
    assert cell_inputs(d, int) == [3,4]

    e = cell(lambda x, y: x +y, x = dict(a = d, b = 4), y = [c,5])
    assert cell_inputs(e)  == [d,c]
    assert cell_inputs(e, int)  == [4,5]
    
def test_cell_push_and_updated():
    a = cell(passthru, data = 1, pk = 'i', i = 0)().pull()
    b = cell(passthru, data = 2, pk = 'i', i = 1)().pull()
    for i in range(2, 10):
        c = cell(add_, a = a, b = b, pk = 'i', i = i)().pull()
        a = b
        b = c
    cell_push()
    assert len(UPDATED) == 0
    assert c.data == 89
    assert get_cell(i = 9).data == 89
    c = get_cell(i = 1).go()
    assert list(UPDATED.keys()) == [(('i', 1),)]
    c.data = 3
    cell_push()
    assert get_cell(i = 9).data == 123    
    assert UPDATED == {}


        
    
    
    
