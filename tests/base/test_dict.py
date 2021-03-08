from pyg import Dict, dictattr, relabel, dictable, upper, items_to_tree, dict_invert
import pytest

def test_relabel():
    assert relabel(['a', 'b', 'c'], a = 'A', b = 'B') == dict(a = 'A', b = 'B')
    assert relabel(['a', 'b', 'c'], dict(a = 'A', b = 'B')) == dict(a = 'A', b = 'B')
    assert relabel(['a', 'b', 'c'], 'x_') == dict(a = 'x_a', b = 'x_b', c = 'x_c')
    assert relabel(['a', 'b', 'c'], '_x') == dict(a = 'a_x', b = 'b_x', c = 'c_x')
    assert relabel('a', lambda v: v * 2) == dict(a = 'aa')
    assert relabel(['a', 'b'], lambda v: v * 2, b = 'other') == dict(a = 'aa', b = 'other')
    assert relabel(['a', 'b', 'c'], 'A', 'B', 'C') == dict(a = 'A', b = 'B', c = 'C')
    assert relabel(['a', 'b', 'c'], ['A', 'B', 'C']) == dict(a = 'A', b = 'B', c = 'C')

def test_dict_raises_AttributeError_for_missing_attribute_so_getattr_works():
    for d in [Dict, dictattr, dictable]:
        with pytest.raises(AttributeError):
            d(a = 1).b
    

def test_dict_relabel():
    for d in [Dict, dictattr, dictable]:
        pass
        self = d(a = 1, b = 2, c = 3)
        assert self.relabel(a = 'd') == d(d = 1, b = 2, c = 3)
        assert self.relabel(dict(a='d')) == d(d = 1, b = 2, c = 3)
        assert self.relabel(upper) == d(A = 1, B = 2, C = 3)
        assert self.relabel(['x', 'y', 'z']) == d(x = 1, y = 2, z = 3)
        assert self.relabel('pre_') == d(pre_a = 1, pre_b = 2, pre_c = 3)

def test_dict_tree_getitem():
    for tp in [Dict, dictattr]:
        d = tp(a = 1, b = tp(c = 2, d = 3, e = tp(f = 4, g = 5)))
        assert d['a'] == 1
        assert d['b'] == tp(c = 2, d = 3, e = tp(f = 4, g = 5))
        assert d['b.c'] == 2
        assert d['b.e.f'] == 4
        del d['b.e.f']
        assert d['b.e'] == tp(g = 5)
        with pytest.raises(KeyError):
            d['b.other']
        d = tp({'a.b' : 1, 'a' : {'b' : 2}})
        assert d['a.b'] == 1
        assert d['a']['b'] == 2




def test_dict__init__and_call_():
    assert dict(Dict(a = 1, b = 2, c = 3)) == dict(a = 1, b = 2, c = 3)
    assert dict(Dict(a = 1)(b = 2, c = 3)) == dict(a = 1, b = 2, c = 3)
    assert dict(Dict(a = 1)(b = 2, c = lambda a, b: a+b)) == dict(a = 1, b = 2, c = 3)
    assert Dict(a = 1)(e = lambda d: d**2, d = lambda a, b, c: a+b+c, c = lambda a, b: a+b, b=2) ==  Dict(a = 1)(b=2)(c = lambda a, b: a+b)(d = lambda a, b, c: a+b+c)(e = lambda d: d**2) 

def test_dict__circular():
    d = Dict(a = 1, b = 2)(b = lambda a, b: a +b) ## That's OK once
    assert d.b == 3
    with pytest.raises(ValueError): ## We do not allow that though
        Dict(a = 1)(b = lambda a, c: a+c, c = lambda d: d**2, d = lambda a,b: a+b)


def test_dict__add__():
    d = Dict(a = 1, b = 2)
    assert d + dict(b = 3, c = 4) == Dict(a = 1, b = 3, c = 4)
    assert Dict(a = 1, b = 2) + dict(b = dict(d = 5), c = 4) == Dict(a = 1, b = dict(d = 5), c = 4)
    assert Dict(a = 1, b = dict(e = 0, d = 3)) + dict(b = dict(d = 5), c = 4) == Dict(a = 1, b = dict(e = 0, d = 5), c = 4)    
    tree = Dict(a = 1, b = dict(e = 0, d = 3))
    update =  dict(b = dict(d = 5), c = 4)
    assert tree+update == Dict(a = 1, b = dict(e = 0, d = 5), c = 4)


def test_dict__getitem__():
    d = Dict(a = 1, b = 2 , c = 3)
    assert d['a', 'b'] == [1,2]
    assert d[('a', 'b')] == [1,2]


def test_dict__getitem__callable_dict():
    d = Dict(a = 1, b = 2 , c = 3)
    class Test(dict):
        def __call__(self, a, b):
            return a + b + self['c']
    assert d[Test(c = 5)] == 8 ## Test is callable so that is what gets called


def test_dict_do():
    d = Dict(a = 1.1, b = 2.2 , c = 3.3)
    assert d.do(int) == Dict(a = 1, b = 2, c = 3)    
    assert d.do([int, lambda x: x**2]) == Dict(a = 1, b = 4, c= 9)
    assert d.do([int, lambda x: x**2], 'a', 'b') == Dict(a = 1, b = 4, c = 3.3)
    assert d.do([int, lambda x: x**2], ['a', 'b']) == Dict(a = 1, b = 4, c = 3.3)


def test_items_to_trees():
    items = [('a', 'b', 1), ('a', 'c', 2), ('b', 3), ('c', 'd', 'e', 'f', 4)]
    assert items_to_tree(items) == {'a': {'b': 1, 'c': 2}, 'b': 3, 'c': {'d': {'e': {'f': 4}}}}
    with pytest.raises(ValueError):
        items_to_tree([('a')])
        

def test_dictattr_delete():
    for tp in (dictattr, Dict):
        d = tp(a = tp(b = 2, d = 4), c = 3)
        del d['a', 'b']
        assert d == tp(a = tp(d = 4), c = 3)
        d = tp(a = tp(b = 2, d = 4), c = 3, b = 5)
        del d[['a', 'b']]
        assert d == tp(c = 3)

def test_dictattr__sub__branch():
    for tp in (dictattr, Dict):
        d = tp(a = tp(b = 2, d = 4), c = 3)
        assert d - ('a', 'b') == tp(a = tp(d = 4), c = 3)
        

def test_dict_invert():
    d = dict(a = 1, b = 2, c = 1, d = 3, e = 2)
    i = dict_invert(d) 
    assert type(i) == Dict    
    assert i == {1: ['a', 'c'], 2: ['b', 'e'], 3: ['d']}
    
    
def test_items_to_trees_raise_if_duplicate():
    items = [('a', 1), ('a', 2)]
    with pytest.raises(ValueError):
        items_to_tree(items)    
    assert items_to_tree(items, raise_if_duplicate=False) == dict(a = 2)
    
    
