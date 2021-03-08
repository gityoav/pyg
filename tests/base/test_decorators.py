from pyg import try_none, try_back, try_zero, presync, wrapper, eq, try_list, try_true, try_false, try_nan, try_value
import numpy as np
import pytest

def test_wrapper():
    class and_add(wrapper):
        def wrapped(self, *args, **kwargs):
            return self.function(*args, **kwargs) + self.add
    
    @and_add(add = 3)
    def f(a,b):
        return a+b
    
    assert isinstance(f, dict)
    assert f['add'] == f.add == 3
    assert f(1,2) == 6

    
    class and_add_version_2(wrapper):
        def __init__(self, function = None, add = 3):
            super(and_add_version_2, self).__init__(function = function, add = add)
        def wrapped(self, *args, **kwargs):
            return self.function(*args, **kwargs) + self.add

    @and_add_version_2
    def f(a,b):
        return a+b

    assert f(1,2) == 6
        

    f = lambda a, b: a+b
    assert and_add_version_2(and_add_version_2(f)) == and_add_version_2(f)

    x = try_none(and_add_version_2(f))
    y = try_none(and_add_version_2(x))
    assert x == y        
    assert x(1, 'no can add') is None        


def test_try():
    f = lambda a: a[0]
    for t, v in [(try_none, None), (try_zero,0), (try_nan,np.nan), (try_true, True), (try_false, False), (try_list, [])]:
        assert eq(t(f)(5), v)
    assert try_value(f, verbose = True)(4) is None
    assert try_value(f, 'should log', verbose = True)(4) == 'should log'
    
    
        
def test_try_back():
    def f(a):
        return a[0]
    assert try_back(f)(5) == 5
    assert try_back(f)('hello') == 'h'
    assert try_back(f).__wrapped__ == f
    assert try_back(f).__repr__().startswith("try_back({'function':")
    
def test_fail_with_hidden_params():
    def f(_a):
        return _a
    class funny(wrapper):
        pass
    assert funny(exposed = 1)(f)(4)==4
    with pytest.raises(ValueError):
        funny(_hidden= 1)(f)
