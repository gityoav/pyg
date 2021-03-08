from pyg.base._inspect import call_with_callargs, getcallargs, getargspec, kwargs2args, argspec_add, argspec_required, getargs, argspec_defaults, argspec_update
import inspect
import pytest

def test_call_with_callargs():
    function = lambda a, b, *args, **kwargs: 1+b+len(args)+10*len(kwargs) 
    args = (1,2,3,4,5); kwargs = dict(c = 6, d = 7) 
    assert function(*args, **kwargs) == 26 
    callargs = getcallargs(function, *args, **kwargs) 
    assert call_with_callargs(function, callargs) == 26 # -*- coding: utf-8 -*-

def test_getcallargs():
    function = lambda a, b, *args, **kwargs: 1  
    args = (1,2,3,4,5); kwargs = dict(c = 6, d = 7) 
    assert inspect.getcallargs(function, *args, **kwargs) == getcallargs(function, *args, **kwargs) 
    
    function = lambda a,b: a +b 
    args = (1,); kwargs = dict(a = 2)
    with pytest.raises(ValueError):
        getcallargs(function, *args, **kwargs) 

    args = (1,2,3); kwargs = dict()
    with pytest.raises(ValueError):
        getcallargs(function, *args, **kwargs) 

    function = lambda a, b, *varargs, **kwargs: 5
    args = (1,2,3,4); kwargs = dict(c = 3)
    assert getcallargs(function, *args, **kwargs)  == inspect.getcallargs(function, *args, **kwargs) 

    assert call_with_callargs('not a function', 'random call args') == 'not a function'


def test_callargs_more():    
    function = lambda a: a + 1
    args = (); kwargs = dict(a=1)
    assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1)

    function = lambda a, b = 1: a + b
    args = (); kwargs = dict(a=1)
    assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 1)
    assert call_with_callargs(function, getcallargs(function, *args, **kwargs)) == function(*args, **kwargs)
    args = (); kwargs = dict(a=1, b = 2)
    assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 2)
    assert call_with_callargs(function, getcallargs(function, *args, **kwargs)) == function(*args, **kwargs)
    args = (1,); kwargs = {}
    assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 1)
    assert call_with_callargs(function, getcallargs(function, *args, **kwargs)) == function(*args, **kwargs)
    args = (1,2); kwargs = {}
    assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 2)
    assert call_with_callargs(function, getcallargs(function, *args, **kwargs)) == function(*args, **kwargs)
    args = (1,); kwargs = {'b' : 2}
    assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 2)
    assert call_with_callargs(function, getcallargs(function, *args, **kwargs)) == function(*args, **kwargs)


def test_argspec_required():
    function = lambda a, b, *args: 5
    with pytest.raises(AssertionError):
        argspec_required(function)
    function = lambda a, b: 5
    assert argspec_required(function) == ['a', 'b']
    function = lambda a, b=1: 5
    assert argspec_required(function) == ['a']
    
    
def test_getargs():
    function = lambda a, b, c: 1
    assert getargs(function, 1) == ['b', 'c']
    assert getargs(function) == ['a', 'b', 'c']
    assert getargs(function, 4) == []

    function = lambda a, b, c, *args: 1
    assert getargs(function, 1) == ['a', 'b', 'c']


def test_argspec_defaults():
    assert argspec_defaults(lambda a: None) == {}
    assert argspec_defaults(lambda a, b = 2: None) == dict(b=2)
    assert argspec_defaults(lambda a, b = 2, c = 3, *args, **kwargs: None) == dict(b=2, c = 3)


def test_argspec_update():
    f = lambda a, b =1 : a + b
    argspec = getargspec(f)    
    assert argspec_update(argspec, args = ['a', 'b', 'c']) == inspect.FullArgSpec(**{'annotations': {},
                                                                                 'args': ['a', 'b', 'c'],
                                                                                 'defaults': (1,),
                                                                                 'kwonlyargs': [],
                                                                                 'kwonlydefaults': None,
                                                                                 'varargs': None,
                                                                                 'varkw': None})

def test_kwargs2args():
    assert kwargs2args(lambda a, b: a+b, (), dict(a = 1, b=2)) == ([1,2], {}) 
    assert kwargs2args(lambda a, b: a+b, (1,), dict(b=2)) == ((1,), {'b': 2})


def test_argspec_add():
    f = lambda b : b
    argspec = getargspec(f)
    updated = argspec_add(argspec, axis = 0)
    assert updated.args == ['b', 'axis'] and updated.defaults == (0,)

    f = lambda b, axis : None ## axis already exists without a default
    argspec = getargspec(f)
    updated = argspec_add(argspec, axis = 0)
    assert updated == argspec

    f = lambda b, axis =1 : None ## axis already exists with a different default
    argspec = getargspec(f)
    updated = argspec_add(argspec, axis = 0)
    assert updated == argspec
