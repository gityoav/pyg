import inspect

__all__ = ['getargspec', 'getargs', 'getcallargs', 'call_with_callargs', 'argspec_defaults', 'argspec_required', 'argspec_update', 'kwargs2args', 'argspec_add']


def getargspec(function):
    """
    Extends inspect.getfullargspec to allow us to decorate functions with a signature.
    
    :Parameters:
    ----------------
    function : callable
        function for which we want to know argspec.

    :Returns:
    -------
    inspect.FullArgSpec

    """
    if hasattr(function, 'fullargspec'):
        return function.fullargspec
    else:
        return inspect.getfullargspec(function)


def argspec_add(fullargspec, **update):
    """
    adds new args with default values at the end of the existing args

    :Parameters:
    ----------
    fullargspec : FullArgSpec
        DESCRIPTION.
    **update : dict
        parameter names with their default values.

    :Returns:
    -------
    FullArgSpec

    :Example:
    ---------
    >>> f = lambda b : b
    >>> argspec = getargspec(f)
    >>> updated = argspec_add(argspec, axis = 0)
    >>> assert updated.args == ['b', 'axis'] and updated.defaults == (0,)

    >>> f = lambda b, axis : None ## axis already exists without a default
    >>> argspec = getargspec(f)
    >>> updated = argspec_add(argspec, axis = 0)
    >>> assert updated == argspec

    >>> f = lambda b, axis =1 : None ## axis already exists with a different default
    >>> argspec = getargspec(f)
    >>> updated = argspec_add(argspec, axis = 0)
    >>> assert updated == argspec


    """
    new_keys = [key for key in update if key not in fullargspec.args]
    if len(new_keys) == 0:
        return fullargspec
    new_defaults = tuple([update[key] for key in new_keys])
    args = fullargspec.args + new_keys
    defaults = (fullargspec.defaults or ()) + new_defaults 
    res = inspect.FullArgSpec(args = args, varargs = fullargspec.varargs, varkw = fullargspec.varkw, defaults = defaults, kwonlyargs = fullargspec.kwonlyargs, 
                               kwonlydefaults = fullargspec.kwonlydefaults, annotations = fullargspec.annotations)
    return res

def getargs(function, n = 0):
    """

    :Parameters:
    ----------------
    function : callable
        The function for  which we want the args
    n : int optional
        get the name of the args after allowing for first n args to be set by *args. 
        The default is 0.

    :Returns:
    -------
    None or a list of args

    """
    try:
        argspec = getargspec(function)
        if argspec.varargs or n == 0:
            return argspec.args
        else:
            return argspec.args[n:]
    except Exception:
        return []

def getcallargs(function, *args, **kwargs):
    """
    replicates inspect.getcallargs with support to functions within decorators
    
    :Example:
    --------------
    >>> from pyg import *; import inspect
    >>> function = lambda a, b, *myargs, **mykwargs: 1 
    >>> args = (1,2,3,4,5); kwargs = dict(c = 6, d = 7)
    >>> assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == {'a': 1, 'b': 2, 'myargs': (3, 4, 5), 'mykwargs': {'c': 6, 'd': 7}} 
    
    >>> function = lambda a: a + 1
    >>> args = (); kwargs = dict(a=1)
    >>> assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1)

    >>> function = lambda a, b = 1: 1
    >>> args = (); kwargs = dict(a=1)
    >>> assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 1)
    >>> args = (); kwargs = dict(a=1, b = 2)
    >>> assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 2)
    >>> args = (1,); kwargs = {}
    >>> assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 1)
    >>> args = (1,2); kwargs = {}
    >>> assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 2)
    >>> args = (1,); kwargs = {'b' : 2}
    >>> assert getcallargs(function, *args, **kwargs) == inspect.getcallargs(function, *args, **kwargs) == dict(a = 1, b = 2)
    """
    spec = getargspec(function)
    arg_names = [] if spec.args is None else spec.args
    res = argspec_defaults(function)
    args2kwargs = dict(zip(arg_names, args))
    duplicates = set(args2kwargs) & set(kwargs)
    if len(duplicates):
        raise ValueError('got multiple values for arguments %s'%duplicates)
    res.update(args2kwargs)
    varargs = args[len(arg_names):]
    if spec.varargs is None:
        if len(varargs)>0:
            raise ValueError('The following varargs %s have no keywords'%varargs)
    else:
        res[spec.varargs] = varargs
    if spec.varkw is not None:
        varkw = {key: value for key, value in kwargs.items() if key not in arg_names}
        res[spec.varkw] = varkw
        res.update({key: value for key, value in kwargs.items() if key in arg_names})
    else:
        res.update(kwargs)
    return res

def call_with_callargs(function, callargs):
    """
    replicates inspect.getcallargs with support to functions within decorators
    
    :Example:
    --------------
    >>> function = lambda a, b, *args, **kwargs: 1+b+len(args)+10*len(kwargs)
    >>> args = (1,2,3,4,5); kwargs = dict(c = 6, d = 7)
    >>> assert function(*args, **kwargs) == 26
    >>> callargs = getcallargs(function, *args, **kwargs)
    >>> assert call_with_callargs(function, callargs) == 26
    """    
    if not callable(function):
        return function
    spec = getargspec(function)
    arg_names = [] if spec.args is None else spec.args
    c = dict(callargs)
    varargs = c.pop(spec.varargs) if spec.varargs else []
    varkw = c.pop(spec.varkw) if spec.varkw else {}
    defs = spec.defaults if spec.defaults else []
    params = dict(zip(arg_names[-len(defs):], defs))
    params.update(c)
    args = [params[arg] for arg in arg_names if arg in params] + list(varargs)
    return function(*args, **varkw)
        
        
    
    
def argspec_defaults(function):
    """
    :Returns: the function defaults as a dict rather than using the inspect structure
    
    :Example:
    --------------
    >>> f = lambda a, b = 1: a+b
    >>> assert argspec_defaults(f) == dict(b=1)

    >>> g = partial(f, b = 2)
    >>> assert argspec_defaults(g) == dict(b=2)

    
    :Parameters:
    ----------------
    function : callable

    :Returns:
    -------
    defaults as a dict.

    """
    argspec = getargspec(function)
    if argspec.defaults is None:
        res = {}
    else:
        res = dict(zip(argspec.args[-len(argspec.defaults):], argspec.defaults))
    if argspec.kwonlydefaults is not None:
        res.update(argspec.kwonlydefaults)
    return res

def argspec_required(function):
    """

    :Parameters:
    ----------------
    function : callable

    :Returns:
    -------
    list
        parameters that *must* be provided in order to run the function

    """
    
    
    argspec = getargspec(function)
    assert argspec.varargs is None, 'function cannot have unnamed *args and have a required args'
    return argspec.args if argspec.defaults is None else argspec.args[:-len(argspec.defaults)]  


def argspec_update(argspec, **kwargs):
    """
    generic function to create new FullArgSpec (python 3) or normal ArgSpec (python 2)

    :Parameters:
    ----------
    argspec : FullArgSpec
        The argspec of the dunction
    **kwargs : TYPE
        updates

    :Returns:
    -------
    FullArgSpec
    
    :Example:
    ---------
    >>> f = lambda a, b =1 : a + b
    >>> argspec = getargspec(f)    
    >>> assert argspec_update(argspec, args = ['a', 'b', 'c']) == inspect.FullArgSpec(**{'annotations': {},
                                                                                 'args': ['a', 'b', 'c'],
                                                                                 'defaults': (1,),
                                                                                 'kwonlyargs': [],
                                                                                 'kwonlydefaults': None,
                                                                                 'varargs': None,
                                                                                 'varkw': None})
    """
    tp = type(argspec)
    params = {key : getattr(argspec, key) for key in dir(tp)  if not key.startswith('_') and key not in ('count', 'index') }
    params.update(kwargs)
    return tp(**params)


def kwargs2args(function, args, kwargs):
    """
    converts a list of paramters that were provided as kwargs, into args

    :Example:
    -------
    >>> assert kwargs2args(lambda a, b: a+b, (), dict(a = 1, b=2)) == ([1,2], {})


    :Parameters:
    ----------------
    function : callable
    args : tuple
        parameters of function.
    kwargs : dict
        key-word parameters of function.

    :Returns:
    -------
    tuple
        a pair of a function args, kwargs.

    """
    if args:
        return args, kwargs
    else:
        kwargs = dict(kwargs)
        return [kwargs.pop(a) for a in getargspec(function).args if a in kwargs], kwargs


