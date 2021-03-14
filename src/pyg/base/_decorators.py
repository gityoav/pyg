import numpy as np
from pyg.base._logger import logger
from pyg.base._inspect import getargspec, getargs
from pyg.base._dictattr import dictattr
from copy import copy
import datetime


__all__ = ['wrapper', 'try_back', 'try_nan', 'try_none', 'try_zero', 'try_false', 'try_true', 'try_list', 'timer']
_function = 'function'

class wrapper(dictattr):
    """
    A base class for all decorators. It is similar to functools.wraps but better. See below why wrapt cannot be used...
    You basically need to define the wrapped method and everything else is handled for you.
    - You can then use it either directly to decorate functions
    - Or use it to create parameterized decorators
    - the __name__, __wrapped__, __doc__ and the getargspec will all be taken care of.
    
    :Example:
    -------
    >>> class and_add(wrapper):
    >>>     def wrapped(self, *args, **kwargs):
    >>>         return self.function(*args, **kwargs) + self.add ## note that we are assuming self.add exists
    
    >>> @and_add(add = 3) ## create a decorator and decorate the function
    >>> def f(a,b):
    >>>     return a+b
        
    >>> assert f.add == 3
    >>> assert f(1,2) == 6
    

    Alternatively you can also use it this directly:
        
    >>> def f(a,b):
    >>>     return a+b
    >>> 
    >>> assert and_add(f, add = 3)(1,2) == 6

    
    :Example: Explicit parameter construction
    -----------------------------------------
    
    You can make the init more explict, also adding defaults for the parameters:

    >>> class and_add_version_2(wrapper):
    >>>     def __init__(self, function = None, add = 3):
    >>>         super(and_add, self).__init__(function = function, add = add)
    >>>     def wrapped(self, *args, **kwargs):
    >>>         return self.function(*args, **kwargs) + self.add

    >>> @and_add_version_2
    >>> def f(a,b):
    >>>     return a+b
    >>> assert f(1,2) == 6
        

    :Example: No recursion
    ------------------
    The decorator is designed to have a single instance of a specific wrapper
    
    >>> f = lambda a, b: a+b
    >>> assert and_add(and_add(f)) == and_add(f)

    This holds even for multiple levels of wrapping:

    >>> x = try_none(and_add(f))
    >>> y = try_none(and_add(x))
    >>> assert x == y        
    >>> assert x(1, 'no can add') is None        

    :Example: wrapper vs wrapt
    --------------------------
    wrapt (wrapt.readthedocs.io) is an awesome wrapping tool. If you have static library functions, none is better.
    The problem we face is that wrapt is too good in pretending the wrapped up object is the same as original function:
        
    >>> import wrapt    
    >>> def add_value(value):
    >>>     @wrapt.decorator
    >>>     def wrapper(wrapped, instance, args, kwargs):
    >>>         return wrapped(*args, **kwargs) + value
    >>>     return wrapper

    >>> def f(x,y):
    >>>     return x*y

    >>> add_three = add_value(value = 3)(f)
    >>> add_four = add_value(value = 4)(f)
    >>> assert add_four(3,4) == 16 and add_three(3,4) == 15

    >>> ## but here is the problem:
    >>> assert encode(add_three) == encode(add_four) == encode(f)
    
    So if we ever encode the function and send it across json/Mongo, the wrapping is lost and the user when she receives it cannot use it

    >>> class add_value(wrapper):
    >>>     def wrapped(self, *args, **kwargs):
    >>>         return self.function(*args, **kwargs) + self.value

    >>> add_three = add_value(value = 3)(f)
    >>> add_four = add_value(value = 4)(f)
    >>> encode(add_three)
    >>> {'value': 3, 'function': '{"py/function": "__main__.f"}', '_obj': '{"py/type": "__main__.add_value"}'}
    >>> encode(add_three)
    >>> {'value': 4, 'function': '{"py/function": "__main__.f"}', '_obj': '{"py/type": "__main__.add_value"}'}
 
    """    
    def __init__(self, function = None, *args, **kwargs):
        function = copy(function)
        if type(function) == type(self):
            kw = function._kwargs
            kw.update(kwargs)
            function = function.function
        else:
            kw = kwargs
        f = function
        while isinstance(f, wrapper):
            if type(f.function) == type(self):
                kw = f.function._kwargs
                kw.update(kwargs)
                f[_function] = f.function.function
            else:
                f = f.function

        super(wrapper, self).__init__(*args, **kw)
        self['function'] = function
        bad_keys = [key for key in kwargs if key.startswith('_')]
        if len(bad_keys):
            raise ValueError('Cannot wrap _hidden parameters %s'%bad_keys)
        for attr in ['doc']:
            attr = '__%s__'%attr
            if hasattr(self.function, attr):
                setattr(self, attr, getattr(self.function, attr))

    @property
    def __name__(self):
        return getattr(self.function, '__name__', 'pyg.base.wrapper(unnamed)')
    
    @property
    def __wrapped__(self):
        return self.function

    @property
    def fullargspec(self):
        return getargspec(self.function)

    def __repr__(self):
        return '%s(%s)'%(self.__class__.__name__, dict(self))

    __str__ = __repr__ 

    @property
    def _kwargs(self):
        return {key: value for key, value in self.items() if key!='function'}

    def __call__(self, *args, **kwargs):
        if self.function is None and len(args) == 1 and len(kwargs) == 0:
            return type(self)(function = args[0], **self._kwargs)
        else:
            return getattr(self, 'wrapped', self.function)(*args, **kwargs)


class try_back(wrapper):
    """
    wraps a function to try an evaluation. If an exception is thrown, returns first argument

    :Example:
    --------------
    >>> f = lambda a: a[0]
    >>> assert try_back(f)('hello') == 'h' and try_back(f)(5) == 5
    """
    def __init__(self, function = None):
        super(try_back, self).__init__(function = function)
    def wrapped(self, *args, **kwargs):
        try:
            return self.function(*args, **kwargs)
        except Exception:
            return args[0] if len(args)>0 else kwargs[getargs(self.function)[0]]


class try_value(wrapper):
    """
    wraps a function to try an evaluation. If an exception is thrown, returns a cached argument
    
    :Parameters:
    ------------
    function callable
        The function we want to decorate
    value: 
        If the function fails, it will return value instead. Default is None
    verbose: bool
        If set to True, the logger will warn with the error message.

    There are various convenience functions with specific values
    try_zero, try_false, try_true, try_nan and try_none will all return specific values if function fails.
    
    :Example:
    --------------
    >>> from pyg import *
    >>> f = lambda a: a[0]
    >>> assert try_none(f)(4) is None
    >>> assert try_none(f, 'failed')(4) == 'failed'

    
    """
    def __init__(self, function = None, value = None, verbose = None):
        super(try_value, self).__init__(function = function, value = value, verbose = verbose)
    def wrapped(self, *args, **kwargs):
        try: 
            return self.function(*args, **kwargs)
        except Exception as e:
            if self.verbose:
                logger.warning('WARN: %s' % e)
            return copy(self.value)

try_nan = try_value(value = np.nan)
try_zero = try_value(value = 0)
try_none = try_value
try_true = try_value(value = True)
try_false = try_value(value = False)
try_list = try_value(value = [])


def _str(value):
    """
    returns a short string:
    >>> _str([1,2,3])
    :Parameters:
    ----------------
    value : TYPE
        DESCRIPTION.

    :Returns:
    -------
    TYPE
        DESCRIPTION.

    """
    if isinstance(value, (int, str, float, bool, datetime.datetime, datetime.date)):
        return str(value) 
    elif hasattr(value, '__len__'):
        return '%s[%i]'%(type(value), len(value))
    else:
        return str(type(value))
        
_txt = 'TIMER:%r args:[%r, %r] (%i runs) took %s sec'    
class timer(wrapper):
    """
    timer is similar to timeit but rather than execution of a Python statement, 
    timer wraps a function to make it log its evaluation time before returning output
    
    :Parameters:
    ------------
    function: callable
        The function to be wraooed 
    
    n: int, optional
        Number of times the function is to be evaluated. Default is 1
    
    time: bool, optional
        If set to True, function will return the TIME it took to evaluate rather than the original function output.


    :Example:
    ---------
    >>> from pyg import *; import datetime
    >>> f = lambda a, b: a+b
    >>> evaluate_100 = timer(f, n = 100, time = True)(1,2)
    >>> evaluate_10000 = timer(f, n = 10000, time = True)(1,2)
    >>> assert evaluate_10000> evaluate_100
    >>> assert isinstance(evaluation_time, datetime.timedelta)
    """
    
    def __init__(self, function, n = 1, time = False):
        super(timer, self).__init__(function = function, n = n, time = time)

    def wrapped(self, *args, **kwargs):
        t0 = datetime.datetime.now()
        for _ in range(self.n):
            res = self.function(*args, **kwargs)
        t1 = datetime.datetime.now()
        time = t1 - t0
        logger.info(_txt%(getattr(self.function,'__name__', ''), 
                            [_str(a) for a in args],
                            ['%s=%s'%(key, _str(value)) for key, value in kwargs.items()],
                            self.n,
                            time
                            ))        
        return time if self.time else res

class kwargs_support(wrapper):
    """
    Extends a function to support **kwargs inputs
    
    :Example:
    ---------
    >>> from pyg import *
    >>> @kwargs_support
    >>> def f(a,b):
    >>>     return a+b
    
    >>> assert f(1,2, what_is_this = 3, not_used = 4, ignore_this_too = 5) == 3
    
    """
    def __init__(self, function = None):
        super(kwargs_support, self).__init__(function = function)
    @property
    def _args(self):
        return getargs(self.function)
        
    def wrapped(self, *args, **kwargs):
        _args = self._args
        kwargs = {key : value for key, value in kwargs.items() if key in _args}
        return self.function(*args, **kwargs)

        
        
        
        
        