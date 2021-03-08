from functools import reduce
from pyg.base._decorators import wrapper
from pyg.base._types import is_str

__all__ = ['reducer', 'reducing']

def reducer(function, sequence, default = None):
    """
    reduce adds stuff to zero by defaults. This is not needed.

    :Parameters:
    ----------------
    function : callable
        binary function.
    sequence : iterable
        list of inputs to be applied iteratively to reduce.
    default : TYPE, optional
        A default value to be returned with an empty sequence

    :Example:
    -------
    >>> from operator import add, mul
    >>> from functools import reduce
    >>> import pytest
    
    >>> assert reducer(add, [1,2,3,4]) == 10
    >>> assert reducer(mul, [1,2,3,4]) == 24
    >>> assert reducer(add, [1]) == 1
    
    >>> assert reducer(add, []) is None
    >>> with pytest.raises(TypeError):
    >>>     reduce(add, [])
    """
    sequence = list(sequence)
    if len(sequence) == 0:
        return default
    elif len(sequence) == 1:
        return sequence[0]
    else:
        return reduce(function, sequence[1:], sequence[0])

    
class reducing(wrapper):
    """
    Makes a bivariate-function being able to act on a sequence of elements using reduction
    
    :Example:
    -------
    >>> from operator import mul
    >>> assert reducing(mul)([1,2,3,4]) == 24    
    >>> assert reducing(mul)(6,4) == 24    

    Since a.join(b).join(c).join(d) is also very common, we provide a simple interface for that:

    :Example: chaining
    -----------------------------
    >>> assert reducing('__add__')([1,2,3,4]) == 10
    >>> assert reducing('__add__')(6,4) == 10
    
    d = dictable(a = [1,2,3,5,4])
    reducing('inc')(d, dict(a=1))
    
    """
    def wrapped(self, lhs, rhs = None, default = None, **kwargs):
        if is_str(self.function):
            func = lambda lhs, rhs: getattr(lhs, self.function)(rhs, **kwargs)
        else:
            if len(kwargs):
                func = lambda lhs, rhs: self.function(lhs, rhs, **kwargs)
            else:
                func = self.function
        if rhs is None:
            return reducer(func, lhs, default)
        else:
            return func(lhs, rhs)


