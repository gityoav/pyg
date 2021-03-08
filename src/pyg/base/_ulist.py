from pyg.base._types import is_int, is_list

__all__ = ['rng', 'ulist']

def rng(*args):
    return list(range(*[a if is_int(a) else len(a) for a in args]))

class ulist(list):
    """
    A list whose members are unique. It has +/- operations overloaded while also supporting set opeations &/|
        
    :Example:
    ---------    
    >>> assert ulist([1,3,2,1]) == list([1,3,2])
    
    :Example: addition adds element(s)
    --------------------------------
    >>> assert ulist([1,3,2,1]) + 4  == list([1,3,2,4])
    >>> assert ulist([1,3,2,1]) + [4,1] == list([1,3,2,4])
    >>> assert ulist([1,3,2,1]) + [4,1,5] == list([1,3,2,4,5])

    :Example: subtraction removes element(s)
    -------------------------------------------
    >>> assert ulist([1,3,2,1]) - 1 == [3,2]
    >>> assert ulist([1,3,2,1]) - [1,3,4] == [2]

    :Example: set operations
    -------------------------------------------
    >>> assert ulist([1,3,2,1]) & 1 == [1]
    >>> assert ulist([1,3,2,1]) & [1,3,4] == [1,3]

    >>> assert ulist([1,3,2,1]) | 1 == [1,3,2]
    >>> assert ulist([1,3,2,1]) | 4 == [1,3,2,4]
    >>> assert ulist([1,3,2,1]) | [1,3,4] == [1,3,2,4]

    """
    def __init__(self, *args, unique = False):
        if unique:
            super(ulist, self).__init__(*args)
        else:
            orig = list(*args)
            super(ulist, self).__init__([v for _, v in sorted([(orig.index(u), u) for u in set(orig)])])

    def __add__(self, other):
        if is_list(other):
            return type(self)(super(ulist, self).__add__(other))
        elif other in self:
            return self.copy()
        else:
            return type(self)(super(ulist, self).__add__([other]))

    def copy(self):
        return type(self)(self, unique = True) 
    
    def __and__(self, other):
        if is_list(other):
            return type(self)([o for o in self if o in other])
        else:
            return type(self)([other], unique = True) if other in self else type(self)()
    
    __or__ = __add__
    
    def __sub__(self, other):
        if is_list(other):
            return type(self)([o for o in self if o not in other])
        elif other not in self:
            return self.copy()
        else:
            return type(self)([o for o in self if o not in [other]])
                
