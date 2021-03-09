from pyg.base._dictattr import dictattr
from pyg.base._decorators import kwargs_support, try_none
from pyg.base._as_list import as_list, as_tuple
from pyg.base._types import is_str
from pyg.base._inspect import getargs
from pyg.base._eq import in_
from copy import copy



__all__ = ['Dict', 'dict_invert', 'items_to_tree', 'tree_items', 'tree_keys', 'tree_values', 'tree_update', 'tree_setitem']

    
class Dict(dictattr):
    """
    Dict extends dictattr to allow access to *functions* of members
    
    :Example:
    --------------
    >>> from pyg import *
    >>> d = Dict(a = 1, b=2)
    >>> assert d[lambda a, b: a+b] == 3
    >>> assert d['a','b', lambda a,b: a+b] == [1,2,3]
    
    Dict is also callable where the key-value is used to add/update current members        
    
    :Example:
    -------
    >>> from pyg import *
    >>> d = Dict(a = 1, b=2)
    >>> assert d(c = 3) == Dict(a = 1, b = 2, c = 3)
    >>> assert d(c = lambda a,b: a+b) == Dict(a = 1, b = 2, c = 3)

    
    >>> assert d(c = 3) == Dict(a = 1, b = 2) + Dict(c = 3)
    >>> assert Dict(a = 1)(b = lambda a: a+1)(c = lambda a,b: a+b) == Dict(a = 1,b = 2,c = 3)
    """
    def __getitem__(self, value):
        if callable(value):
            return self.apply(value)
        else:
            return super(Dict, self).__getitem__(value)
    
    def apply(self, function):
        return kwargs_support(function)(**self)
    
    def __call__(self, **kwargs):
        res = self.copy()
        res.update({key : value for key, value in kwargs.items() if not callable(value)})
        callables = {key: value for key, value in kwargs.items() if callable(value)}
        while len(callables) > 1:        
            keys = set(callables.keys())        
            independent = {key: value for key, value in callables.items() if len(keys & set(getargs(value))) == 0}
            if len(independent) == 0:
                raise ValueError('circular function calling')
            else:
                for key, value in independent.items():
                    res[key] = res[value]
                callables = {key: value for key, value in callables.items() if not key in independent}
        for key, value in callables.items():
            res[key] = res[value]
        return res
    
    def __add__(self, other):
        return tree_update(self, other)
    
    def do(self, function, *keys):
        """
        applies a function(s) on multiple keys at the same time

        :Parameters:
        ----------------
        function : callable or list of callables
            function to be applied per column
        *keys : string/list of strings
            list of columns to be applied. If missing, applied to all columns

        :Returns:
        -------
        res : Dict

        :Example:
        --------------
        >>> from pyg import *
        >>> d = Dict(name = 'adam', surname = 'atkins')
        >>> assert d.do(proper) == Dict(name = 'Adam', surname = 'Atkins')

        :Example: using another key in the calculation
        -------
        >>> from pyg import *
        >>> d = Dict(a = 1, b = 5, denominator = 10)
        >>> d = d.do(lambda value, denominator: value/denominator, 'a', 'b')
        >>> assert d == Dict(a = 0.1, b = 0.5, denominator = 10)
        
        """
        res = self.copy()
        keys = as_list(keys)
        if len(keys)  == 0:
            keys = self.keys()
        for key in keys:
            for f in as_list(function):
                args = as_list(try_none(getargs)(f))
                res[key] = f(res[key], **{k : v for k, v in res.items() if k in args[1:]})
        return res


def dict_invert(d):
    """
    inverts a dict to create value -> keys

    :Example:
    --------------
    >>> d = dict(a = 1, b = 2, c = 1, d = 2, e = 4)
    >>> assert dict_invert(d) == {1: ['a', 'c'], 2: ['b', 'd'], 4: ['e']}
    
    
    If you think your data is unique:
    >>> d = dict(a = 1, b = 2, c = 3)
    >>> assert dict_invert(d).do(first) ==  {1: 'a', 2: 'b', 3: 'c'}

    :Parameters:
    ----------------
    d : dict
        a dict whose key-to-value.

    :Returns:
    -------
    dict of values to [lists]

    """
    res = Dict()
    for key, value in d.items():
        res[value] = res.get(value, []) + [key]
    return res


def _tree_types(types):
    return (dict, Dict, dictattr) if types is None else as_tuple(types)


def _tree_setitem(tree, item, base, ignore, types):
    if len(item)<2:
        raise ValueError('node item too short %s'%item)
    res = tree
    for key in item[:-2]:
        if key not in res or not isinstance(res[key], types):
            res[key] = base()
        res = res[key]
    if item[-2] in res and in_(item[-1], ignore):
        return
    else:
        res[item[-2]] = item[-1]

def tree_setitem(tree, key, value, ignore = None, types = None):
    """
    sets an item of a tree
    
    :Parameters:
    ----------
    tree : tree (dicts of dict)
    key : a dot-separated string or a tuple of values
        the branch to hang value on
    value : object
        the leaf at the end of the branch
    ignore : None or list, optional
        what values of leaf will be ignored and not overwrite existing data. The default is None.
    types : types, optional
        As we go down the tree, when do we stop and say: what we have is a leaf already?

    :Example:
    ---------
    >>> tree = dict()
    >>> tree_setitem(tree, 'a', 1)
    >>> assert tree == dict(a = 1)
    >>> tree_setitem(tree, 'b.c', 2)
    >>> assert tree == {'a': 1, 'b': {'c': 2}}
    >>> tree_setitem(tree, ('b','c','d'), 2)
    >>> tree_setitem(tree, ('b','c','e'), 3)
    >>> assert tree == {'a': 1, 'b': {'c': {'d': 2, 'e': 3}}}

    :Example: types
    ---------------
    >>> from pyg import *
    >>> tree = dict(mycell = cell(lambda a, b: a * b, b = 2, a = cell(lambda x: x**2, x = cell(lambda y: y*3))))
    >>> # We are missing y....
    >>> tree_setitem(tree, 'mycell.a.x.y', 3, types = (dict,cell)) ## drill into cell
    >>> assert tree['mycell'].a.x.y == 3
    >>> tree_setitem(tree, 'mycell.a.x.y', 1) ## stop when you hit cell
    >>> assert tree['mycell'].a.x == dict(y = 1)

    Returns
    -------
    None.

    """
    
    types = _tree_types(types)
    ignore = as_list(ignore)
    base = type(tree)
    if is_str(key):
        key = key.split('.')
    elif isinstance(key, tuple):
        key = list(key)
    _tree_setitem(tree, key + [value], base = base, ignore = ignore, types = types)

def tree_items(tree, types = None):
    """
    An extension of dict.items(), returning a list of tuples but of varying length, each a branch of a tree

    :Parameters:
    ----------------
    tree : dict of dicts
        a tree of data.
    types : dict or a list of dict-types, optional
        The types that we consider as 'branches' of the tree. Default is (dict, Dict, dictattr).

    :Returns:
    -------
    a list of tuples
        these are an extension of dict.items() and are of varying length
        
    :Example:
    --------------
    >>> school = dict(pupils = dict(id1 = dict(name = 'james', surname = 'maxwell', gender = 'm'),
                          id2 = dict(name = 'adam', surname = 'smith', gender = 'm'),
                          id3 = dict(name = 'michell', surname = 'obama', gender = 'f'),
                          ),
            teachers = dict(math = dict(name = 'albert', surname = 'einstein', grade = 3),
                            english = dict(name = 'william', surname = 'shakespeare', grade = 3),
                            physics = dict(name = 'richard', surname = 'feyman', grade = 4)
                            ))

    >>> items = tree_items(school)
    >>> items 
    
    >>> [('pupils', 'id1', 'name', 'james'),
    >>>  ('pupils', 'id1', 'surname', 'maxwell'),
    >>>  ('pupils', 'id1', 'gender', 'm'),
    >>>  ('pupils', 'id2', 'name', 'adam'),
    >>>  ('pupils', 'id2', 'surname', 'smith'),
    >>>  ('pupils', 'id2', 'gender', 'm'),
    >>>  ('pupils', 'id3', 'name', 'michell'),
    >>>  ('pupils', 'id3', 'surname', 'obama'),
    >>>  ('pupils', 'id3', 'gender', 'f'),
    >>>  ('teachers', 'math', 'name', 'albert'),
    >>>  ('teachers', 'math', 'surname', 'einstein'),
    >>>  ('teachers', 'math', 'grade', 3),
    >>>  ('teachers', 'english', 'name', 'william'),
    >>>  ('teachers', 'english', 'surname', 'shakespeare'),
    >>>  ('teachers', 'english', 'grade', 3),
    >>>  ('teachers', 'physics', 'name', 'richard'),
    >>>  ('teachers', 'physics', 'surname', 'feyman'),
    >>>  ('teachers', 'physics', 'grade', 4)]
    
    #To reverse this, we call:
        
    >>> assert items_to_tree(items) == school
    
    """
    types = (dict, Dict, dictattr) if types is None else as_list(types)
    if type(tree) in types:
        return sum([[(key,) + item for item in tree_items(tree[key], types)] for key in tree], [])
    else: # this is a leaf
        return [(tree,)]
    
def tree_keys(tree, types = None):
    """
    returns the keys (branches) of a tree as a list of of tuples

    :Example:
    ---------
    >>> tree = dict(a = 1, b = dict(c = 2, d = 3, e = dict(f = 4)))
    >>> assert tree_keys(tree) == [('a',), ('b', 'c'), ('b', 'd'), ('b', 'e', 'f')]

    :Parameters:
    ----------
    tree : tree (dict of dicts)
    types : types of dicts, optional

    """
    types = _tree_types(types)
    if type(tree) in types:
        return sum([[(key,) + item for item in tree_keys(tree[key], types)] for key in tree], [])
    else: # this is a leaf
        return [()]
    
def tree_values(tree, types = None):
    """
    returns the values (leaf) of a tree (a collection of tuples)

    :Example:
    ---------
    >>> tree = dict(a = 1, b = dict(c = 2, d = 3, e = dict(f = 4)))
    >>> assert tree_values(tree) == [1,2,3,4]

    :Parameters:
    ----------
    tree : tree (dict of dicts)
    types : types of dicts, optional

    """
    types = _tree_types(types)
    if type(tree) in types:
        return sum([[item for item in tree_values(tree[key], types)] for key in tree], [])
    else: # this is a leaf
        return [tree]


    
def items_to_tree(items, tree = None, raise_if_duplicate = True, ignore = None, types = None):
    """
    converts **items** to branches of a tree. 
    If an original **tree** is provided, hang the additional branches on the existing tree
    If **ignore** is provided as a list of values, will not overwrite branches with last value (the leaf) in these values
    
    :Example:
    --------------
    >>> items = [('cambridge', 'smith', 'economics',),
             ('cambridge', 'keynes', 'economics'), 
             ('cambridge', 'lyons',  'maths'),
             ('cambridge', 'maxwell', 'maths'),
             ('oxford', 'penrose', 'maths'),
             ]
    
    >>> tree = items_to_tree(items)
    >>> print(tree_repr(tree))
    
    >>> cambridge:
    >>>     smith:
    >>>         economics
    >>>     keynes:
    >>>         economics
    >>>     lyons:
    >>>         maths
    >>>     maxwell:
    >>>         maths
    >>> oxford:
    >>>     {'penrose': 'maths'}
    
    We can add to tree:
    
    
    :Parameters:
    ----------------
    items : list of tuples, 
        items are just like dict items, only longer, 
    tree : tree, optional
        a pre-existing tree of trees. The default is None.
    raise_if_duplicate : TYPE, optional
        DESCRIPTION. The default is True.
    ignore : list, optional
        list of values that when over-writing an existing tree, should ignore. The default is None.


    :Example: using ignore
    ----------------------
    >>> tree = dict(a = 1, b = 'keep_old_value')
    >>> update = dict(a = 'valid_new_value', b = None, c = None)
    >>> tree_update(tree, update, ignore = [None])
    >>> {'a': valid_new_value, 'b': 'keep_old_value', 'c': None}

    * a is over-ridden as the new value is valid
    * b is not over-ridden since the update b = None is considereed invalid
    * c is added as it did not exist before, even though c = None is invalid value

    :Returns:
    -------
    tree : dict of dicts

    """
    types = _tree_types(types)
    if raise_if_duplicate and len(set([tuple(node[:-1]) for node in items])) < len(items):
        raise ValueError('items are not unique and will overwrite each other')
    if tree is None:
        tree = dictattr()
    else:
        tree = copy(tree)
    base = type(tree)
    ignore = as_list(ignore)
    for item in items:
        _tree_setitem(tree, item, base, ignore, types)
    return tree

def tree_update(tree, update, types = (dict, Dict, dictattr), ignore = None):
    """
    equivalent to dict.update() except: not in-place and also updates further down the tree
    
    :Example:
    --------------
    >>> ranking = dict(cambridge = dict(trinity = 1, stjohns = 2, christ = 3), 
                oxford = dict(trinity = 1, jesus = 2, magdalene = 3))
    >>> new_ranking = dict(oxford = dict(wolfson = 3, magdalene = 4))

    >>> print(tree_repr(tree_update(ranking, new_ranking)))

    >>> cambridge:
    >>>     {'trinity': 1, 'stjohns': 2, 'christ': 3}
    >>> oxford:
    >>>     {'trinity': 1, 'jesus': 2, 'magdalene': 4, 'wolfson': 3}

    Note how values for magdalene in Oxford were overwritten even though they are further down the tree

    :Example: using ignore
    -----------------------
    >>> update = dict(a = None, b = np.nan, c = 0)
    >>> tree = dict(a = 1, b = 2, c = 3)
    >>> assert tree_update(tree, update) == update
    >>> assert tree_update(tree, update, ignore = [None]) == dict(a = 1, b = np.nan, c = 0)
    >>> assert tree_update(tree, update, ignore = [None, np.nan]) == dict(a = 1, b = 2, c = 0)
    >>> assert tree_update(tree, update, ignore = [None, np.nan, 0]) == tree

    :Parameters:
    ----------------
    tree : tree
        existing tree.
    update : tree
        new information.
    types : types, optional
        see tree_items. The default is (dict, Dict, dictattr).

    :Returns:
    -------
    tree
        updated tree.

    """
    items = tree_items(update, types)
    return items_to_tree(items, tree, ignore = ignore, types = types)