from pyg.base._dictable import dictable
from pyg.base._dictattr import dictattr 
from pyg.base._types import is_str
from pyg.base._tree import tree_to_table, is_tree
from pyg.base._dict import Dict, tree_items, tree_setitem, tree_keys, tree_values, items_to_tree

__all__ = ['tree_repr']

def tree_repr(value, offset = 0):
    """
    a cleaner representation of a tree
    
    :Example:
    ---------
    >>> school = dict(pupils = dict(id1 = dict(name = 'james', surname = 'maxwell', gender = 'm'),
    >>>                   id2 = dict(name = 'adam', surname = 'smith', gender = 'm'),
    >>>                   id3 = dict(name = 'michell', surname = 'obama', gender = 'f'),
    >>>                   ),
    >>>     teachers = dict(math = dict(name = 'albert', surname = 'einstein', grade = 3),
    >>>                     english = dict(name = 'william', surname = 'shakespeare', grade = 3),
    >>>                     physics = dict(name = 'richard', surname = 'feyman', grade = 4)
    >>>                     ))

    >>> print(tree_repr(school, 4))
    >>> pupils:
    >>>     id1:
    >>>         {'name': 'james', 'surname': 'maxwell', 'gender': 'm'}
    >>>     id2:
    >>>         {'name': 'adam', 'surname': 'smith', 'gender': 'm'}
    >>>     id3:
    >>>         {'name': 'michell', 'surname': 'obama', 'gender': 'f'}
    >>> teachers:
    >>>     math:
    >>>         {'name': 'albert', 'surname': 'einstein', 'grade': 3}
    >>>     english:
    >>>         {'name': 'william', 'surname': 'shakespeare', 'grade': 3}
    >>>     physics:
    >>>         {'name': 'richard', 'surname': 'feyman', 'grade': 4}

    :Parameters:
    ----------------
    value : a tree
    
    offset : int, optional
        offset from the left for printing. The default is 0.

    :Returns:
    -------
    string
        a tree-like string representation of a dict-of-dicts.

    """
    o = ' ' * offset
    res = str(value)
    if isinstance(value, dict) and not isinstance(value, dictable) and len(value) and len(res) > 80:
        if type(value) == dict:
            res = '\n'.join(['%s:\n%s'%(k, tree_repr(v, 4)) for k, v in value.items()])
        else:
            res = '\n'.join([value.__class__.__name__] + ['%s:\n%s'%(k, tree_repr(v, 4)) for k, v in value.items()])
    elif isinstance(value, list) and len(value) and len(res)>80:
        res = '\n'.join([tree_repr(v, 0) for v in value])
    return o + res.replace('\n', '\n' + o)


            
            
    