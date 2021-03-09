from pyg.base._types import is_str

__all__ = ['is_tree', 'tree_to_table']

def _update(dicts, rhs):
    for d in dicts:
        d.update(rhs)
    return dicts


def tree_to_table(tree, pattern):
    """
    The best way to understand is to give an example:
        
    :Examples:
    ---------------
    >>> school = dict(pupils = dict(id1 = dict(name = 'james', surname = 'maxwell', gender = 'm'),
                              id2 = dict(name = 'adam', surname = 'smith', gender = 'm'),
                              id3 = dict(name = 'michell', surname = 'obama', gender = 'f'),
                              ),
                teachers = dict(math = dict(name = 'albert', surname = 'einstein', grade = 3),
                                english = dict(name = 'william', surname = 'shakespeare', grade = 3),
                                physics = dict(name = 'richard', surname = 'feyman', grade = 4)
                                ))
    
    Suppose we wanted to identify all male students:
        
    >>> res = tree_to_table(school, 'pupils/%id/gender/m')
    >>> assert res == [dict(id = 'id1'), dict(id = 'id2')]

    or grades:
    
    >>> res = tree_to_table(school, 'teachers/%subject/grade/%grade')
    >>> assert res == [{'grade': 3, 'subject': 'math'},
                         {'grade': 3, 'subject': 'english'},
                         {'grade': 4, 'subject': 'physics'}]
        
    :Parameters:
    ----------------
    tree : tree (dict of dicts)
        tree is a yaml-like structure
    pattern : string
        The pattern whose instances we wish to find in tree

    :Returns:
    -------
    list of dicts


    """
    match = pattern.split('/') if is_str(pattern) else pattern
    if len(match) == 0:
        return [{}]
    key = match[0]
    if isinstance(tree, dict):
        t = dict(tree)
        if key.startswith('%'):
            return sum([_update(tree_to_table(t[k], match[1:]), {key[1:]: k}) for k in t], [])
        else:
            if key in t:
                return tree_to_table(t[key], match[1:])
            else:
                return []
    else:
        if len(match)>1:
            return []
        elif key.startswith('%'):
            return [{key[1:] : tree}]
        elif key == tree:
            return [{}]
        else:
            return []

def is_tree(pattern):
    if not is_str(pattern):
        return False
    match = pattern.split('/')
    if not max([m.startswith('%') for m in match]):
        return False
    if max(['%' in m[1:] for m in match]):
        return False
    return True
    
