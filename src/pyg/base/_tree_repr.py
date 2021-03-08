from pyg.base._dictable import dictable

__all__ = ['tree_repr']

def tree_repr(value, offset = 0):
    """
    a cleaner representation of a tree
    
    :Example:
    >>> school = dict(pupils = dict(id1 = dict(name = 'james', surname = 'maxwell', gender = 'm'),
                      id2 = dict(name = 'adam', surname = 'smith', gender = 'm'),
                      id3 = dict(name = 'michell', surname = 'obama', gender = 'f'),
                      ),
        teachers = dict(math = dict(name = 'albert', surname = 'einstein', grade = 3),
                        english = dict(name = 'william', surname = 'shakespeare', grade = 3),
                        physics = dict(name = 'richard', surname = 'feyman', grade = 4)
                        ))

    >>> print(tree_repr(school, 4))
    pupils:
        id1:
            {'name': 'james', 'surname': 'maxwell', 'gender': 'm'}
        id2:
            {'name': 'adam', 'surname': 'smith', 'gender': 'm'}
        id3:
            {'name': 'michell', 'surname': 'obama', 'gender': 'f'}
    teachers:
        math:
            {'name': 'albert', 'surname': 'einstein', 'grade': 3}
        english:
            {'name': 'william', 'surname': 'shakespeare', 'grade': 3}
        physics:
            {'name': 'richard', 'surname': 'feyman', 'grade': 4}

    :Parameters:
    ----------------
    value : a tree
    
    offset : int, optional
        offset from the left for printing. The default is 0.

    :Returns:
    -------
    TYPE
        DESCRIPTION.

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

