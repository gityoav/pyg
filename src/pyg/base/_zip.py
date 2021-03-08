from pyg.base._types import is_iterable
from pyg.base._loop import len0

__all__ = ['zipper', 'lens']

def lens(*values):
    """
    measures (and enforces) a common length across all values

    :Parameters:
    ----------------
    *values : lists

    Raises
    ------
    ValueError
        if you have values with multi lengths.

    :Returns:
    -------
    int
        common length.
        
    :Example:
    --------------
    >>> assert lens() == 0
    >>> assert lens([1,2,3], [2,4,5]) == 3
    >>> assert lens([1,2,3], [2,4,5], [6]) == 3
    """
    if len0(values) == 0:
        return 0
    all_lens = [len0(value) for value in values]
    lens = set(all_lens) - {1}
    if len(lens)>1:
        raise ValueError('found multiple lengths %s '%lens)
    return list(lens)[0] if lens else 1

def zipper(*values):
    """
    a safer version of zip

    :Examples: zipper works with single values as well as full list:
    ---------------
    >>> assert list(zipper([1,2,3], 4)) == [(1, 4), (2, 4), (3, 4)]
    >>> assert list(zipper([1,2,3], [4,5,6])) == [(1, 4), (2, 5), (3, 6)]
    >>> assert list(zipper([1,2,3], [4,5,6], [7])) ==  [(1, 4, 7), (2, 5, 7), (3, 6, 7)]
    >>> assert list(zipper([1,2,3], [4,5,6], None)) ==  [(1, 4, None), (2, 5, None), (3, 6, None)]
    >>> assert list(zipper((1,2,3), np.array([4,5,6]), None)) ==  [(1, 4, None), (2, 5, None), (3, 6, None)]
    
    :Examples: zipper rejects multi-length lists
    ---------------
    >>> import pytest
    >>> with pytest.raises(ValueError):
    >>>     zipper([1,2,3], [4,5])
    

    :Parameters:
    ----------------
    *values : lists 
        values to be zipped

    :Returns:
    -------
    zipped values

    """
    values = [list(value) if isinstance(value, zip) else value if is_iterable(value) else [value] for value in values]
    n = lens(*values)
    values = [value * n if len(value) == 1 else value for value in values]
    return zip(*values)

