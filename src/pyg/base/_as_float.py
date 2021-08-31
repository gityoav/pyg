from pyg.base._loop import loop
from pyg.base._types import is_str

_k = {0 : '', 3 : 'k', 6:'m', 9:'b', 12: 't', -2 : '%'}
_n = {v: 10**k for k,v in _k.items()}

__all__ = ['as_float']

@loop(list, tuple, dict)
def as_float(value):
    """
    converts a string to float

    :Parameters:
    ----------
    value : string
        number in string format.

    :Returns:
    -------
    float

    :Example:
    --------
    >>> from pyg import *
    >>> assert as_float('1.3k') == 1300
    >>> assert as_float('1.4m') == 1400000
    >>> assert as_float('100%') == 1
    >>> assert as_float('1,234') == 1234
    >>> assert as_float('-1,234k') == -1234000
    """
    if is_str(value):
        txt = value.replace(',','').replace(' ','')
        if not txt:
            return None
        if txt[-1] in _n:
            mult = _n[txt[-1]]
            txt = txt[:-1]
        else:
            mult = 1
        try:
            return mult * float(txt)
        except ValueError:
            return None
    else:
        return value

