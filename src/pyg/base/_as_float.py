from pyg.base._loop import loop
from pyg.base._types import is_str

_k = {0 : '', 3 : 'k', 6:'m', 9:'b', 12: 't'}
_n = {v: 10**k for k,v in _k.items()}

__all__ = ['as_float']

@loop(list, tuple, dict)
def as_float(value):
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

