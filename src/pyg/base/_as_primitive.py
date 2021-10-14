from pyg.base._types import is_int, is_float, is_str, is_date, is_bool
from pyg.base._dates import dt
from pyg.base._loop import loop
from enum import Enum

__all__ = ['as_primitive', ]

@loop(list, tuple)
def _as_primitive(value):
    if is_bool(value):
        return True if value else False
    elif is_int(value):
        return int(value)
    elif is_float(value):
        return float(value)
    elif is_date(value):
        return dt(value)
    elif value is None or is_str(value):
        return value
    elif isinstance(value, Enum):
        return _as_primitive(value.value)
    else:
        return value

def as_primitive(value):
    return _as_primitive(value)

