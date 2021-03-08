import jsonpickle as jp
import re
from pyg.base._types import is_int, is_float, is_str, is_date, is_bool, is_pd, is_arr
from pyg.base._dates import dt, iso
from pyg.base._decorators import try_back
from pyg.base._logger import logger
from pyg.base._loop import loop
import pickle
import datetime
from functools import partial
from enum import Enum
from bson.binary import Binary
from bson.objectid import ObjectId
import numpy as np


_obj = '_obj'
_data = 'data'
iso_quote = re.compile('^"[0-9]{4}-[0-9]{2}-[0-9]{2}T')

__all__ = ['encode', 'decode', 'pd2bson', 'bson2pd', 'as_primitive', 'bson2np']

@try_back
def decode_str(value):
    """
    A safer version of jp.decode

    :Parameters:
    ----------------
    value : str
        string to be decoded.

    :Returns:
    -------
    object
        value decoded or original value if failed.

    """
    res = jp.decode(value)
    if res is None and value!='null':
        logger.warning('could not decode value: %s'%value)
        return value
    else:
        return res



@loop(list, tuple)
def _decode(value, date = None):
    if is_str(value):
        if value.startswith('{'):
            value = decode_str(value)
            if not isinstance(value, str):
                value = _decode(value, date)
            return value
        elif value == 'null':
            return None
        if date in (None, False):
            return value
        elif date == 'iso' or date is True:
            if iso.search(value) is not None:
                return datetime.datetime.fromisoformat(value) 
            elif iso_quote.search(value) is not None:
                return datetime.datetime.fromisoformat(value.replace('"', ''))
            else:
                return value
        else:
            return value if date.search(value) is None else dt(value)
    elif isinstance(value, dict):
        res = type(value)(**{_decode(k, date) : _decode(v, date) for k, v in value.items()})
        return res.pop(_obj)(**res) if _obj in res.keys() else res
    else:
        return value
    
def decode(value, date = None):
    """
    decodes a string or an object dict 

    :Parameters:
    -------------
    value : str or dict
        usually a json
    date : None, bool or a regex expression, optional
        date format to be decoded
        
    :Returns:
    -------
    obj
        the json decoded.
    
    :Examples:
    --------------
    >>> from pyg import *
    >>> class temp(dict):
    >>>    pass
    
    >>> orig = temp(a = 1, b = dt(0))
    >>> encoded = encode(orig)
    >>> assert eq(decode(encoded), orig) # type matching too...
    
    """
    return _decode(value, date = date)

loads = partial(decode, date = True)

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

@loop(list, tuple)
def _encode(value):
    if hasattr(value, '_encode') and not isinstance(value, type):
        res = value._encode
        if not isinstance(res, str):
            res = res()
        return res
    if is_bool(value):
        return True if value else False
    elif is_int(value):
        return int(value)
    elif is_float(value):
        return float(value)
    elif is_date(value):
        return value if isinstance(value, datetime.datetime) else dt(value) 
    elif value is None or is_str(value):
        return value
    elif isinstance(value, Enum):
        return _as_primitive(value.value)
    elif isinstance(value, ObjectId):
         return value
    elif isinstance(value, dict):
        res = {k : _encode(v) for k, v in value.items()}
        if _obj not in res and type(value)!=dict:
            res[_obj] = _encode(type(value))
        return res
    elif is_pd(value):
        return {_data : pd2bson(value), _obj : _bson2pd}
    elif is_arr(value):
        if value.dtype == np.dtype('O'):
            return {_data : pd2bson(value), _obj : _bson2pd}
        else:
            return {_data : value.tobytes(), 'shape' : value.shape, 'dtype' : encode(value.dtype), _obj : _bson2np}
    else:
        if hasattr(value, 'cache'):
            cache = value.cache
            del value.cache
            res = jp.encode(value)
            value.cache = cache
        else:
            res = jp.encode(value)
        return res

def encode(value):
    """
    
    encode/decode are performed prior to sending to mongodb or after retrieval from db. 
    The idea is to make object embedding in Mongo transparent to the user.
    
    - We use jsonpickle package to embed general objects. These are encoded as strings and can be decoded as long as the original library exists when decoding.
    - pandas.DataFrame are encoded to bytes using pickle while numpy arrays are encoded using the faster array.tobytes() with arrays' shape & type exposed and searchable.
    
    :Example:
    ----------
    >>> from pyg import *; import numpy as np
    >>> value = Dict(a=1,b=2)
    >>> assert encode(value) == {'a': 1, 'b': 2, '_obj': '{"py/type": "pyg.base._dict.Dict"}'}
    >>> assert decode({'a': 1, 'b': 2, '_obj': '{"py/type": "pyg.base._dict.Dict"}'}) == Dict(a = 1, b=2)
    >>> value = dictable(a=[1,2,3], b = 4)
    >>> assert encode(value) == {'a': [1, 2, 3], 'b': [4, 4, 4], '_obj': '{"py/type": "pyg.base._dictable.dictable"}'}
    >>> assert decode(encode(value)) == value
    >>> assert encode(np.array([1,2])) ==  {'data': bytes,
    >>>                                     'shape': (2,),
    >>>                                     'dtype': '{"py/reduce": [{"py/type": "numpy.dtype"}, {"py/tuple": ["i4", false, true]}, {"py/tuple": [3, "<", null, null, null, -1, -1, 0]}]}',
    >>>                                     '_obj': '{"py/function": "pyg.base._encode.bson2np"}'}
    
    :Example: functions and objects
    -------------------------------
    >>> from pyg import *; import numpy as np
    >>> assert encode(ewma) == '{"py/function": "pyg.timeseries._ewm.ewma"}'
    >>> assert encode(Calendar) == '{"py/type": "pyg.base._drange.Calendar"}'
    
    :Parameters:
    ----------------
    value : obj
        An object to be encoded 
        
    :Returns:
    -------
    A pre-json object

    """
    return _encode(value)

def pd2bson(value):
    """
    converts a value (usually a pandas.DataFrame/Series) to bytes using pickle
    """
    return Binary(pickle.dumps(value))


def np2bson(value):
    """
    converts a numpy array to bytes using value.tobytes(). This is much faster than pickle but does not save shape/type info which we save separately.
    """
    return value.tobytes()

def bson2np(data, dtype, shape):
    """
    converts a byte with dtype and shape information into a numpy array.
    
    """
    res = np.frombuffer(data, dtype = dtype)
    return np.reshape(res, shape) if len(shape)!=1 else res

def bson2pd(data):
    """
    converts a pickled object back to an object. We insist that new object has .shape to ensure we did not unpickle gibberish.
    """
    try:
        res = pickle.loads(data)
        res.shape
        return res
    except Exception:
        return None        
    
_bson2pd = encode(bson2pd)
_bson2np = encode(bson2np)
