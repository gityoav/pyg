import re
import numpy as np
import datetime
from collections import Counter

from pyg.base import is_bool, try_back, replace, alphabet, ALPHABET, is_str, \
    as_primitive, encode, as_list, logger, tree_repr, NoneType
from operator import and_, or_
from functools import reduce

__all__ = ['Q','q', 'mdict']


_id = '_id'
_js = 'js'
_regex = 'regex'
_bin = 'bin'
_and = '$and'
_or = '$or'
_not_in = '$nin'
_eq = '$eq'
_ne = '$ne'
_ge = '$gte'
_le = '$lte'
_gt = '$gt'
_lt = '$lt'
_not = '$not'
_mod = '$mod'
_set = '$set'
_unset = '$unset'
_exists = '$exists'
_in = '$in'
_not_in = '$nin'
_type = '$type'
_prev = '_prev'
_data = 'data'
_doc = 'doc'
_pk = '_pk'
_deleted = '_deleted'


_type2bson = {float : [1,19], 
              str : 2,
              np.nan : 10, 
              np.ndarray : 4, 
              _id : 7, 
              _js : [13,15], 
              bool : 8, 
              None: 10, 
              _regex : 11,
              _bin : 5,
              datetime.datetime: [9,17],
              datetime.date: 9,
              int : [16,18]
              }

_sorted = try_back(sorted)

def _is_query(query):
    if isinstance(query, mdict):
        return True
    for key, value in query.items():
        if key.startswith('$'):
            return True
        elif isinstance(value, dict):
            if _is_query(value):
                return True
    return False


def _as_key(key):
    txt = ''.join([char if char in alphabet + ALPHABET else '_' for char in key])
    txt = replace(txt, '__','_')
    if txt.startswith('_') and not key.startswith('_'):
        txt = txt[1:]
    if txt.endswith('_') and not key.endswith('_'):
        txt = txt[:-1]
    return txt

def _case_proxies(keys):
    """
    creates a dict of possible keys mapping to original keys
    
    The reason is to allow getattr on keys with invalid characters in them
    
    :Example:
    ---------
    >>> from pyg import Q
    >>> query = Q(['hello world', 'James Joyce'])
    >>> (query.hello_world == 1) & (query.james_joyce == 2)
    >>> {"$and": [{"James Joyce": {"$eq": 2}}, {"hello world": {"$eq": 1}}]}
    
    
    :Parameters:
    ----------------
    keys : list of strings
        keys for which we will use a proxy to specify these keys


    """
    keys = set(as_list(keys))
    res = {key: key for key in keys}
    proxy = [_as_key(str(k)) for k in keys]
    counter = Counter(proxy)    
    res.update({p : value for p, value in zip(proxy, keys) if counter[p] == 1})
    proxy = [p.lower() for p in proxy]
    counter = Counter(proxy)    
    res.update({p : value for p, value in zip(proxy, keys) if counter[p] == 1})
    proxy = [p.upper() for p in proxy]
    counter = Counter(proxy)    
    res.update({p : value for p, value in zip(proxy, keys) if counter[p] == 1})
    return res
    

def _as_items(query):
    """
    converts a query into keys and tuples
    """
    if query is None:
        return []
    elif isinstance(query, dict):
        return _sorted([(key, _as_items(value)) for key, value in query.items()])
    elif isinstance(query, list):
        return _sorted([_as_items(key) for key in query])
    else:
        return query


def _q_set(values):
    if len(values) > 1:
        values = _sorted(values, key = _as_items)
        items = [_as_items(v) for v in values]
        values = [values[i] for i in range(len(values)) if i ==0 or items[i]!=items[i-1]]
        return values[1:] if items[0] == [] else values
    elif len(values) == 1:
        return [] if _as_items(values[0]) == [] else values
    else:
        return values

def _q_and(values):
    values = _q_set(values)
    return reduce(and_, values) if len(values)>0 else mdict()

def _q_or(values):
    values = _q_set(values)
    return reduce(or_, values) if len(values)>0 else mdict()


class mkey(object):
    """
    mongo query key
    """
    def __init__(self, key):
        self._key = key

    def __getattr__(self, subkey):
        return mkey('%s.%s'%(self._key, subkey))
    
    def __getitem__(self, subkey):
        if is_str(subkey):
            return mkey('%s.%s'%(self._key, subkey))
        elif isinstance(subkey, slice):
            if subkey.start is None and subkey.stop is None:
                return self
            elif subkey.start is None:
                return self < subkey.stop
            elif subkey.stop is None:
                return self >= subkey.start
            else:
                return (self >= subkey.start) & (self < subkey.stop)
        else:
            return self == subkey

    def __call__(self, **kwargs):
        return _q_and([self[key] == value for key, value in kwargs.items()])
    
    def _set(self, other):
        other = encode(as_primitive(other))
        return mdict({self._key : other})
    
    def __pos__(self):
        return self._set({_exists : True})

    def __neg__(self):
        return self._set({_exists : False})

    __invert__ = __neg__
    
    @property 
    def exists(self):
        return self._set({_exists : True})

    @property 
    def not_exists(self):
        return self._set({_exists : False})

    def __eq__(self, other):
        if isinstance(other, re.Pattern):
            return self._set({_regex : other.pattern})
        other = as_primitive(other)
        if isinstance(other, list):
            if len(other) == 1:
                return self._set({_eq: other[0]})
            else:
                return self._set({_in : other})
        elif is_bool(other):
            return self._set({_in : [True, 1] if other else [False, 0]})
        elif isinstance(other, dict):
            return self(**other)
        else:
            return self._set({_eq: other}) 
    
    def __ne__(self, other):
        if isinstance(other, re.Pattern):
            return self._set({_not : {_regex : other.pattern}})
        elif isinstance(other, list):
            if len(other) == 1:
                return self._set({_ne : other})
            else:
                return self._set({_not_in : other})
        else:
            return self._set({_ne : other})
    
    def __ge__(self, other):
        return self._set({ _ge : other })

    def __gt__(self, other):
        return self._set({ _gt : other })

    def __le__(self, other):
        return self._set({ _le : other })
    
    def __lt__(self, other):
        return self._set({ _lt : other })

    def __mod__(self, other):
        return modkey(self._key, other)

    def __and__(self, other) : 
        return (+self) & other

    def __add__(self, other) : 
        return (+self) + other

    def __sub__(self, other) : 
        return (+self) & (~other)

    def __or__(self, other) : 
        return (+self) | other
            
    def isinstance(self, *types):
        """
         see https://docs.mongodb.com/manual/reference/operator/type  
        """
        bson_types = _sorted(set(sum([as_list(_type2bson.get(t)) for t in types],[])))
        return self._set({_type : bson_types})


class modkey(object):

    def __init__(self, key, div):
        self._key = key
        self._div = div

    def __eq__(self, remainder):
        return mdict({self._key: {_mod : [self._div, remainder]}})

    def __ne__(self, remainder):
        return mdict({self._key: { _not: {_mod : [self._div, remainder]}}})
    
    def __pos__(self):
        return self.__ne__(0)
    
    def __neg__(self):
        return self.__eq__(0)
    
    __invert__ = __neg__
    
    def __and__(self, other) : 
        return (+self) & other

    def __add__(self, other) : 
        return (+self) + other

    def __sub__(self, other) : 
        return (+self) - other

    def __or__(self, other) : 
        return (+self) | other

    def __repr__(self) : 
        return 'mod(%s, %s)'%(self._key, self._div)
    
class mdict(dict):
    def _chain(self, other, key):
        rollback = False
        if not isinstance(other, dict):
            _mkey = other
            other = +other
            rollback = True
        if list(self.keys()) == [key]:
            res = mdict({key : list(self[key])})
            res[key].append(other)
        elif list(other.keys()) == [key]:
            res = mdict({key : list(other[key])})
            res[key].append(self)
        else:
            res = mdict({key : [self, other]})
        if rollback:
            res._mkey = _mkey
            res._prev = self
            res._key = key
        res[key] = _q_set(res[key])
        if len(res[key]) == 1:
            return self
        else:
            return res
    
    def __and__(self, other):
        return self._chain(other, _and)
    
    __add__ = __and__ 
    
    update = __and__
    
    def __sub__(self, other):
        return self + (-other)
    
    def __or__(self, other):
        return self._chain(other, _or)
    
    def __pos__(self): 
        return self

    def __neg__(self):
        return mdict({_not: self})
    
    __invert__ = __neg__
    
    def __eq__(self, other):
        if hasattr(self, _prev):
            logger.warning('WARN: operation order ambiguity, you should use brackets')
            return self._prev._chain(self._mkey == other, self._key)
        else:
            return super(mdict, self).__eq__(other)

    def __ne__(self, other):
        if hasattr(self, _prev):
            logger.warning('WARN: operation order ambiguity, you should use brackets')
            return self._prev._chain(self._mkey != other, self._key)
        else:
            return super(mdict, self).__ne__(other)

    def __ge__(self, other):
        if hasattr(self, _prev):
            logger.warning('WARN: operation order ambiguity, you should use brackets')
            return self._prev._chain(self._mkey>=other, self._key)
        else:
            raise ValueError('cannot compare dicts this way')

    def __gt__(self, other):
        if hasattr(self, _prev):
            logger.warning('WARN: operation order ambiguity, you should use brackets')
            return self._prev._chain(self._mkey > other, self._key)
        else:
            raise ValueError('cannot compare dicts this way')


    def __le__(self, other):
        if hasattr(self, _prev):
            logger.warning('WARN: operation order ambiguity, you should use brackets')
            return self._prev._chain(self._mkey<=other, self._key)
        else:
            raise ValueError('cannot compare dicts this way')

    def __lt__(self, other):
        if hasattr(self, _prev):
            logger.warning('WARN: operation order ambiguity, you should use brackets')
            return self._prev._chain(self._mkey < other, self._key)
        else:
            raise ValueError('cannot compare dicts this way')

    def __repr__(self):
        res = tree_repr(dict(self))
        return res.replace('True', 'true').replace('False', 'false').replace("'", '"')

    def __str__(self):
        return 'M%s'%(dict(self).__str__().replace('M{', '{'))
    

class Q(dict):
    """
    
    The MongoDB interface for query of a collection (table) is via a creation of a complicated looking dict:
    https://docs.mongodb.com/manual/tutorial/query-documents/
    
    This is rather complicated for the average user so Q simplifies it greatly.
    Q is based on TinyDB and users of TinyDB will recognise it. 
    https://tinydb.readthedocs.io/en/latest/usage.html
    
    q is the singleton of Q.
    
    q supports both *calling* to generate the querying dict

    >>> q(a = 1, b = 2)

    or

    >>> (q.a == 1) & (q.b == 2)  # {"$and": [{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]}
    >>> (q.a == 1) | (q.b == 2)  # {"$or": [{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]}

    or indeed 
    
    >>> q(q.a == 1, q.b  == 2)
       
    :Example:
    -------
    >>> from pyg import q
    >>> import re
    
    >>> assert dict(q.a == 1) == {"a": {"$eq": 1}}
    >>> assert dict(q(a = [1,2])) == {'a': {'$in': [1, 2]}}
    >>> assert dict(q(q.a == [1,2], q.b > 3)) == {'$and': [{"a": {"$in": [1, 2]}}, {"b": {"$gt": 3}}]}  # a in [1,2] and b greater than 3
    >>> assert dict(q(a = re.compile('^hello'))) == {'a': {'regex': '^hello'}}     # a regex query using regex expressions    
    
    Existence
    ---------------
    >>> assert dict(q.a.exists + q.b.not_exists)  == {"$and": [{"a": {"$exists": true}}, {"b": {"$exists": false}}]}

    Not operator
    ------------------
    >>> assert dict(~(q.a==1))  == {'$not': {"a": {"$eq": 1}}}


    """
        
    def __init__(self, keys = None):
        if hasattr(keys, 'keys'):
            keys = getattr(keys, 'keys')
        if hasattr(keys, 'columns'):
            keys = getattr(keys, 'columns')
        if callable(keys):
            keys = keys()
        super(Q, self).__init__(_case_proxies(as_list(keys)))
    
    def __dir__(self):
        return _sorted(self.keys())
    
    def __getattr__(self, value):
        return mkey(super(Q, self).__getitem__(value)) if len(self) else mkey(value)
    
    def __call__(self, *args, **kwargs):
        values = [self[key] == value for key, value in kwargs.items()] + [self[arg] for arg in args]
        return _q_and(values)
    
    def __getitem__(self, value):
        if isinstance(value, list) and min([isinstance(v, (dict, list, NoneType)) for v in value], default = True):
            values = [self[v] for v in value]
            return _q_or(values)
        elif isinstance(value, dict):
            if _is_query(value):
                return value
            else:
                return self(**value)
        elif value is None:
            return mdict()
        else:
            return mkey(super(Q, self).__getitem__(value)) if len(self) else mkey(value)

q = Q()
                        