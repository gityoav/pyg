from _collections_abc import dict_keys, dict_values
from pyg.base._as_list import as_list, as_tuple
from pyg.base._dict import Dict
from pyg.base._zip import zipper, lens
from pyg.base._types import is_str, is_strs, is_arr, is_df, is_dicts, is_int, is_ints, is_tuple, is_bools, is_nan
from pyg.base._tree import is_tree, tree_to_table
from pyg.base._inspect import getargs
from pyg.base._decorators import kwargs_support, try_none
from pyg.base._sort import sort, cmp
from pyg.base._file import read_csv
from functools import reduce
import pandas as pd

__all__ = ['dict_concat', 'dictable', 'is_dictable']


def dict_concat(*dicts):
    """
    A method of turning a list of dicts into one happy dict sharing its keys

    :Parameters:
    ----------------
    *dicts : list of dicts
        a list of dicts

    :Returns:
    -------
    dict
        a dict of a list of values.
    
    :Example:
    --------------
    >>> dicts = [dict(a=1,b=2), dict(a=3,b=4,c=5)]
    >>> assert dict_concat(dicts) == dict(a = [1,3], b = [2,4], c = [None,5])
    >>> dicts = [dict(a=1,b=2)]
    >>> assert dict_concat(dicts) == dict(a = [1], b = [2])
    >>> assert dict_concat([]) == dict()
    """
    
    dicts = as_list(dicts)
    if len(dicts) == 0:
        return {}
    elif len(dicts) == 1:
        return {key: [value] for key, value in dicts[0].items()}  # a shortcut, we dont need to run a full merge
    possible_keys = list(set([tuple(sorted(d.keys())) for d in dicts]))
    if len(possible_keys) == 1:
        pairs = [sorted(d.items()) for d in dicts]
        keys = possible_keys[0]
        values = zip(*[[value for _, value in row] for row in pairs])
        res = dict(zip(keys, map(list, values)))
        return res
    else:
        keys = reduce(lambda res, keys: res | set(keys), possible_keys, set())
        return {key: [d.get(key) for d in dicts] for key in keys}
    

def _text_box(value, max_rows = 5, max_chars = 50):
    v = str(value)
    res = v.split('\n')
    if max_rows:
        res = res[:max_rows]
    if max_chars:
        res = [row[:max_chars] for row in res]
    return res


def _data_columns_as_dict(data, columns = None):
    """
    >>> assert _data_columns_as_dict(data = [], columns = []) == dict()
    >>> assert _data_columns_as_dict([], 'a') == dict(a = [])
    >>> assert _data_columns_as_dict(data = [], columns = ['a','b']) == dict(a = [], b = [])
    
    :Parameters:
    ----------------
    data : TYPE
        DESCRIPTION.
    columns : TYPE, optional
        DESCRIPTION. The default is None.

    :Returns:
    -------
    TYPE
        DESCRIPTION.

    """
    if data is None and columns is None:
        return {}
    if isinstance(data, zip):
        data = list(data)
    if columns is not None:
        if is_str(columns):
            if is_tree(columns) and isinstance(data, dict):
                return dict_concat(tree_to_table(data, columns))
            else:
                return {columns : data}
        else:
            if len(data) == 0:
                return {column : [] for column in columns}
            return dict(zipper(columns, zipper(*data)))
    else:
        if is_str(data):
            if data.endswith('.csv'):
                data = read_csv(data)
            elif 'xls' in data:
                data = pd.ExcelFile(data).parse()
            else:
                return dict(data = data)
        if is_df(data):
            if data.index.name is not None:
                data = data.reset_index()
            return data.to_dict('list')
        tp = str(type(data)).lower()        
        if hasattr(data, 'next') and 'cursor' in tp:
            data = list(data)
        elif hasattr(data, 'find') and 'collection' in tp:
            data = list(data.find({}))
        if isinstance(data, list):
            if len(data) == 0:
                return {}
            elif min([is_tuple(row) and len(row) == 2 for row in data]):
                return dict(data)
            elif is_dicts(data):
                return dict_concat(data)
            elif min([isinstance(i, list) for i in data]):
                return dict(zipper(data[0], zipper(*data[1:])))
            else:
                return dict(data = data)
        elif isinstance(data, dict):
            return dict(data)
        else:
            return dict(data = data)


def _value(value):
    if value is None:
        return [None]
    elif isinstance(value, (dict_values, dict_keys, range)):
        return list(value)
    else:
        return as_list(value)

class dictable(Dict):
    """
    :What is dictable?:
    -------------------
    dictable is a table, a collection of iterable records. It is also a dict with each key being a column. 
    Why not use a pandas.DataFrame? pd.DataFrame leads a dual life: 
        - by day an index-based optimized numpy array supporting e.g. timeseries analytics etc.
        - by night, a table with keys supporting filtering, aggregating, pivoting on keys as well as inner/outer joining on keys.

    
    dictable only tries to do the latter. dictable should be thought of as a 'container for complicated objects' rather than just an array of primitive floats.
    In general, each cell may contain timeseries, yield_curves, machine-learning experiments etc.
    The interface is very succinct and allows the user to concentrate on logic of the calculations rather than boilerplate.
    
    dictable supports quite a flexible construction:

    :Example: construction using records
    ------------------------------------
    >>> from pyg import *; import pandas as pd
    >>> d = dictable([dict(name = 'alan', surname = 'atkins', age = 39, country = 'UK'), 
    >>>               dict(name = 'barbara', surname = 'brown', age = 29, country = 'UK')])
    
    :Example: construction using columns and constants
    ---------------------------------------------------
    >>> d = dictable(name = ['alan', 'barbara'], surname = ['atkins', 'brown'], age = [39, 29], country = 'UK')
    
    :Example: construction using pandas.DataFrame
    ---------------------------------------------
    >>> original = dictable(name = ['alan', 'barbara'], surname = ['atkins', 'brown'], age = [39, 29], country = 'UK')
    >>> df_from_dictable = pd.DataFrame(original)
    >>> dictable_from_df = dictable(df_from_dictable)
    >>> assert original == dictable_from_df

    :Example: construction rows and columns
    ---------------------------------------
    >>> d = dictable([['alan', 'atkins', 39, 'UK'], ['barbara', 'brown', 29, 'UK']], columns = ['name', 'surname', 'age', 'country'])


    :Access: column access
    ----------------------
    >>> assert d.keys() ==  ['name', 'surname', 'age', 'country']
    >>> assert d.name == ['alan', 'barbara']
    >>> assert d['name'] == ['alan', 'barbara']
    >>> assert d['name', 'surname'] == [('alan', 'atkins'), ('barbara', 'brown')]
    >>> assert d[lambda name, surname: '%s %s'%(name, surname)] == ['alan atkins', 'barbara brown']


    :Access: row access & iteration
    -------------------------------
    >>> assert d[0] == {'name': 'alan', 'surname': 'atkins', 'age': 39, 'country': 'UK'}
    >>> assert [row for row in d] == [{'name': 'alan', 'surname': 'atkins', 'age': 39, 'country': 'UK'},
    >>>                               {'name': 'barbara', 'surname': 'brown', 'age': 29, 'country': 'UK'}]

    Note that members access is commutative: 

    >>> assert d.name[0] == d[0].name == 'alan'
    >>> d[lambda name, surname: name + surname][0] == d[0][lambda name, surname: name + surname]
    >>> assert sum([row for row in d], dictable()) == d

    :Example: adding records
    ------------------------
    >>> d = dictable(name = ['alan', 'barbara'], surname = ['atkins', 'brown'], age = [39, 29], country = 'UK')
    >>> d = d + {'name': 'charlie', 'surname': 'chocolate', 'age': 49} # can add a record directly
    >>> assert d[-1] == {'name': 'charlie', 'surname': 'chocolate', 'age': 49, 'country': None}
    >>> d += dictable(name = ['dana', 'ender'], surname = ['deutch', 'esterhase'], age = [10, 20], country = ['Germany', 'Hungary'])
    >>> assert d.name == ['alan', 'barbara', 'charlie', 'dana', 'ender']
    >>> assert len(dictable.concat([d,d])) == len(d) * 2

    :Example: adding columns
    ------------------------
    >>> d = dictable(name = ['alan', 'barbara'], surname = ['atkins', 'brown'], age = [39, 29], country = 'UK')
    
    >>> ### all of the below are ways of adding columns ####
    >>> d.gender == ['m', 'f']
    >>> d = d(gender = ['m', 'f'])
    >>> d['gender'] == ['m', 'f']
    >>> d2 = dictable(gender = ['m', 'f'], profession = ['astronaut', 'barber'])
    >>> d = d(**d2)

    :Example: adding derived columns
    --------------------------------
    >>> d = dictable(name = ['alan', 'barbara'], surname = ['atkins', 'brown'], age = [39, 29], country = 'UK')
    >>> d = d(full_name = lambda name, surname: proper('%s %s'%(name, surname))) 
    >>> d['full_name'] = d[lambda name, surname: proper('%s %s'%(name, surname))]
    >>> assert d.full_name == ['Alan Atkins', 'Barbara Brown']

    :Example: dropping columns
    ---------------------------
    >>> d = dictable(name = ['alan', 'barbara'], surname = ['atkins', 'brown'], age = [39, 29], country = 'UK')
    >>> del d.country # in place
    >>> del d['age'] # in place
    >>> assert (d - 'name')[0] ==  {'surname': 'atkins'} and d[0] == {'name': 'alan', 'surname': 'atkins'}

    :Example: row selection, inc/exc
    --------------------------------------
    >>> d = dictable(name = ['alan', 'barbara'], surname = ['atkins', 'brown'], age = [39, 29], country = 'UK')
    >>> assert len(d.exc(name = 'alan')) == 1
    >>> assert len(d.exc(lambda age: age<30)) == 1 # can filter on *functions* of members, not just members.
    >>> assert d.inc(name = 'alan').surname == ['atkins']
    >>> assert d.inc(lambda age: age<30).name == ['barbara']
    >>> assert d.exc(lambda age: age<30).name == ['alan']

    dictable supports:
        - sort 
        - group-by/ungroup
        - list-by/ unlist
        - pivot/unpivot
        - inner join, outer join and xor

    Full details are below.
    """
    def __init__(self, data = None, columns = None, **kwargs):
        kwargs = {key :_value(value) for key, value in kwargs.items()}
        data_kwargs = {key: _value(value) for key, value in _data_columns_as_dict(data, columns).items()}
        kwargs.update(data_kwargs)
        n = lens(*kwargs.values())
        kwargs = {str(key) if is_int(key) else key : value * n if len(value)==1 else value for key, value in kwargs.items()}
        super(dictable, self).__init__(kwargs)
        
    def __len__(self):
        return lens(*self.values())
    
    @property
    def shape(self):
        return (len(self), len(self.keys()))
    
    @property
    def columns(self):
        return self.keys()

    def get(self, key, default = None):
        if key in self:
            return self[key]
        else:
            return [default] * len(self)
        
    def __iter__(self):
        for row in zip(*self.values()):
            yield Dict(zip(self.keys(), row))
            
    def update(self, other):
        for k, v in other.items():
            self[k] = v
    
    def __setitem__(self, key, value):
        n = len(self)
        value = _value(value)
        if len(value) == n or len(self.keys()) == 0:
            pass
        elif len(value) == 1:
            value = value * n 
        else:
            raise ValueError('cannot set item of length %s in table of length %s'%(len(value), n))
        super(dictable, self).__setitem__(str(key) if is_int(key) else key, value)
    
    def __getitem__(self, item):
        if is_arr(item) and len(item.shape) == 1:
            item = list(item)
        if isinstance(item, slice):
            return type(self)({key : value[item] for key, value in self.items()})
        if isinstance(item, (dict_keys, dict_values, range)):
            item = list(item)
        if isinstance(item , list):
            if len(item) == 0:
                return dictable(data = [], columns = self.keys())
            elif is_strs(item):
                return type(self)(super(dictable, self).__getitem__(item))
            elif is_bools(item):
                res = type(self)([row for row, tf in zipper(self, item) if tf])
                return res if len(res) else type(self)([], self.keys())
            elif is_ints(item):
                values = list(zip(*self.values()))
                return dictable(data = [values[i] for i in item], columns = self.keys())
            else:
                raise ValueError('We dont know how to understand this item %s'%item)
        elif is_int(item):
            return Dict({key : value[item] for key, value in self.items()})
        elif item in self.keys():
            return super(dictable, self).__getitem__(item)
        elif is_tuple(item):
            return list(zip(*[self[i] for i in item]))
        elif callable(item):
            return self.apply(item)
        else:
            raise KeyError('item %s not found'%item)

    def __getattr__(self, attr):
        if attr in self:
            return self[attr]
        elif attr.startswith('_'):
            return super(dictable, self).__getattr__(attr)
        elif attr.startswith('find_'):
            key = attr[5:]
            if key not in self:
                raise KeyError('cannot find %s in dictable'%key)
            def f(*args, **kwargs):
                items = self.inc(*args, **kwargs)
                if len(items) == 0:
                    raise ValueError('no %s found'%key)
                item = items[key]
                if len(item)>1:
                    item = list(set(item))
                    if len(item)>1:
                        raise ValueError('multiple %s found %s'%(key, item))
                return item[0]
            f.__name__ = 'find_%s'%key
            return f
        else:
            raise AttributeError('%s not found'%attr)
 
    def __setattr__(self, attr, value):
        if attr.startswith('_'):
            super(dictable, self).__setattr__(attr, value)
        else:
            self.__setitem__(attr, value)

    def __delattr__(self, attr):
        if attr.startswith('_'):
            super(dictable, self).__delattr__(attr)
        else:
            super(dictable, self).__delitem__(attr)

    def inc(self, *functions, **filters):
        """
        performs a filter on what rows to include

        :Parameters:
        ----------------
        *functions : callables or a dict
            filters based on functions of each row
        **filters : value or list of values
            filters per each column

        :Returns:
        -------
        dictable
            table with rows that satisfy all conditions.
            
        
        :Example: filtering on keys
        -------
        >>> from pyg import *; import numpy as np
        >>> d = dictable(x = [1,2,3,np.nan], y = [0,4,3,5])
        >>> assert d.inc(x = np.nan) == dictable(x = np.nan, y = 5)            
        >>> assert d.inc(x = 1) == dictable(x = 1, y = 0)            
        >>> assert d.inc(x = [1,2]) == dictable(x = [1,2], y = [0,4]) 

        :Example: filtering on callables
        --------------
        >>> from pyg import *; import numpy as np
        >>> d = dictable(x = [1,2,3,np.nan], y = [0,4,3,5])
        >>> assert d.inc(lambda x,y: x>y) == dictable(x = 1, y = 0)

        """
        if len(functions) + len(filters) == 0:
            return self
        res = self.copy()
        functions = as_list(functions)
        for function in functions:
            if type(function) == dict:
                filters.update(function)
            else:
                f = kwargs_support(function)
                res = type(self)([row for row in res if f(**row)])
        for key, value in filters.items():
            if value is None:
                res = res[[r is None for r in res[key]]]
            elif is_nan(value):
                res = res[[is_nan(r) for r in res[key]]]
            else:
                value = as_list(value)
                res = res[[r in value for r in res[key]]]
        if len(res) == 0:
            return type(self)([], self.keys())
        return res                

    def exc(self, *functions, **filters):
        """
        performs a filter on what rows to exclude

        :Parameters:
        ----------------
        *functions : callables or a dict
            filters based on functions of each row
        **filters : value or list of values
            filters per each column

        :Returns:
        -------
        dictable
            table with rows that satisfy all conditions excluded.
            
        
        :Example: filtering on keys
        -------
        >>> from pyg import *; import numpy as np
        >>> d = dictable(x = [1,2,3,np.nan], y = [0,4,3,5])
        >>> assert d.exc(x = np.nan) == dictable(x = [1,2,3], y = [0,4,3])         
        >>> assert d.exc(x = 1) == dictable(x = [2,3,np.nan], y = [4,3,5])
        >>> assert d.exc(x = [1,2]) == dictable(x = [1,2], y = [0,4]) 

        :Example: filtering on callables
        --------------
        >>> from pyg import *; import numpy as np
        >>> d = dictable(x = [1,2,3,np.nan], y = [0,4,3,5])
        >>> assert d.exc(lambda x,y: x>y) == dictable(x = 1, y = 0)

        """
        if len(functions) + len(filters) == 0:
            return self
        res = self.copy()
        functions = as_list(functions)
        for function in functions:
            if type(function) == dict:
                filters.update(function)
            else:
                f = kwargs_support(function)
                res = type(self)([row for row in res if not f(**row)])
        for key, value in filters.items():
            if value is None:
                res = res[[r is not None for r in res[key]]]
            elif is_nan(value):
                res = res[[not is_nan(r) for r in res[key]]]
            else:
                value = as_list(value)
                res = res[[r not in value for r in res[key]]]
        if len(res) == 0:
            return type(self)([], self.keys())
        return res                

    
    def apply(self, function):
        f = kwargs_support(function)
        return [f(**row) for row in self]

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
        res : dictable

        :Example:
        --------------
        >>> from pyg import *
        >>> d = dictable(name = ['adam', 'barbara', 'chris'], surname = ['atkins', 'brown', 'cohen'])
        >>> assert d.do(proper) == dictable(name = ['Adam', 'Barbara', 'Chris'], surname = ['Atkins', 'Brown', 'Cohen'])

        :Example: using another column in the calculation
        -------
        >>> from pyg import *
        >>> d = dictable(a = [1,2,3,4], b = [5,6,9,8], denominator = [10,20,30,40])
        >>> d = d.do(lambda value, denominator: value/denominator, 'a', 'b')
        >>> assert d == dictable(a = 0.1, b = [0.5,0.3,0.3,0.2], denominator = [10,20,30,40])
        
        """
        res = self.copy()
        keys = as_list(keys)
        if len(keys)  == 0:
            keys = self.keys()
        for key in keys:    
            for f in as_list(function):
                args = as_list(try_none(getargs)(f))
                res[key] = [f(row[key], **{k : v for k, v in row.items() if k in args[1:]}) for row in res]
        return res

    @classmethod
    def concat(cls, *others):
        """
        adds together multiple dictables. equivalent to sum(others, self) but a little faster

        :Parameters:
        ----------------
        *others : dictables
            records to be added to current table

        :Returns:
        -------
        merged : dictable
            sum of all records
            
        :Example:
        -------
        >>> from pyg import *
        >>> d1 = dictable(a = [1,2,3])
        >>> d2 = dictable(a = [4,5,6])
        >>> d3 = dictable(a = [7,8,9])
        
        >>> assert dictable.concat(d1,d2,d3) == dictable(a = range(1,10))
        >>> assert dictable.concat([d1,d2,d3]) == dictable(a = range(1,10))
        """
        others = as_list(others)
        others = [cls(other) if not isinstance(other, cls) else other for other in others]
        if len(others) == 0:
            return cls()
        elif len(others) == 1:
            return others[0]
        concated = dict_concat(others)
        merged = cls({key : sum(value, []) for key, value in concated.items()}) 
        return merged

    def __repr__(self):
        return 'dictable[%s x %s]'%self.shape + '\n%s'%self.__str__(3, 150)
    
    
    def __str__(self, max_rows = None, max_width = 150):
        return self.to_string(cat = max_rows, mid = '...%i rows...'%len(self), max_width=max_width)
    
    def to_string(self, colsep = '|', rowsep = '', cat = None, max_rows = 5, max_width = None, mid = '...', header = None, footer = None):
        if cat and len(self) > 2 * cat:
            box = {key: [_text_box(key)] + 
                        [_text_box('dictable[%s x %s]\n%s'%(len(self), len(self.keys()), self.keys()) if isinstance(v, dictable) else v, max_rows) for v in value[:cat]+value[-cat:]]
                    for key, value in self.items()}
        else:
            box = {key: [_text_box(key)] + 
                        [_text_box('dictable[%s x %s]\n%s'%(len(self), len(self.keys()), self.keys()) if isinstance(v, dictable) else v, max_rows) for v in value]
                    for key, value in self.items()}
    
        chars = {key : max([max([len(r) for r in v]) for v in value]) for key, value in box.items()}
        padded = {key : [[r + ' ' * (chars[key]-len(r)) for r in v] for v in value] for key, value in box.items()}
        def text(padded, keys = None):
            padded_ = padded if keys is None else {key: padded[key] for key in keys}
            rows = list(zip(*padded_.values()))
            empty_rows = [[' '*chars[key]] for key in padded_]
            ns = [max([len(v) for v in row]) for row in rows]
            if len(rowsep) == 1:
                sep_rows = [[rowsep*chars[key]] for key in padded_]
                res = ['\n'.join([colsep.join(v) for v in zip(*[r + e * (n-len(r)) + s for r,e,s in zip(row, empty_rows, sep_rows)])]) 
                       for row,n in zip(rows, ns)]
            else:
                res = ['\n'.join([colsep.join(v) for v in zip(*[r+e*(n-len(r)) for r, e in zip(row, empty_rows)])]) for row, n in zip(rows, ns)]
            if mid and cat and len(self)>2*cat:
                n = (len(res)+1)//2
                res = res[:n] + [mid] + res[n:]
            if rowsep == 'header':
                res = res[:1] + ['+'.join(['-'*chars[key] for key in padded_])] + res[1:]
            if header:
                res = [header] + res
            if footer:
                res = res + [footer]
            return '\n'.join(res)
        if max_width is None or max_width<=0:
            return text(padded)
        else:
            keys = list(padded.keys())
            res = []
            while len(keys)>0:
                i = 0
                width = 0
                while width<max_width and i<len(keys):
                    width+= chars[keys[i]]
                    i+=1
                res.append(text(padded, keys[:i]))
                keys = keys[i:]
            return '\n\n'.join(res)

    def sort(self, *by):
        """
        Sorts the table either using a key, list of keys or functions of members
        
        
        :Example:
        -------
        >>> import numpy as np
        >>> self = dictable(a = [_ for _ in 'abracadabra'], b=range(11), c = range(0,33,3))
        >>> self.d = list(np.array(self.c) % 11)
        >>> res = self.sort('a', 'd')
        >>> assert list(res.c) == list(range(11))
        
                
        >>> d = dictable(a = ['a', 1, 'c', 0, 'b', 2]).sort('a')        
        >>> res = d.sort('a','c')
        >>> print(res)
        >>> assert ''.join(res.a) == 'aaaaabbcdrr' and list(res.c) == [0,4,8,9,10] + [2,3] + [1] + [7] + [5,6]
        
        >>> d = d.sort(lambda b: b*3 % 11) ## sorting again by c but using a function
        >>> assert list(d.c) == list(range(11))
        """
        by = as_list(by)
        if len(self) == 0 or len(by) == 0:
            return self
        keys = self[as_tuple(by)]
        keys2id = list(zip(keys, range(len(self))))
        _, rows = zip(*sort(keys2id))
        return type(self)({key: [value[i] for i in rows] for key, value in self.items()})

    def __add__(self, other):
        return self.concat(self, other)                
        
    def _listby(self, by):
        keys = self[by]
        keys2id = sort(list(zip(keys, range(len(self)))))
        prev = None
        res = []
        row = []
        for key, i in keys2id:
            if len(row) == 0 or key==prev:
                row.append(i)
            else:
                res.append((prev, row))
                row = [i]
            prev = key 
        res.append((prev, row))
        return zip(*res)

    def listby(self,*by):
        if len(self) == 0:
            return self
        if len(by) == 0:
            by = self.keys()
        by = as_tuple(by)
        if len(by) == 0:
            return dictable({key: [value] for key, value in dict(self).items()})
        xs, ids = self._listby(by)
        rtn = dictable(xs, by)
        rtn.update({k: [[self[k][i] for i in y] for y in ids] for k in self.keys() if k not in by})
        return rtn
    
    def unlist(self):
        """
        undoes listby...
        
        :Example:
        -------
        >>> x = dictable(a = [1,2,3,4], b= [1,0,1,0])
        >>> x.listby('b')
        
        dictable[2 x 2]
        b|a     
        0|[2, 4]
        1|[1, 3]
        
        
        >>> assert x.listby('b').unlist().sort('a') == x

        :Returns:
        -------
        dictable
            a dictable where all rows with list in them have been 'expanded'.

        """
        return self.concat([row for row in self]) if len(self) else self
    
    def groupby(self,*by, grp = 'grp'):
        """
        Similar to pandas groupby but returns a dictable of dictables with a new column 'grp' 
        
        :Example:
        -------
        >>> x = dictable(a = [1,2,3,4], b= [1,0,1,0])
        >>> res = x.groupby('b')
        >>> assert res.keys() == ['b', 'grp']
        >>> assert is_dictable(res[0].grp) and res[0].grp.keys() == ['a']

        :Parameters:
        ----------------
        *by : str or list of strings
        
            gr.
        grp : str, optional
            The name of the column for the dictables per each key. The default is 'grp'.

        :Returns:
        -------
        dictable
            A dictable containing the original keys and a dictable per unique key.

        """
        if len(self) == 0:
            return self
        if len(by) == 0:
            by = self.keys()
        by = as_tuple(by)
        if len(by) == 0:
            raise ValueError('cannot groupby on no keys, left with original dictable')
        elif len(by) == len(self.keys()):
            raise ValueError('cannot groupby on all keys... nothing left to group')
        xs,ys = self._listby(by)
        rtn = dictable(xs, by)
        rtn[grp] = [dictable({k: [self[k][i] for i in y] for k in self.keys() if k not in by}) for y in ys]
        return rtn

    def ungroup(self, grp = 'grp'):
        """
        Undoes groupby

        :Example:
        -------
        >>> x = dictable(a = [1,2,3,4], b= [1,0,1,0])
        >>> self = x.groupby('b')
        
        :Parameters:
        ----------------
        grp : str, optional
            column name where dictables are. The default is 'grp'.

        :Returns:
        -------
        dictable.

        """
        return self.concat([row.pop(grp)(**row.do(lambda v: [v])) for row in self])
        
    
    def join(self, other, lcols = None, rcols = None, mode = None):
        """
        
        Performs either an inner join or a cross join between two dictables

        :Example: inner join
        -------------------------------
        >>> from pyg import *
        >>> x = dictable(a = ['a','b','c','a']) 
        >>> y = dictable(a = ['a','y','z'])
        >>> assert x.join(y) == dictable(a = ['a', 'a'])

        :Example: outer join
        -------------------------------
        >>> from pyg import *
        >>> x = dictable(a = ['a','b']) 
        >>> y = dictable(b = ['x','y'])
        >>> assert x.join(y) == dictable(a = ['a', 'a', 'b', 'b'], b = ['x', 'y', 'x', 'y'])


        """
        if not isinstance(other, dictable):
            other = dictable(other)
        if lcols is None:
            lcols = self.keys() & other.keys()
        if rcols is None:
            rcols = lcols
        lcols = as_tuple(lcols); rcols = as_tuple(rcols)
        if len(lcols)!=len(rcols):
            raise ValueError('cannot inner join when cols on either side mismatch in length %s vs %s'%(lcols, rcols))
        cols = []
        for lcol, rcol in zip(lcols, rcols):
            if is_str(lcol):
                cols.append(lcol)
            elif is_str(rcol):
                cols.append(rcol)
            else:
                raise ValueError('Cannot use a formula to inner join on both left and right %s %s'%(lcol, rcol))
        lkeys = self.keys() - cols
        rkeys = other.keys() - cols
        jkeys = lkeys & rkeys
        lkeys = lkeys - jkeys
        rkeys = rkeys - jkeys
        
        if len(cols):
            lxs, lids = self._listby(lcols)
            rxs, rids = other._listby(rcols)
            ls = len(lxs)
            rs = len(rxs)
            l = 0
            r = 0
            res = []
            while l<ls and r<rs:
                while l<ls and r<rs and cmp(lxs[l],rxs[r]) == -1:
                    l+=1
                while l<ls and r<rs and cmp(lxs[l],rxs[r]) == 1:
                    r+=1
                if l<ls and r<rs and lxs[l] == rxs[r]:
                    res.append((lxs[l], lids[l], rids[r]))
                    r+=1
                    l+=1
            if len(res) == 0:
                return dictable([], cols + lkeys + rkeys + jkeys)
            xs, lids, rids = zip(*res)
            ns = [len(l)*len(r) for l, r in zip(lids, rids)]
            rtn = dictable(sum([[x]*n for x,n in zip(xs,ns)], []), cols)
        else:
            rtn = dictable()
            lids = [range(len(self))]; rids = [range(len(other))]
        for k in lkeys:
            v= self[k]
            rtn[k] = sum([[v[l] for l in lid for r in rid] for lid, rid in zip(lids, rids)], [])
        for k in rkeys:
            v= other[k]
            rtn[k] = sum([[v[r] for l in lid for r in rid] for lid, rid in zip(lids, rids)], [])            
        for k in jkeys:
            if (is_str(mode) and mode[0].lower() == 'l') or mode == 0:
                v = self[k]
                rtn[k] = sum([[v[l] for l in lid for r in rid] for lid, rid in zip(lids, rids)], [])
            elif (is_str(mode) and mode[0].lower() == 'r') or mode == 1:
                v = other[k]
                rtn[k] = sum([[v[r] for l in lid for r in rid] for lid, rid in zip(lids, rids)], [])
            elif callable(mode):
                lv = self[k]
                rv = other[k]
                rtn[k] = sum([[mode(lv[l], rv[r]) for l in lid for r in rid] for lid, rid in zip(lids, rids)], [])            
            else:                
                lv = self[k]
                rv = other[k]
                rtn[k] = sum([[(lv[l], rv[r]) for l in lid for r in rid] for lid, rid in zip(lids, rids)], [])            
        return rtn
    
    def xor(self, other, lcols = None, rcols = None, mode = 'l'):
        """
        returns what is in lhs but NOT in rhs (or vice versa if mode = 'r'). Together with inner joining, can be used as left/right join

        :Examples:
        --------------
        >>> from pyg import *
        >>> self = dictable(a = [1,2,3,4])
        >>> other = dictable(a = [1,2,3,5])
        >>> assert self.xor(other) == dictable(a = 4) # this is in lhs but not in rhs
        >>> assert self.xor(other, lcols = lambda a: a * 2, rcols = 'a') == dictable(a = [2,3,4]) # fit can be done using formulae rather than actual columns

        The XOR functionality can be performed using quotient (divide):
        >>> assert lhs/rhs == dictable(a = 4)
        >>> assert rhs/lhs == dictable(a = 5)

        >>> rhs = dictable(a = [1,2], b = [3,4])
        >>> left_join_can_be_done_simply_as = lhs * rhs + lhs/rhs


        :Parameters:
        ----------------
        other : dictable (or something that can be turned to one)
            what we exclude with.
        lcols : str/list of strs, optional
            the left columns/formulae on which we match. The default is None.
        rcols : str/list of strs, optional
            the right columns/formulae on which we match. The default is None.
        mode : string, optional
            When set to 'r', performs xor the other way. The default is 'l'.

        :Returns:
        -------
        dictable
            a dictable containing what is in self but not in ther other dictable.

        """
        if not isinstance(other, dictable):
            other = dictable(other)
        if lcols is None:
            lcols = self.keys() & other.keys()
        if rcols is None:
            rcols = lcols
        lcols = as_tuple(lcols); rcols = as_tuple(rcols)
        if len(lcols)!=len(rcols):
            raise ValueError('cannot xor-join when cols on either side mismatch in length %s vs %s'%(lcols, rcols))
        if len(lcols) == 0:
            return self
        lxs, lids = self._listby(lcols)
        rxs, rids = other._listby(rcols)
        mode = 1 if (is_str(mode) and mode[0].lower() == 'r') or mode == 1 else 0
        ls = len(lxs)
        rs = len(rxs)
        l = 0
        r = 0
        res = []
        while l<ls and r<rs:
            while l<ls and r<rs and cmp(lxs[l],rxs[r]) == -1:
                if mode == 0:
                    res.append(lids[l])
                l+=1
            while l<ls and r<rs and cmp(lxs[l],rxs[r]) == 1:
                if mode == 1:
                    res.append(rids[r])                    
                r+=1
            if l<ls and r<rs and lxs[l] == rxs[r]:
                r+=1
                l+=1
        if mode == 0:
            if l<ls:
                res.extend(lids[l:])
            return self[sum(res, [])]
        else:
            if r<rs:
                res.extend(rids[r:])
            return other[sum(res, [])]

    __mul__ = join
    __truediv__ = xor
    __div__ = xor
    
    def xyz(self, x, y, z, agg = None):
        """
        
        pivot table functionality.
        
        :Parameters:
        ----------------
        x : str/list of str
            unique keys per each row
        y : str
            unique key per each column
        z : str/callable
            A column in the table or an evaluated quantity per each row
        agg : None/callable or list of callables, optional
            Each (x,y) cell can potentially contain multiple z values. so if agg = None, a list is returned
            If you want the data aggregated in any way, then supply an aggregating function(s)

        :Returns:
        -------
        A dictable which is a pivot table of the original data
        

        :Example:
        -------
        >>> from pyg import *
        >>> timetable_as_list = dictable(x = [1,2,3]) * dictable(y = [1,2,3]) 
        >>> timetable = timetable_as_list.xyz('x','y',lambda x, y: x * y)
        >>> assert timetable = dictable(x = [1,2,3], )

        :Example:
        -------
        >>> self = dictable(x = [1,2,3]) * dictable(y = [1,2,3]) 
        >>> x = 'x'; y = 'y'; z = lambda x, y: x * y
        >>> self.exc(lambda x, y: x+y==5).xyz(x,y,z, len)
        
        """
        if not is_strs(x):
            raise ValueError('x must be columns %s'%x)
        agg = as_list(agg)
        x = as_tuple(x)
        xykeys = x + as_tuple(y)
        xys, ids = self._listby(xykeys)
        zs = self[z]
        y_ = y if is_str(y) else '_columns'
        rs = dictable(xys, x + (y_,))        
        ys = rs[as_list(y_)].listby(y_)
        y2id = dict(zip(ys[y_], range(len(ys))))
        xs, yids = rs._listby(x)
        res = [[None for _ in range(len(ys))] for _ in range(len(xs))]
        for i in range(len(xs)):
            for j in yids[i]:
                xy = xys[j]
                k = y2id[xy[-1]]
                value = [zs[id_] for id_ in ids[j]]
                if agg:
                    for a in agg:
                        value = a(value)
                res[i][k] = value
        dx = dictable(xs, x)
        dy = dictable(res, y2id.keys())
        dx.update(dy)
        return dx
    
    pivot = xyz
    
    def unpivot(self, x, y, z):
        """
        undoes self.xyz / self.pivot
        
        :Example:
        -------
        >>> from pyg import *
        >>> orig = (dictable(x = [1,2,3,4]) * dict(y = [1,2,3,4,5]))(z = lambda x, y: x*y)
        >>> pivot = orig.xyz('x', 'y', 'z', last)
        >>> unpivot = pivot.unpivot('x','y','z').do(int, 'y') # the conversion to column names mean y is now string... so we convert back to int
        >>> assert orig == unpivot

        :Parameters:
        ----------------
        x : str/list of strings
            list of keys in the pivot table.
        y : str
            name of the columns that wil be used for the values that are currently column headers.
        z : str
            name of the column that describes the data currently within the pivot table.

        :Returns:
        -------
        dictable

        """
        xcols = as_list(x)
        if isinstance(y, dict) and len(y) == 1:
            y, ycols = list(y.items())[0]
        else:
            ycols = self.keys() - x
        ycols = as_tuple(ycols)
        n = len(ycols)

        res = dictable({k: sum([[row[k]]*n for row in self], []) for k in xcols})
        res[y] = ycols * len(self)
        res[z] = sum([[row[ycol] for ycol in ycols] for row in self], [])
        return res
    

            

def is_dictable(value):
    return isinstance(value, dictable)

