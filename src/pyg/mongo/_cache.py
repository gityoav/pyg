from pyg.base import wrapper, getargs, as_list, getcallargs, cell_item
from pyg.mongo._table import mongo_table
from pyg.mongo._periodic_cell import periodic_cell
from functools import partial

class cell_cache(wrapper):
    """
    Rather than write this:

    >>> def function(a, b):
    >>>     return a + b        

    >>> db = partial(mongo_table, db = 'db', table = 'table', pk = ['key'])
    >>> periodic_cell(function, a =  1, b = 2, db = db, key = 'my key')
        
    You can write this:
        
    >>> f = cell_cache(function, db = 'db', table = 'table', pk = 'key')
    >>> f(a = 1, b = 2, key = 'my key')

    If we are interested just in values...
    
    >>> f = db_cache(function, db = 'db', table = 'table', pk = 'key')
    >>> assert f(a = 1, b = 2, key = 'my key') == 3

    :Parameters:
    ------------
    pk: str/list
        primary keys of the table, using the keyword arguments of the function. If missing, uses all keywords

    db: str
        name of database where data is to be stored

    table: str
        name of table/collection where data is stored. If not provided, defaults to the function's name

    cell: cell
        type of cell to use when caching the data

    cell_kwargs: dict
        parameters for the cell determining its operation, e.g. for periodic_cell, the periodicity

    :Example:
    ---------
    >>> from pyg import *
    >>> @db_cache
    >>> def f(a,b):
    >>>     return a+b
    
    >>> assert f(1,2) == 3

    >>> @cell_cache
    >>> def f(a,b):
    >>>     return a+b
    >>> assert f(1,2).data == 3

    >>> @cell_cache(pk = 'key')
    >>> def f(a,b):
    >>>     return a+b
    >>> f(1,2, key = 'key', go = 1)

    
    """
    def __init__(self, function = None, db = 'cache', table = None, pk = None, cell = periodic_cell, cell_kwargs = None, external = None):
        cell_kwargs  = cell_kwargs or {}
        super(cell_cache, self).__init__(function = function, pk = pk, db = db, table = table, cell = cell, cell_kwargs = cell_kwargs, external = external)

    @property
    def _pk(self):
        if self.pk is None and self.function is not None:
            args = getargs(self.function)
            self.pk = args
        return as_list(self.pk)

    @property
    def _external(self):
        if self.external is None and self.function is not None:
            args = getargs(self.function)
            self.external = [key for key in self._pk if key not in args]
        return self.external

    @property
    def _table(self):
        if self.table is None and self.function is not None:
            return self.function.__name__
        else:
            return self.table
    
    @property
    def _db(self):
        return partial(mongo_table, table = self._table, db = self.db, pk = self._pk)
    
    def wrapped(self, *args, **kwargs):
        go = kwargs.pop('go',0)
        mode = kwargs.pop('mode',0)
        external = self._external
        external_kwargs = {key : value for key, value in kwargs.items() if key in external}
        kwargs = {key : value for key, value in kwargs.items() if key not in external}
        callargs = getcallargs(self.function, *args, **kwargs)
        db = self._db
        external_kwargs.update(self.cell_kwargs)
        external_kwargs.update(callargs)
        return self.cell(self.function, db = db, **external_kwargs)(go = go, mode = mode)

class db_cache(cell_cache):
    def wrapped(self, *args, **kwargs):
        return cell_item(super(db_cache, self).wrapped(*args, **kwargs))
    


