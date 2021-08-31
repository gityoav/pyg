from pyg.base import dt, dt_bump, wrapper, as_list, getcallargs, getargs
from pyg.mongo._db_cell import db_cell, _updated
from pyg.mongo._table import mongo_table
from functools import partial

__all__ = ['periodic_cell']
_period = 'period'

class periodic_cell(db_cell):
    """
    periodic_cell inherits from db_cell its ability to save itself in MongoDb using its db members
    Its calculation schedule depends on when it was last updated. 
    
    :Example:
    ---------
    >>> from pyg import *
    >>> c = periodic_cell(lambda a: a + 1, a = 0)
    
    We now assert it needs to be calculated and calculate it...

    >>> assert c.run()
    >>> c = c.go()
    >>> assert c.data == 1
    >>> assert not c.run()
    
    Now let us cheat and tell it, it was last run 3 days ago...
    
    >>> c.updated = dt(-3)
    >>> assert c.run()
            
    """
    def __init__(self, function = None, output = None, db = None, period = '1b', updated = None, **kwargs):
        self[_updated] = updated
        self[_period] = period 
        super(periodic_cell, self).__init__(function, output = output, db = db, **kwargs)
            
    def run(self):
        time = dt()
        if self[_updated] is None or dt_bump(self[_updated], self[_period]) < time:
            return True
        return super(periodic_cell, self).run() 


class periodic_cache(wrapper):
    def __init__(self, function = None, pk = None, period = '1b', db = 'cache', table = 'cache'):
        pk = as_list(pk)
        if 'function' not in pk:
            pk = ['function'] + pk
        args = getargs(function)
        for key in pk:
            if not (key == 'function' or key in args):
                raise ValueError('cannot cache function on a key "%s" which is not in its function signature %s'%(key, args))
            assert key == 'function' or key in args
        super(periodic_cache, self).__init__(function = function, pk = pk, period = period, db = db, table = table)

    @property
    def _db(self):
        return partial(mongo_table, table = self.table, db = self.db, pk = self.pk)
    
    def wrapped(self, *args, **kwargs):
        callargs = getcallargs(self.function, *args, **kwargs)
        db = self._db
        c = periodic_cell(self.function, db = db, period = self.period, **callargs)()
        return c.get('data')
