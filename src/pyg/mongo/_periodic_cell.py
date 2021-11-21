from pyg.base import dt, dt_bump
from pyg.mongo._db_cell import db_cell, _updated
from pyg.mongo._db_acell import db_acell

__all__ = ['periodic_cell', 'periodic_acell']
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

class periodic_acell(db_acell):
    """
    periodic_acell inherits from db_acell its ability to save itself in MongoDb using its db members
    Its calculation schedule depends on when it was last updated. 
    
    :Example:
    ---------
    >>> from pyg import *
    >>> db = partial(mongo_table, db = 'test', table = 'test', pk = 'key')
    >>> c = periodic_acell(add_, a = 1, b = 2, db = db, key = 1)
    >>> await c()

    
    We now assert it needs to be calculated and calculate it...

    >>> assert c.run()
    >>> c = await c.go()
    >>> assert c.data == 1
    >>> assert not c.run()
    
    Now let us cheat and tell it, it was last run 3 days ago...
    
    >>> c.updated = dt(-3)
    >>> assert c.run()
            
    """
    def __init__(self, function = None, output = None, db = None, period = '1b', updated = None, **kwargs):
        self[_updated] = updated
        self[_period] = period 
        super(periodic_acell, self).__init__(function, output = output, db = db, **kwargs)
            
    def run(self):
        time = dt()
        if self[_updated] is None or dt_bump(self[_updated], self[_period]) < time:
            return True
        return super(periodic_acell, self).run() 
