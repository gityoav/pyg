from pyg.base import cell, is_strs, as_tuple, ulist, logger, tree_update
from pyg.mongo._q import _deleted, _id
_updated = 'updated'
_db = 'db'

_GRAPH = {}

__all__ = ['db_ref', 'db_load', 'db_save', 'db_cell']


def db_save(value):
    """
    saves a db_cell from the database. Will iterates through lists and dicts

    :Parameters:
    ------------
    value: obj
        db_cell (or list/dict of) to be loaded 
        
    :Example:
    ---------
    >>> from pyg import *
    >>> db = partial(mongo_table, table = 'test', db = 'test', pk = ['a','b'])
    >>> c = db_cell(add_, a = 2, b = 3, key = 'test', db = db)
    >>> c = db_save(c)    
    >>> assert get_cell('test', 'test', a = 2, b = 3).key == 'test'
        

    """
    if isinstance(value, db_cell):
        return value.save()
    elif isinstance(value, (tuple, list)):
        return type(value)([db_save(v) for v in value])
    elif isinstance(value, dict):
        return type(value)(**{k : db_save(v) for k, v in value.items()})
    else:
        return value

def db_load(value, mode = 0):
    """
    loads a db_cell from the database. Iterates through lists and dicts
    
    :Parameters:
    ------------
    value: obj
        db_cell (or list/dict of) to be loaded 
    
    mode: int
        loading mode -1: dont load, +1: load and throw an exception if not found, 0: load if found
    
    """
    if isinstance(value, db_cell):
        return value.load(mode)
    elif isinstance(value, (tuple, list)):
        return type(value)([db_load(v, mode) for v in value])
    elif isinstance(value, dict):
        return type(value)(**{k : db_load(v,mode) for k, v in value.items()})
    else:
        return value

def db_ref(value):
    """
    db_ref strips a db_cell so that it contains only the reference needed to its location in the database.
    When we save OTHER cells, referencing this cell, we apply db_ref and only save the bare data needed to reload cell
    
    :Example:
    ---------
    >>> from pyg import *
    >>> db = partial(mongo_table, table = 'test', db = 'test', pk = 'key')
    >>> c = db_cell(add_, a = 1, b = 2, key = 'key', db = db)()
    >>> assert c.data == 3    

    >>> bare = db_ref(c)
    >>> assert 'a' not in bare and 'b' not in bare and 'data' not in bare

    >>> reloaded = db_load(bare)
    >>> assert reloaded.a == 1 and reloaded.data == 3
    
    :Parameters:
    ------------
    value: obj
        db_cell (or list/dict of) to be made into reference

    """
    if isinstance(value, db_cell):
        return value._reference()
    elif isinstance(value, (tuple, list)):
        return type(value)([db_ref(v) for v in value])
    elif isinstance(value, dict):
        return type(value)(**{k : db_ref(v) for k, v in value.items()})
    else:
        return value


class db_cell(cell):
    """
    a db_cell is a specialized cell with a 'db' member pointing to a database where cell is to be stored.    
    We use this to implement save/load for the cell.
    
    It is important to recognize the duality in the design:
    - the job of the cell.db is to be able to save/load based on the primary keys.
    - the job of the cell is to provide the primary keys to the db object.
    
    The cell saves itself by 'presenting' itself to cell.db() and say... go on, load my data based on my keys. 
    
    :Example: saving & loading
    --------------------------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> anna = db_cell(db = people, name = 'anna', surname = 'abramzon', age = 46).save()
    >>> bob  = db_cell(db = people, name = 'bob', surname = 'brown', age = 25).save()
    >>> james = db_cell(db = people, name = 'james', surname = 'johnson', age = 39).save()


    Now we can pull the data directly from the database

    >>> people()['name', 'surname', 'age'][::]
    >>> dictable[3 x 4]
    >>> _id                     |age|name |surname 
    >>> 601e732e0ef13bec9cd8a6cb|39 |james|johnson 
    >>> 601e73db0ef13bec9cd8a6d4|46 |anna |abramzon
    >>> 601e73db0ef13bec9cd8a6d7|25 |bob  |brown       

    db_cell can implement a function:

    >>> def is_young(age):
    >>>    return age < 30
    >>> bob.function = is_young
    >>> bob = bob.go()
    >>> assert bob.data is True

    When run, it saves its new data to Mongo and we can load its own data:

    >>> new_cell_with_just_db_and_keys = db_cell(db = people, name = 'bob', surname = 'brown')
    >>> assert 'age' not in new_cell_with_just_db_and_keys 
    >>> now_with_the_data_from_database = new_cell_with_just_db_and_keys.load()
    >>> assert now_with_the_data_from_database.age == 25

    >>> people()['name', 'surname', 'age', 'data'][::]
    >>>  dictable[3 x 4]
    >>> _id                     |age|name |surname |data
    >>> 601e732e0ef13bec9cd8a6cb|39 |james|johnson |None
    >>> 601e73db0ef13bec9cd8a6d4|46 |anna |abramzon|None
    >>> 601e73db0ef13bec9cd8a6d7|25 |bob  |brown   |True
    >>> people().raw.drop()    

    """

    def __init__(self, function = None, output = None, db = None, **kwargs):
        if db is not None:
            super(db_cell, self).__init__(function = function, output = output, db = db, **kwargs)
        else:
            self[_db] = None
            super(db_cell, self).__init__(function = function, output = output, **kwargs)

    @property
    def _address(self):
        """
        :Example:
        ----------
        >>> from pyg import *
        >>> self = db_cell(db = partial(mongo_table, 'test', 'test', pk = 'key'), key = 1)
        >>> db = self.db()
        >>> self._address
        >>> self._reference()
        >>> self.get('key')
        
        :Returns:
        -------
        tuple
            returns a tuple representing the unique address of the cell.
        """
        if self.db is None:
            return None
        if is_strs(self.db):
            pk = as_tuple(self.db)
            return (None, None, None, None) + (pk, tuple([self.get(k) for k in pk]))
        db = self.db()
        pk = tuple(db.pk)
        address = db.address  + (pk, tuple([self.get(k) for k in pk])) 
        return address

    def _reference(self):
        return self[['db'] + self.db().pk]
    
    def __call__(self, go = 0, mode = 0, **kwargs):
        return (self + kwargs).load(mode).go(go)
    
    def save(self):
        if is_strs(self.db) or self.db is None:
            return self
        db = self.db()
        address = self._address
        missing = ulist(db.pk) - self.keys()
        if len(missing):
            logger.warning('WARN: document not saved as some keys are missing %s'%missing)
            return self            
        doc = (self - _deleted)
        ref = type(doc)(**db_ref(dict(doc)))
        try:
            doc[_id] = db.update_one(ref)[_id]
        except Exception:
            doc[_id] = db.update_one(ref-_id)[_id]
        new = doc
        _GRAPH[address] = new
        return new
    
    def load(self, mode = 0, keys = None):
        """
        loads a document from the database and updates various keys

        Parameters
        ----------
        mode : int , optional
            if -1, then does not load and skips this function
            if 0, then will load if found. If not found, will return original document
            if 1, then will throw an exception if no document is found in the database
            The default is 0.
        keys : str/list of str/True, optional
            determines which additional keys (other than output) are loaded onto the existing cell from the saved one. output keys are always loaded.
 
        Returns
        -------
        document

        """
        if mode == -1:
            return self
        if 'db' not in self or self.db is None:
            return self
        db = self.db()
        missing = ulist(db.pk) - self.keys()
        if len(missing):
            logger.warning('WARN: document not loaded as some keys are missing %s'%missing)
            return self            
        address = self._address
        if address not in _GRAPH:
            try:
                _GRAPH[address] = db[self]    
            except Exception:
                if mode in (1, True):
                    raise ValueError('unable to load %s'%list(address))                    
        if address in _GRAPH:
            saved = _GRAPH[address]
            res = tree_update(saved, dict(self), ignore = [None])
            if self.function is None:
                res.function = saved.function
            return res
        return self
