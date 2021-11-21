from pyg.base import acell, is_strs, is_date, ulist, logger, tree_update, cell_clear, dt, as_list, get_DAG, topological_sort, add_edge, del_edge, eq, waiter
from pyg.base import cell_item, cell_inputs, Dict
from pyg.base._cell import is_pairs, GRAPH, _GAD, UPDATED, _pk, _updated

from pyg.mongo._q import _id, q, _deleted
from pyg.mongo._table import mongo_table
from pyg.mongo._db_cell import db_cell, _load_asof, get_cell
from functools import partial

# import networkx as nx

_db = 'db'

__all__ = ['db_aload', 'db_asave', 'db_acell']

async def db_asave(value):
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
    if isinstance(value, db_acell):
        return await value.save()
    elif isinstance(value, (tuple, list)):
        return type(value)(await waiter([db_asave(v) for v in value]))
    elif isinstance(value, dict):
        saved = await waiter({k : db_asave(v) for k, v in value.items()})
        return type(value)(**saved)
    else:
        return value

async def db_aload(value, mode = 0):
    """
    loads a db_cell from the database. Iterates through lists and dicts
    
    :Parameters:
    ------------
    value: obj
        db_cell (or list/dict of) to be loaded 
    
    mode: int
        loading mode -1: dont load, +1: load and throw an exception if not found, 0: load if found
    
    """
    if isinstance(value, db_acell):
        return await value.load(mode)
    elif isinstance(value, (tuple, list)):
        return type(value)(await waiter([db_aload(v, mode = mode) for v in value]))
    elif isinstance(value, dict):
        loaded = await waiter({k : db_aload(v, mode = mode) for k, v in value.items()})
        return type(value)(**loaded)
    else:
        return value
    
async def _async_load_asof(table, kwargs, deleted):
    t = table.inc(kwargs)    
    if await t.count() == 0:
        raise ValueError('no cells found matching %s'%kwargs)
    live = t(deleted = False)
    if deleted in (True, None): # we just want live values
        live_count = await live.count()    
        if live_count == 0:
            raise ValueError('no undeleted cells found matching %s'%kwargs)        
        elif live_count>1:
            raise ValueError('multiple cells found matching %s'%kwargs)
        res = await live[0]
    else:
        history = t(deleted = deleted) #cells alive at deleted
        history_count = await history.count()
        if history_count == 0:
            if live_count == 0:
                raise ValueError('no undeleted cells found matching %s'%kwargs)        
            elif live_count>1:
                raise ValueError('multiple cells found matching %s'%kwargs)
            elif deleted is True:
                raise ValueError('No deleted cells are avaialble but there is a live document matching %s. set delete = False to fetch it'%kwargs)
            res = await live[0]
        else:
            if history_count > 1 and deleted is True:
                raise ValueError('multiple historic cells are avaialble %s. set delete = DATE to find the cell on that date'%kwargs)
            res = await history.sort('deleted')[0]
    return res


class db_acell(acell):
    """
    a db_acell is a specialized cell with a 'db' member pointing to a database where cell is to be stored.    
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
    >>> people().reset.drop()    

    """

    def __init__(self, function = None, output = None, db = None, **kwargs):
        if db is not None:
            if not isinstance(db, partial):
                raise ValueError('db must be a partial of a function like mongo_table initializing a mongo cursor')
            super(db_acell, self).__init__(function = function, output = output, db = db, **kwargs)
        else:
            self[_db] = None
            super(db_acell, self).__init__(function = function, output = output, **kwargs)
        
    @property
    def _pk(self):
        if self.get(_db) is None:
            return super(db_cell, self)._pk
        else:
            return self.db.keywords.get(_pk)        

    @property
    def _address(self):
        """
        :Example:
        ----------
        >>> from pyg import *
        >>> self = db_acell(db = partial(mongo_table, 'test', 'test', pk = 'key'), key = 1)
        >>> db = self.db()
        >>> self._address
        >>> self._reference()
        >>> self.get('key')
        
        :Returns:
        -------
        tuple
            returns a tuple representing the unique address of the cell.
        """
        if self.get(_db) is None:
            return super(db_acell, self)._address
        db = self.db(asynch = True)
        return db.address + tuple([(key, self.get(key)) for key in db._pk])


    def _clear(self):
        """
        Removes most of the data from the cell. Just keeps it so that we have enough data to load it back from the database

        :Returns:
        -------
        db_cell
            skeletal reference to the database

        """
        if self.get(_db) is None: 
            return super(db_acell, self)._clear()
        else:
            return self[[_db] + self.db(asynch = True)._pk]


    async def save(self):
        if self.get(_db) is None:
            return super(db_acell, self).save()
        address = self._address
        doc = (self - _deleted)
        db = self.db(asynch = True)
        missing = ulist(db._pk) - self.keys()
        if len(missing):
            logger.warning('WARN: document not saved as some keys are missing %s'%missing)
            return self            
        ref = type(doc)(**cell_clear(dict(doc)))
        try:
            updated = await db.update_one(ref)
        except Exception:
            updated = await db.update_one(ref-_id)
        doc[_id] = updated[_id]
        GRAPH[address] = doc
        return doc
                
        
    async def load(self, mode = 0):
        """
        loads a document from the database and updates various keys.
        
        :Persistency:
        -------------
        Since we want to avoid hitting the database, there is a singleton GRAPH, a dict, storing the cells by their address.
        Every time we load/save from/to Mongo, we also update GRAPH.
        
        We use the GRAPH often so if you want to FORCE the cell to go to the database when loading, use this:

        >>> cell.load(-1) 
        >>> cell.load(-1).load(0)  # clear GRAPH and load from db
        >>> cell.load([0])     # same thing: clear GRAPH and then load if available

        :Merge of cached cell and calling cell:
        ----------------------------------------
        Once we load from memory (either MongoDB or GRAPH), we tree_update the cached cell with the new values in the current cell.
        This means that it is next to impossible to actually *delete* keys. If you want to delete keys in a cell/cells in the database, you need to:
        
        >>> del db.inc(filters)['key.subkey']

        :Parameters:
        ----------
        mode : int , dataetime, optional
            if -1, then does not load and clears the GRAPH
            if 0, then will load from database if found. If not found, will return original document
            if 1, then will throw an exception if no document is found in the database
            if mode is a date, will return the version alive at that date 
            The default is 0.
            
            IF you enclose any of these in a list, then GRAPH is cleared prior to running and the database is called.
    
        :Returns:
        -------
        document

        """
        if self.get(_db) is None:
            return super(db_acell, self).load(mode = mode)
        if isinstance(mode, (list, tuple)):
            if len(mode) == 0:
                mode = [0]
            if len(mode) == 1 or (len(mode)==2 and mode[0] == -1):
                res = await self.load(-1)
                return await res.load(mode[-1])
            else:
                raise ValueError('mode can only be of the form [], [mode] or [-1, mode]')
        db = self.db(asynch = True)
        pk = ulist(db._pk)
        missing = pk - self.keys()
        if len(missing):
            logger.warning('WARN: document not loaded as some keys are missing %s'%missing)
            return self            
        address = self._address
        kwargs = {k : self[k] for k in pk}
        if mode == -1:
            if address in GRAPH:
                del GRAPH[address]
            return self
        if address not in GRAPH:
            if is_date(mode):
                GRAPH[address] = await _async_load_asof(db.reset, kwargs, deleted = mode)
            else:
                try:
                    GRAPH[address] = await db[kwargs]
                except Exception:
                    if mode in (1, True):
                        raise ValueError('no cells found matching %s'%kwargs)
                    else:
                        return self         
        if address in GRAPH:
            saved = GRAPH[address]
            if saved.get(_updated) is None and self.get(_updated) is None:
                res = tree_update(self, dict(saved), ignore = [None])
            elif saved.get(_updated) is not None and (self.get(_updated) is None or saved.get(_updated) > self.get(_updated)):
                res = tree_update(self, dict(saved), ignore = [None])
            else:
                res = tree_update(saved, dict(self), ignore = [None])
            if self.function is None:
                res.function = saved.function
            return res
        return self        

    async def push(self):        
        me = self._address
        res = await self.go() # run me on my own as I am not part of the push
        await acell_push(me, exc = 0)
        return res


async def acell_push(nodes = None, exc = None):
    global UPDATED
    if nodes is None:
        nodes = UPDATED.keys()
    generations = topological_sort(get_DAG(), as_list(nodes))['gen2node']
    for i, children in sorted(generations.items())[1:]: # we skop the first generation... we just calculated it
        GRAPH.update(await waiter({child : GRAPH[child].go() for child in children}))            
        for child in children:
            if child in UPDATED:
                del UPDATED[child]
