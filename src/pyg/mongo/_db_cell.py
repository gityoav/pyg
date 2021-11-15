from pyg.base import cell, is_strs, is_date, ulist, logger, tree_update, cell_clear, dt, as_list, get_DAG, descendants, add_edge, del_edge, eq
from pyg.base import cell_item, cell_inputs, Dict
from pyg.base._cell import is_pairs, GRAPH, _GAD, UPDATED, _pk
from pyg.mongo._q import _id, q, _deleted
from pyg.mongo._table import mongo_table
from functools import partial

# import networkx as nx

_updated = 'updated'
_db = 'db'

__all__ = ['db_load', 'db_save', 'db_cell']

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
        return type(value)([db_load(v, mode = mode) for v in value])
    elif isinstance(value, dict):
        return type(value)(**{k : db_load(v, mode = mode) for k, v in value.items()})
    else:
        return value
    
def _load_asof(table, kwargs, deleted):
    t = table.inc(kwargs)
    if len(t) == 0:
        raise ValueError('no cells found matching %s'%kwargs)
    live = t.inc(q.deleted.not_exists)
    if deleted is None:
        if len(live) == 0:
            raise ValueError('no undeleted cells found matching %s'%kwargs)        
        elif len(live)>1:
            raise ValueError('multiple cells found matching %s'%kwargs)
        res = live[0]
    else:
        history = t.inc(q.deleted > deleted) #cells alive at deleted
        if len(history) == 0:
            if len(live) == 0:
                raise ValueError('no undeleted cells found matching %s'%kwargs)        
            elif len(live)>1:
                raise ValueError('multiple cells found matching %s'%kwargs)
            res = live[0]
        else:
            res = history.sort('deleted')[0]
    return res


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
            if not isinstance(db, partial):
                raise ValueError('db must be a partial of a function like mongo_table initializing a mongo cursor')
            super(db_cell, self).__init__(function = function, output = output, db = db, **kwargs)
        else:
            self[_db] = None
            super(db_cell, self).__init__(function = function, output = output, **kwargs)

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
        if self.get(_db) is None:
            return super(db_cell, self)._address
        db = self.db()
        return db.address + tuple([(key, self.get(key)) for key in db.pk])


    def _clear(self):
        """
        Removes most of the data from the cell. Just keeps it so that we have enough data to load it back from the database

        :Returns:
        -------
        db_cell
            skeletal reference to the database

        """
        if self.get(_db) is None: 
            return super(db_cell, self)._clear()
        else:
            return self[[_db] + self.db().pk]


    def save(self):
        if self.get(_db) is None:
            return super(db_cell, self).save()
        address = self._address
        doc = (self - _deleted)
        db = self.db()
        missing = ulist(db.pk) - self.keys()
        if len(missing):
            logger.warning('WARN: document not saved as some keys are missing %s'%missing)
            return self            
        ref = type(doc)(**cell_clear(dict(doc)))
        try:
            doc[_id] = db.update_one(ref)[_id]
        except Exception:
            doc[_id] = db.update_one(ref-_id)[_id]
        GRAPH[address] = doc
        return doc
                
        
    def load(self, mode = 0):
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
            return super(db_cell, self).load(mode = mode)
        if isinstance(mode, (list, tuple)):
            if len(mode) == 0:
                mode = [0]
            if len(mode) == 1 or (len(mode)==2 and mode[0] == -1):
                res = self.load(-1)
                return res.load(mode[-1])
            else:
                raise ValueError('mode can only be of the form [], [mode] or [-1, mode]')
        db = self.db()
        pk = ulist(db.pk)
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
                GRAPH[address] = _load_asof(db.raw, kwargs, deleted = mode)
            else:
                try:
                    GRAPH[address] = db[kwargs]
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
    
    def go(self, go = 1, mode = 0, **kwargs):
        res = (self + kwargs)._go(go = go, mode = mode)
        address = res._address
        if address in UPDATED:
            res[_updated] = UPDATED[address] 
        else: 
            res[_updated] = dt()
        return res.save()
        

    def push(self):        
        me = self._address
        res = self.go() # run me on my own as I am not part of the push
        cell_push(me, exc = 0)
        return res

    def bind(self, **bind):
        """
        bind adds key-words to the primary keys of a cell

        :Parameters:
        ----------
        bind : dict
            primary keys and their values.
            The value can be a callable function, transforming existing values

        :Returns:
        -------
        res : cell
            a cell with extra binding as primary keys.

        :Example:
        ---------
        >>> from pyg import *
        >>> db = partial(mongo_table, 'test', 'test', pk = 'key')
        >>> c = db_cell(passthru, data = 1, db = db, key = 'old_key')()
        >>> d = c.bind(key = 'key').go()
        >>> assert d.pk == ['key']
        >>> assert d._address in GRAPH
        >>> e = d.bind(key2 = lambda key: key + '1')()
        >>> assert e.pk == ['key', 'key2']
        >>> assert e._address == (('key', 'key'), ('key2', 'key1'))
        >>> assert e._address in GRAPH
        """
        db = self.get(_db)
        if db is None:
            return super(db_cell, self).bind(**bind)
        else:
            kw = self.db.keywords
            for k in bind: # we want to be able to override tables/db/url
                if k in ['db', 'table', 'url']:
                    kw[k] = bind.pop(k)
            pk = sorted(set(as_list(kw.get(_pk))) | set(bind.keys()))
            kw[_pk] = pk
            db = partial(db.func, *db.args, **kw)
            res = Dict({key: self.get(key) for key in pk})
            res = res(**bind)
            res[_db] = db
            return self + res


def cell_push(nodes = None, exc = None):
    global UPDATED
    if nodes is None:
        nodes = UPDATED.keys()
    children = [child for child in descendants(get_DAG(), nodes, exc = exc) if child is not None]
    for child in children:
        GRAPH[child] = (GRAPH[child] if child in GRAPH else get_cell(**dict(child))).go()
    for child in children:
        del UPDATED[child]




def cell_pull(nodes, types = cell):
    for node in as_list(nodes):
        node = node.pull()
        children = [get_cell(**dict(a)) for a in _GAD.get(node._address,[])]        
        cell_pull(children, types)
    return None        


def get_cell(table = None, db = None, url = None, deleted = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. In addition, can look at earlier versions using deleted.
    It is important to note that this DOES NOT go through the cache mechanism but goes to the database directly every time.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection). alternatively, you can just provide an address
    db : str
        name of database.
    url : TYPE, optional
        DESCRIPTION. The default is None.
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_cell('test','test', surname = 'brown').name == 'bob'
        
    """
    global GRAPH
    if is_pairs(table):
        params = dict(table)
        params.update({key: value for key, value in dict(db = db, url = url, deleted = deleted).items() if value is not None})
        params.update(kwargs)
        return get_cell(**params)
    
    if db is not None and table is not None:
        t = mongo_table(db = db, table = table, url = url)
        return _load_asof(t, kwargs, deleted)
    else:
        pk = kwargs.pop('pk', None)
        if pk is None:
            address = tuple(sorted(kwargs.items()))
            res = GRAPH[address]
        else:
            address = tuple([(key, kwargs.get(key)) for key in sorted(as_list(pk))])
            res = GRAPH[address]
        return res


def get_data(table = None, db = None, url = None, deleted = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. In addition, can look at earlier versions using deleted.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection).
    db : str
        name of database.
    url : TYPE, optional
        DESCRIPTION. The default is None.
    deleted : datetime/None, optional
        The date corresponding to the version of the cell we want
        None = live cell
        otherwise, the cell that was first deleted after this date.
    **kwargs : keywords
        key words for grabbing the cell.

    :Returns:
    -------
    The document

    :Example:
    ---------
    >>> from pyg import *
    >>> people = partial(mongo_table, db = 'test', table = 'test', pk = ['name', 'surname'])
    >>> people().raw.drop()
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_data('test','test', surname = 'brown') is None
        
    """    
    return cell_item(get_cell(table, db = db, url = url, deleted = deleted, **kwargs), key = 'data')
