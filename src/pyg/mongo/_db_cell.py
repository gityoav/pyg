from pyg.base import cell, is_strs, is_date, ulist, logger, tree_update, cell_clear, dt, as_list, get_DAG, descendants, add_edge
from pyg.base import cell_item
from pyg.base._cell import is_pairs
from pyg.mongo._q import _deleted, _id, q
from pyg.mongo._table import mongo_table


# import networkx as nx

_updated = 'updated'
_db = 'db'

GRAPH = {}

__all__ = ['db_load', 'db_save', 'db_cell', 'GRAPH']


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
    
def _load_asof(table, kwargs, _deleted):
    t = table.inc(kwargs)
    if len(t) == 0:
        raise ValueError('no cells found matching %s'%kwargs)
    live = t.inc(q._deleted.not_exists)
    if _deleted is None:
        if len(live) == 0:
            raise ValueError('no undeleted cells found matching %s'%kwargs)        
        elif len(live)>1:
            raise ValueError('multiple cells found matching %s'%kwargs)
        res = live[0]
    else:
        history = t.inc(q._deleted >_deleted) #cells alive at _deleted
        if len(history) == 0:
            if len(live) == 0:
                raise ValueError('no undeleted cells found matching %s'%kwargs)        
            elif len(live)>1:
                raise ValueError('multiple cells found matching %s'%kwargs)
            res = live[0]
        else:
            res = history.sort('_deleted')[0]
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
        elif is_strs(self.db):
            return tuple([(key, self.get(key)) for key in as_list(self.db)])
        db = self.db()
        return db.address + tuple([(key, self.get(key)) for key in db.pk])

    def _clear(self):
        if not callable(self.get(_db)): 
            return super(db_cell, self)._clear()
        else:
            return self[[_db] + self.db().pk]


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
        ref = type(doc)(**cell_clear(dict(doc)))
        try:
            doc[_id] = db.update_one(ref)[_id]
        except Exception:
            doc[_id] = db.update_one(ref-_id)[_id]
        new = doc
        GRAPH[address] = new
        return new
                
        
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
        if isinstance(mode, (list, tuple)):
            if len(mode) == 0:
                mode = [0]
            if len(mode) == 1 or (len(mode)==2 and mode[0] == -1):
                res = self.load(-1)
                return res.load(mode[-1])
            else:
                raise ValueError('mode can only be of the form [], [mode] or [-1, mode]')
        if not callable(self.get(_db)):
            return self
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
                GRAPH[address] = _load_asof(db.raw, kwargs, _deleted = mode)
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
            if saved.get(_updated) is not None and self.get(_updated) is not None and saved.get(_updated) > self.get(_updated):
                res = tree_update(self, dict(saved), ignore = [None])
            else:
                res = tree_update(saved, dict(self), ignore = [None])
            if self.function is None:
                res.function = saved.function
            return res
        return self
    
    def go(self, go = 1, mode = 0, **kwargs):
        res = self._go(go = go, mode = mode, **kwargs)
        res[_updated] = dt()
        return res.save()
    
    def pull(self, inputs = True):
        """
        :Example:
        ---------
        >>> from pyg import * 
        >>> db = partial(mongo_table, 'test', 'test', pk  = 'key')
        >>> c = db_cell(add_, a = 1, b = 2, db = db, key = 'c')().register()
        >>> d = db_cell(add_, a = 1, b = c, db = db, key = 'd')().register()
        >>> e = db_cell(add_, a = c, b = d, db = db, key = 'e')().register()
        >>> f = db_cell(add_, a = e, b = d, db = db, key = 'f')().register()
        
        >>> nodes = nx.descendants(DAG(), c._address)
        >>> sub = nx.algorithms.dag.topological_sort(DAG().subgraph(nodes))
        >>> for node in sub:
        >>>     print(node)

        self = d.register()
        g = DIGRAPH()
        g.edges
        inputs = True
        
        Parameters
        ----------
        inputs : TYPE, optional
            DESCRIPTION. The default is True.

        Returns
        -------
        cell
            DESCRIPTION.

        """
        if inputs is True:
            inputs = {key: None for key in self._inputs}
        elif isinstance(inputs, (str, list)):
            inputs = {key: None for key in as_list(inputs)}
        else:
            inputs = {}
        me = self._address
        dag = get_DAG()
        for key, value in inputs.items():
            parent = self.get(key)
            if isinstance(parent, db_cell):
                add_edge(parent._address, me, dag = dag)
        return self

    def push(self):
        children = descendants(get_DAG(), self._address)
        GRAPH[children[0]] = res = self.go()
        for child in children[1:]:
            GRAPH[child] = get_cell(**dict(child)).go()
        return res
        
def get_cell(table, db, url = None, _deleted = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. In addition, can look at earlier versions using _deleted.
    It is important to note that this DOES NOT go through the cache mechanism but goes to the database directly every time.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection). alternatively, you can just provide an address
    db : str
        name of database.
    url : TYPE, optional
        DESCRIPTION. The default is None.
    _deleted : datetime/None, optional
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
    if is_pairs(table):
        params = dict(table)
        params.update({key: value for key, value in dict(db = db, url = url, _deleted = _deleted).items() if value is not None})
        params.update(kwargs)
        return get_cell(**params)
    
    t = mongo_table(db = db, table = table, url = url)
    return _load_asof(t, kwargs, _deleted)
    

def get_data(table, db, url = None, _deleted = None, **kwargs):
    """
    retrieves a cell from a table in a database based on its key words. In addition, can look at earlier versions using _deleted.

    :Parameters:
    ----------
    table : str
        name of table (Mongo collection).
    db : str
        name of database.
    url : TYPE, optional
        DESCRIPTION. The default is None.
    _deleted : datetime/None, optional
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
    return cell_item(get_cell(table, db = db, url = url, _deleted = _deleted, **kwargs), key = 'data')
