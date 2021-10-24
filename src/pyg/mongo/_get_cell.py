from pyg.base import cell_item
from pyg.base._cell import is_pairs
from pyg.mongo._table import mongo_table
from pyg.mongo._db_cell import _load_asof, GRAPH



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
    # if _deleted is None:
    #     address = t.address + tuple(sorted(kwargs.items()))
    #     if address in GRAPH:
    #         return GRAPH[address]
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