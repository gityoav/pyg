from pyg.base import cell_item
from pyg.mongo._table import mongo_table
from pyg.mongo._q import q

# def _fetch(cursor, output):
#     if len(cursor) == 0:
#         raise ValueError('No documents found %s'%cursor)
#     if output is True or output == 1:
#         return cursor[0]
#     elif is_str(output) or isinstance(output, list):
#         return Dict(cursor[output][0])[output]
#     else:
#         raw = cursor.read(0, passthru)
#         return raw


def get_cell(table, db, url = None, _deleted = None, **kwargs):
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
    >>> brown = db_cell(db = people, name = 'bob', surname = 'brown', age = 39).save()
    >>> assert get_cell('test','test', surname = 'brown').name == 'bob'
        
    """
    t = mongo_table(db = db, table = table, url = url)
    t = t.inc(kwargs)
    if len(t) == 0:
        raise ValueError('no cells found matching %s'%kwargs)
    live = t.inc(q._deleted.not_exists)
    if _deleted is None:
        if len(live) == 0:
            raise ValueError('no undeleted cells found matching %s'%kwargs)        
        elif len(live)>1:
            raise ValueError('multiple cells found matching %s'%kwargs)
        return live[0]        
    else:
        history = t.inc(q._deleted >_deleted) #cells alive at _deleted
        if len(history) == 0:
            if len(live) == 0:
                raise ValueError('no undeleted cells found matching %s'%kwargs)        
            elif len(live)>1:
                raise ValueError('multiple cells found matching %s'%kwargs)
            return live[0]
        else:
            return history.sort('_deleted')[0]
        
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