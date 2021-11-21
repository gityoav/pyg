
from pyg.base import logger, passthru, tree_update, dictable
from pyg.base import zipper, is_strs, is_dict, Dict, is_dictable, is_int, as_list, ulist
from pyg.mongo._q import _set, _id, _unset, _rename, _deleted, _data
from pyg.mongo._reader import mongo_reader
from pyg.mongo._base_reader import _pk, _items1, _dict1
import datetime


__all__ = ['mongo_cursor', 'mongo_pk_cursor']

class mongo_cursor(mongo_reader):
    """
    mongo_cursor is a souped-up combination of mongo.Cursor and mongo.Collection with a simple API.

    :Parameters:
    ----------------
    cursor : MongoDB cursor or MongoDB collection
        
    writer : True/False/string, optional
        The default is None.
        
        writer allows you to transform the data before saving it in Mongo. You can create a function yourself or use built-in options:
        
        - False: do nothing, save the document as is
        - True/None: use pyg.base.encode to encode objects. This will transform numpy array/dataframes into bytes that can be stored
        - '.csv': save dataframes into csv files and then save reference of these files to mongo
        - '.parquet' save dataframes into .parquet and np.ndarray into .npy files.
        
        For .csv and .parquet to work, you will need to specify WHERE the document is to be saved. This can be done either:
        
        - the document has a 'root' key, specifying the root.
        - you specify root by setting writer = 'c:/%name%surname.parquet'

    reader : callable or None, optional
        The default is None, using decode. Use reader = False to passthru
    query : dict, optional
        This is used to specify the Mongo query, e.g. q.a==1.
    **_ : 
    
        
    :Example:
    ---------
    >>> from pyg import *
    >>> cursor = mongo_table('test', 'test')
    >>> cursor.drop()

    ## insert some data
    
    >>> table = dictable(a = range(5)) * dictable(b = range(5))
    >>> cursor.insert_many(table)
    >>> cursor.set(c = lambda a, b: a * b)
    

    :filtering:
    -----------

    >>> assert len(cursor) == 25
    >>> assert len(cursor.find(a = 3)) == 5
    >>> assert len(cursor.exc(a = 3)) == 20
    >>> assert len(cursor.find(a = [3,2]).find(q.b<3)) == 6 ## can chain queries as well as use q to create complicated expressions
    
    :row access:
    -------------
    
    >>> cursor[0]
    
    {'_id': ObjectId('603aec85cd15e2c090c07b87'), 'a': 0, 'b': 0}
    
    >>> cursor[::] - '_id' == dictable(cursor) - '_id'
    
    >>> dictable[25 x 3]
    >>> a|b|c 
    >>> 0|0|0 
    >>> 0|1|0 
    >>> 0|2|0 
    >>> ...25 rows...
    >>> 4|2|8 
    >>> 4|3|12
    >>> 4|4|16
    
    :column access:
    ---------------
    >>> cursor[['a', 'b']]  ## just columns 'a' and 'b'
    >>> del cursor['c'] ## delete all c
    >>> cursor.set(c = lambda a, b: a * b)
    >>> assert cursor.find_one(a = 3, b = 2)[0].c == 6
    
    :Example: root specification
    ----------------------------
    >>> from pyg import *
    >>> t = mongo_table('test', 'test', writer = 'c:/temp/%name/%surname.parquet')
    >>> t.drop()
    >>> doc = dict(name = 'adam', surname = 'smith', ts = pd.Series(np.arange(10)))
    >>> t.insert_one(doc)
    >>> assert eq(pd_read_parquet('c:/temp/adam/smith/ts.parquet'), doc['ts'])
    >>> assert eq(t[0]['ts'], doc['ts'])
    >>> doc = dict(name = 'beth', surname = 'brown', a = np.arange(10))
    >>> t.insert_one(doc)
    
    Since mongo_cursor is too powerful, we also have a mongo_reader version which is read-only.
    
    
    """
    def delete_many(self, *args, **kwargs):
        """
        Equivalent to drop: deletes all documents the cursor currently points to.
        
        :Note: 
        ------
        If you want to drop a subset of the data, then use c.find(criteria).delete_many()

        :Returns:
        -------
        itself
        """
        target = self.inc(*args, **kwargs)
        n = target.count()
        spec = target._spec
        logger.info('INFO: deleting %i documents based on %s'%(n, spec))
        if n:
            target.collection.delete_many(spec)
        return self

        
    def delete_one(self, *args, **kwargs):
        """
        drops a specific record after verifying exactly one exists.

        :Parameters:
        ----------
        *args : query
        **kwargs : query

        :Returns:
        -------
        itself

        """
        c = self.find_one(*args, **kwargs)
        c.collection.delete_one(c._spec)
        return self

    drop = delete_many
    
    def _update_one(self, doc):
        """
        Uses the doc to find the existing document matching the doc. Then updates it. Throws an exception if no document found
        
        :Returns:
        ----------
        the updated document
        """
        update = self._write(doc)
        c = self.find_one(doc = update)
        update.pop(_id, None)
        self.collection.update_one(c._spec, {_set: update})
        return c[0]
    
    def update_one(self, doc, upsert = True):
        """
        - updates a document if an _id is present in doc.
        - insert a document if an _id is not present and upsert is true

        :Parameters:
        ----------
        doc : document
            doc to be upserted.
        upsert : bool, optional
            insert if no document present? The default is True.
        
        :Returns:
        -------
        doc
            document updated.

        """
        if upsert:
            return self.insert_one(doc)
        else:
            return self._update_one(doc)
    
    def update_many(self, doc, upsert  = False):
        """
        updates all documents in current cursor based on the doc. The two are equivalent:
        
        >>> cursot.update_many(doc)
        >>> collection.update_many(cursor._spec, { 'set' : update})

        :Parameters:
        ----------
        doc : dict of values to be updated

        :Returns:
        -------
        itself
        """
        update = self._write(doc)
        update.pop(_id, None)
        self.collection.update_many(self._spec, {_set : update})
        return self
    
    def __setitem__(self, key, value):
        update = dict(zipper(key, value))
        self.set(**update)
    
    def set(self, **kwargs):
        """
        updates all documents in current cursor based on the kwargs. 
        It is similar to update_many but supports also functions

        :Parameters:
        ----------
        kwargs: dict of values to be updated

        :Example:
        ---------
        >>> from pyg import *
        >>> t = mongo_table('test', 'test')
        >>> t = t.drop()
        >>> values = dictable(a = [1,2,3,4,], b = [5,6,7,8])
        >>> t = t.insert_many(values)
        >>> assert t[::]-'_id' == values

        >>> t.set(c = lambda a, b: a+b)
        >>> assert t[::]-'_id' == values(c = [6,8,10,12])
        >>> t.set(d = 1)
        >>> assert t[::]-'_id' == values(c =lambda a, b: a+b)(d = 1)

        :Returns:
        -------
        itself
        """
        static = {key: value for key, value in kwargs.items() if not callable(value)}
        if len(static) == len(kwargs):
            self.update_many(static)
        else:
            for row in self:
                tp = type(row)
                row = Dict(row) if not isinstance(row, Dict) else row
                doc = tp(row(**kwargs))
                self.update_one(doc)
        return self

    def rename(self, **kwargs):
        self.collection.update_many(self._spec, {_rename : kwargs})
        return self
    
    def __delitem__(self, item):
        if isinstance(item, int):
            self.collection.delete_one({_id : self[item][_id]})
        elif is_strs(item):
            self.collection.update_many(self._spec, {_unset: _dict1(item)})
        elif is_dict(item):
            self.find(item).delete_one()
    
    def delete(self, item):
        del self[item]
        return self
    
    def insert_one(self, doc):
        """
        inserts/updates a single document. 
        
        If the document ALREADY has _id in it, it updates that document
        If the document has no _id in it, it inserts it as a new document

        :Parameters:
        ----------
        doc : dict
            document.
            
        :Example:
        ---------
        >>> from pyg import *
        >>> t = mongo_table('test', 'test')
        >>> t = t.drop()
        >>> values = dictable(a = [1,2,3,4,], b = [5,6,7,8])
        >>> t = t.insert_many(values)
        
        :Example: used to update an existing document
        ---------------------------------------------
        >>> doc = t[0]
        >>> doc['c'] = 8
        >>> str(doc)
        >>> "{'_id': ObjectId('602d36150a5cd32717323197'), 'a': 1, 'b': 5, 'c': 8}"

        >>> t = t.insert_one(doc)
        >>> assert len(t) == 4        
        >>> assert t[0] == doc        

        :Example: used to insert 
        ------------------------
        >>> doc = Dict(a = 1, b = 8, c = 10)
        >>> t = t.insert_one(doc)
        >>> assert len(t) == 5
        >>> t.drop()
        """
        if _id in doc:
            return self._update_one(doc)
        else:
            res = doc.copy()
            new = self._write(doc)
            res[_id] = self.collection.insert_one(new).inserted_id
            return res

    def insert_many(self, table):
        """
        inserts multiple documents into the collection

        Parameters
        ----------
        table : sequence of documents
            list of dicts or dictable
        
        Returns
        -------
        mongo_cursor
        
        :Example: simple insertion
        ---------------------------
        >>> from pyg import *
        >>> t = mongo_table('test', 'test')
        >>> t = t.drop()
        >>> values = dictable(a = [1,2,3,4,], b = [5,6,7,8])
        >>> t = t.insert_many(values)
        >>> t[::]        

        >>> dictable[4 x 3]
        >>> _id                     |a|b
        >>> 602daee68c336f6429a77bdd|1|5
        >>> 602daee68c336f6429a77bde|2|6
        >>> 602daee68c336f6429a77bdf|3|7
        >>> 602daee68c336f6429a77be0|4|8

        :Example: update
        ----------------
        >>> table = t[::]
        >>> modified = table(b = lambda b: b**2)
        >>> t = t.insert_many(modified)

        Since each of the documents we uploaded already has an _id...

        >>> assert len(t) == 4
        >>> t[::]
        >>> dictable[4 x 3]
        >>> _id                     |a|b
        >>> 602daee68c336f6429a77bdd|1|25
        >>> 602daee68c336f6429a77bde|2|36
        >>> 602daee68c336f6429a77bdf|3|49
        >>> 602daee68c336f6429a77be0|4|64
    
        """
        with_ids = [self._write(doc) for doc in table if _id in doc]
        no_ids = [self._write(doc) for doc in table if _id not in doc]
        if len(no_ids)>0:
            self.collection.insert_many(no_ids)
        for doc in with_ids:
            spec = {_id: doc.pop(_id)}
            self.collection.update_one(spec, {_set: doc})
        return self

    def __add__(self, item):
        if is_dict(item) and not is_dictable(item):
            self.insert_one(item)
        else:
            self.insert_many(item)
        return self


    def __call__(self, **kwargs):
        callargs = self._callargs(**kwargs)
        obj = mongo_cursor if not callargs.get(_pk) else mongo_pk_cursor
        return obj(**callargs)
        

class mongo_pk_cursor(mongo_cursor):
    """
    The logic for reading with/without pk is similar
    for insertion though, the two are VERY different
    
    """
    def delete_one(self, doc = {}):
        return self.find_one(doc).delete_many()
    
    def delete_many(self):
        self.collection.update_many(self._spec, {_set: {_deleted : datetime.datetime.now()}})
        return self

    drop = delete_many

    @property
    def _pk(self):
        if not self.pk:
            raise ValueError('a mongo_pk_cursor must have some primary keys')
        res = ulist(sorted(set(as_list(self.pk))))
        return res
            

    def insert_one(self, doc):
        """
        marks the old document as deleted and inserts the new one.
        In actual fact, we maintain the old id when we insert the new document, The deleted document receives a new id 

        :Parameters:
        ----------------
        doc : document (dict)
            document to be inserted

        :Returns:
        -------
        _id 
            returned from Mongo

        """
        c = self.find(self._id(doc))
        n = c._assert_one_or_none()
        new = self._write(doc)
        new.pop(_id, None)
        if n == 1:
            old = c.read(0, reader = passthru)
            i = old.pop(_id)
            unset = _dict1(list(old))
            c.collection.update_one({_id : i}, {_unset: unset})
            c.collection.update_one({_id : i}, {_set: new})
            new[_id] = i
            old[_deleted] = datetime.datetime.now()
            self.collection.insert_one(old)
        else:
            missing_keys = [key for key in self._pk if key not in doc]
            if len(missing_keys)>0:
                raise ValueError('cannot save a new doc with primary keys %s missing'%missing_keys)
            new[_id] = self.collection.insert_one(new).inserted_id
        return new[_id]
    
    
    def _update_one(self, new):
        """
        receives a doc, returns updated doc
        """
        old = self.read(0, reader = passthru)
        if old is None:
            new[_id] = self.collection.insert_one(new).inserted_id
        else:
            i = old.pop(_id)
            new = tree_update(old, new)
            unset = _dict1(list(old))
            self.collection.update_one({_id : i}, {_unset: unset})
            self.collection.update_one({_id : i}, {_set: new})            
            new[_id] = i
            old[_deleted] = datetime.datetime.now()
            self.collection.insert_one(old)
        return new
    
    def update_one(self, doc, upsert = True):
        """
        updates an existing document

        :Parameters:
        ----------
        doc : document (dict or dict of dicts)
            DESCRIPTION.
        upsert : bool, optional
            If there isn't an existing document, should it insert doc as a new one?. The default is True.

        :Returns:
        ---------
        document
            new document post insertion/update.
            
        :Example:
        ---------
        >>> db = mongo_table('test', 'test', pk = ['name', 'surname'])
        >>> db.insert_one(dict(name = 'anna', surname = 'auburn', age = 39))        
        >>> db.update_one(dict(name = 'bobby', surname = 'benedict', age = 39))        

        >>> assert get_cell('test', 'test', surname = 'benedict')

        """
        new = self._write(doc)
        c = self.find(self._id(new))
        n = c._assert_one_or_none()
        if n == 1:
            return c._update_one(new)
        elif upsert:
            new = c._write(doc)
            new[_id] = c.collection.insert_one(self._write(new)).inserted_id
            return new ## always returns the encoded cell rather than the original
            
    def update_many(self, update, upsert = True):
        return type(update)([self.update_one(doc, upsert = upsert) for doc in update])

    def __setitem__(self, key, value):
        if isinstance(key, dict):
            doc = key.copy()
            doc.update(value if isinstance(value, dict) else {_data: value})
            self.update_one(doc, upsert = True)
        else:
            super(mongo_pk_cursor, self).__setitem__(key, value)
    


    def __delitem__(self, item):            
        if is_int(item):
            self.delete_one(self[item])
        elif is_strs(item):
            items = [i for i in as_list(item) if not i.startswith('_')]
            cannot_drop = self._pk & items
            if len(cannot_drop) > 0:
                raise ValueError('cannot drop primary keys %s'%cannot_drop)
            deleted = datetime.datetime.now()
            for doc in self:
                del doc[_id]
                doc.update({_deleted: deleted})
                self.collection.insert_one(self._write(doc))
            self.collection.update_many(self._spec, {_unset: _dict1(item)})
        elif isinstance(item, dict):
            self.delete_one(item)

    def delete(self, item):
        del self[item]
        return self

    def insert_many(self, table):
        for doc in table:
            self.insert_one(doc)
        return self
    
    def __add__(self, item):
        if isinstance(item, (list, dictable)):
            self.insert_many(item)
        elif is_dict(item):
            self.update_one(item, upsert = True)
        else:
            raise ValueError('can only add a dict, a dictable or a list of dicts')
        return self

    def set(self, **kwargs):
        rows = [row for row in self]
        for row in rows:
            doc = type(row)(Dict(row)(**kwargs))
            self.update_one(doc)
        return self
    
    @property
    def reset(self):
        return mongo_cursor(self.collection, writer = self.writer, reader = self.reader)

                    
