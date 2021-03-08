
from pyg.base import logger
from pyg.base import zipper, is_strs, is_dict, Dict, is_dictable
from pyg.mongo._q import _set, _id, _unset
from pyg.mongo._reader import mongo_reader, _dict1



class mongo_cursor(mongo_reader):
    """
    mongo_cursor is a souped-up combination of mongo.Cursor and mongo.Collection with a simple API.

    :Parameters:
    ----------------
    cursor : MongoDB cursor or MongoDB collection
        
    writer : True/False/string, optional
        The default is None.
        
        writer determines how data is written onto Mongo. MongoDB is great for manipulating/searching dict keys/values. 
        If set to None, dataframes will be converted seamlessly to bytes and stored in MongoDB. Most often, this is fine.
        
        At times, the actual dataframes in each doc, we may want to save in a file system. This may be because:
        - The DataFrames are stored as bytes in MongoDB anyway, so they are not searchable
        - Storing in files allows other non-python/non-MongoDB users easier access, allowing data to be detached from app
        - MongoDB free version has limitations on size of document
        - file based system may be faster
        - for data licensing issues, data must not sit on servers but stored on local computer

        Therefore, if you set writer to .csv or .parquet, dataframes within will be saved to files first and we store in mongo references to these files.
        
        For this to work, you need to tell us WHERE to store each document and this is how it works: If your document are primary-keyed by name, surname. Then...
        you can set the root centrally using expression like writer = 'c:/%name%surname.parquet'

        Alternatively, writer = '.parquet' will encode only documents for which a 'root' key exists. This defers the decision of how to store itself to the cell.
                    
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
    
    :roww access:
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
    
    Since mongo_cursor is too powerful, we also have a mongo_reader version which is read-only.
    
    
    """
    def delete_many(self):
        """
        Equivalent to drop: deletes all documents the cursor currently points to.
        
        :Note: 
        ------
        If you want to drop a subset of the data, then use c.find(criteria).delete_many()

        :Returns:
        -------
        itself
        """
        logger.info('INFO: deleting %i documents based on %s'%(len(self), self.spec))
        self.collection.delete_many(self.spec)
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
        c.collection.delete_one(c.spec)
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
        self.collection.update_one(c.spec, {_set: update})
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
        >>> collection.update_many(cursor.spec, { 'set' : update})

        :Parameters:
        ----------
        doc : dict of values to be updated

        :Returns:
        -------
        itself
        """
        update = self._write(doc)
        update.pop(_id, None)
        self.collection.update_many(self.spec, {_set : update})
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
    
    def __delitem__(self, item):
        if isinstance(item, int):
            self.collection.delete_one({_id : self[item][_id]})
        elif is_strs(item):
            self.collection.update_many(self.spec, {_unset: _dict1(item)})
        elif is_dict(item):
            self.find(item).delete_one()
            
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

    @property
    def raw(self):
        return mongo_cursor(self.collection, writer = self._writer, reader = self._reader)
            
        
    
        