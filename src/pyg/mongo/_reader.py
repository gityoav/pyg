from pyg.base import zipper, encode, decode, is_list, as_list, is_strs, is_str, is_dict, is_int, dictable, sort, passthru
from pyg.mongo._q import q, _id, _doc
from pyg.mongo._types import is_collection, is_cursor
from pyg.mongo._encoders import csv_write, parquet_write, _csv, _parquet

__all__ = ['mongo_reader', 'clone_cursor']

from functools import partial

def _dict1(keys):
    if keys is None:
        return None
    else:
        return dict(zipper(keys,1))


_clonables = ['batch_size', 'collation', 'comment', 'explain', 'hint', 'limit', 'manipulate', 
             'max', 'max_await_time_ms', 'max_scan', 'max_time_ms', 'min', 'modifiers', 
             'ordering', 'projection', 'query_flags', 'skip', 'spec']


def is_mongo_reader(value):
    return isinstance(value, mongo_reader)

def _as_reader(reader):
    if reader is None or reader is True or reader == ():
        return decode
    elif reader is False or reader == 0:
        return passthru
    else:
        return reader

def _as_writer(writer):
    if writer is None or writer is True or writer == ():
        return encode
    elif writer is False or writer == 0:
        return passthru
    elif is_str(writer):
        if writer.endswith(_csv[1:]):
            root = writer[:-len(_csv)]
            if root:
                return [partial(csv_write, root = root), encode]
            else:
                return [csv_write, encode]
        elif writer.endswith(_parquet[1:]):
            root = writer[:-len(_parquet)]
            if root:
                return [partial(parquet_write, root = root), encode]
            else:
                return [parquet_write, encode]
        else:
            raise ValueError('We support only parquet/csv writers and writer should look like: c:/somewhere/%name/%surname.csv or d:/archive/%country/%city/results.parquet')
    else:
        return writer

def clone_cursor(cursor, clonables = None, **kwargs):
    if clonables is None:
        clonables = _clonables
    res = cursor.clone()
    update = {'_Cursor__' + key: value for key, value in kwargs.items() if key in clonables}
    res.__dict__.update(update)
    return res

class mongo_reader(object):
    """
    mongo_reader is a read-only version of the mongo_cursor. 
    You can instantiate it with a mongo_reader(cursor) call where cursor can be a mongo_cursor, a pymongo.Cursor or a pymongo.Collection    
    """
    def __init__(self, cursor, writer = None, reader = None, query = None, **_):
        if isinstance(cursor, mongo_reader):
            self._reader = cursor.reader if reader is None else reader
            self._writer = cursor.writer if writer is None else writer
            self._cursor = cursor._cursor
        elif is_collection(cursor):
            self._cursor = cursor.find(q(query))
        elif is_cursor(cursor):
            self._cursor = cursor
        self._reader = _as_reader(reader)
        self._writer = _as_writer(writer)
    
    def __eq__(self, other):
        return type(other) == type(self) and self.reader == other.reader and self.writer == other.writer \
            and self.collection == other.collection and self.spec == other.spec and self.projection == other.projection

    def insert_one(self, *_, **__):
        raise AttributeError('mongo_reader is read-only')
    def insert_many(self, *_, **__):
        raise AttributeError('mongo_reader is read-only')
    def delete_one(self, *_, **__):
        raise AttributeError('mongo_reader is read-only')
    def delete_many(self, *_, **__):
        raise AttributeError('mongo_reader is read-only')
    def set(self, *_, **__):
        raise AttributeError('mongo_reader is read-only')
    def drop(self, *_, **__):
        raise AttributeError('mongo_reader is read-only')
        
        
    @property
    def reader(self):
        return self._reader
    
    @property
    def writer(self):
        return self._writer
    
    def _read(self, doc, reader = None):
        """
        converts doc from Mongo into something we want
        """
        res = doc
        if reader is None:
            reader = self._reader
        for r in as_list(reader):
            res = res[r] if is_strs(r) else r(res)
        return res

    def _write(self, doc, writer = None):
        res = doc.copy()
        if writer is None:
            writer = self._writer
        for w in as_list(writer):
            w = _as_writer(w)                
            res = w(res)
        return res

    def read(self, item = 0, reader = None):
        """
        reads the next document from the collection.

        :Parameters:
        ----------
        item : int, optional
            Please read the ith record. The default is 0.
        reader : callable/list of callables, optional
            When we read the document from the collection, we first transform them. 
            The default behaviour is to use pyg.base._encode.decode but you may pass reader = False to grab the raw data from mongo

        :Returns:
        -------
        document
            The document from Mongo

        """
        if reader is None: 
            reader = self._reader
        else:
            reader = _as_reader(reader)
        if is_dict(item):
            doc = self.find_one(item).cursor[0]
            return self._read(doc, reader = reader)
        elif is_int(item):
            doc = self.cursor[0]
            return self._read(doc, reader = reader)    
        elif isinstance(item, slice):
            return dictable([self._read(i, reader = reader) for i in self.cursor[self._item(item)]])
        elif is_list(item):
            return [self.read(i, reader) for i in item]

    @property
    def collection(self):
        """

        :Returns:
        -------
        pymongo.Collection object

        """
        return self._cursor.collection
    
    @property
    def address(self):
        """
        :Returns:
        ---------
        tuple
            A unique combination of the client addres, database name and collection name, identifying the collection uniquely.

        """
        collection = self._cursor.collection
        return ('url', '%s:%s' % collection.database.client.address), ('db', collection.database.name), ('table', collection.name)

    
    def clone(self, **params):
        """
        :Returns:
        ---------
        mongo_reader
            Returns a cloned version of current mongo_reader but allows additional parameters to be set (see spec and project)
        """
        reader = params.pop('reader', self._reader)
        writer = params.pop('writer', self._writer)
        result = clone_cursor(self._cursor, **params)
        return type(self)(result, reader = reader, writer = writer)
    
    @property
    def spec(self): 
        """
        :Returns:
        ---------
            The 'spec' is the cursor's filter on documents (can think of it as row-selection) within the collection
        """
        return self._cursor._Cursor__spec

    @property
    def projection(self):
        """
        :Returns:
        ---------
            The 'projection' is the cursor's column selection on documents. If in SQL we write SELECT col1, col2 FROM ..., in Mongo, the cursor.projection = ['col1', 'col2']
        """
        return self._cursor._Cursor__projection
    
    def _id(self, doc):
        if _id in doc:
            return q[_id] == doc[_id]
        else:
            return doc
    
    def find_one(self, doc = None, *args, **kwargs):
        """
        searches for records based either on the doc, or the args/kwargs specified.
        Unlike mongo cursor which finds one of many, here, when we ask for find_one, we will throw an exception if more than one documents are found.
        
        :Returns:
        ---------
            A cursor pointing to a single record (document)
        """
        c = self
        if doc:
            c = c.find(self._id(doc))
        if len(args) + len(kwargs) > 0:
            c = c.find(*args, **kwargs)
        c._assert_unique()
        return c
    
    def project(self, projection = None): 
        """
        The 'projection' is the cursor's column selection on documents. If in SQL we write SELECT col1, col2 FROM ..., in Mongo, the cursor.projection = ['col1', 'col2']

        :Parameters:
        ------------
        projection: a list/str of keys we are interested in reading. Note that nested keys are OK: 'level1.level2.name' is perfectly good

        :Returns:
        ---------
        A mongo_reader cursor filtered to read just these keys
        """
        return self if projection is None else self.clone(projection = _dict1(projection))
    
    def specify(self, *args, **kwargs):
        """
        The 'spec' is the cursor's filter on documents (can think of it as row-selection) within the collection.
        We use q (see pyg.mongo._q.q) to specify the filter on the cursor.
        
        :Returns:
        ---------
        A filtered mongo_reader cursor
        """
        if len(args) or len(kwargs):
            return self.clone(spec = q(*args, **kwargs))
        else:
            return self

    def find(self, *args, **kwargs):
        """
        Same as self.specify()

        The 'spec' is the cursor's filter on documents (can think of it as row-selection) within the collection.
        We use q (see pyg.mongo._q.q) to specify the filter on the cursor.
        
        :Returns:
        ---------
        A filtered mongo_reader cursor
        
        :Example:
        ---------
        >>> from pyg import *; import pymongo
        >>> table = pymongo.MongoClient()['test']['test']
        >>> table.insert_one(dict(name = 'alan', surname = 'abrahams', age = 39, marriage = dt(2000)))
        >>> table.insert_one(dict(name = 'barbara', surname = 'brown', age = 50, marriage = dt(2020)))
        >>> table.insert_one(dict(name = 'charlie', surname = 'cohen', age = 20))

        >>> t = mongo_reader(table)
        >>> assert len(t.find(name = 'alan')) == 1
        >>> assert len(t.find(q.age>25)) == 2
        >>> assert len(t.find(q.age>25, q.marriage<dt(2010))) == 1
        
        >>> table.drop()

        """
        if len(args) or len(kwargs):
            return self.clone(spec = q(self.spec, *args, **kwargs))
        else:
            return self
        
    @property
    def cursor(self):
        if self._cursor._Cursor__retrieved or self._cursor._Cursor__id is not None:
            self._cursor.rewind()
        return self._cursor

    def count(self):
        """
        cursor.count() and len(cursor) are the same and return the number of documents matching current specification.
        """
        return self.collection.count_documents(self.spec)
    
    __len__ = count
            
    def _assert_one_or_none(self):
        n = len(self)
        if n>1:
            raise ValueError('%s\nNon-unique %i documents %s... e.g. \n%s'%(self.collection, n, self.spec, self.read(slice(None,3,None))))
        else:
            return n

    def _assert_unique(self):
        n = self._assert_one_or_none()
        if n == 0:
            raise ValueError('%s\nNo documents %s'%(self.collection,self.spec))
        return n
        
    def _item(self, item):
        if isinstance(item, slice):
            return slice(self._item(item.start), self._item(item.stop), item.step)
        if  is_int(item) and item<0:
            return len(self) + item
        else:
            return item
        
    def __iter__(self):
        """
        When we iterate over documents we often change them, causing the cursor to change as well.
        This means often unexpected behaviour and in particular can lead to infinite loops too.
        We therefore choose to fix the _ids in advance.
        """
        _ids = self.collection.distinct(_id, self.spec)
        for i in _ids:
            yield self.specify({_id : i}).read(0)
        # for i in range(len(self)):
        #     yield self._read(self.cursor[i])
        
    inc = find
    
    def exc(self, **kwargs):
        """
        filters 'negatively' removing documents that match the criteria specified. 

        Returns
        -------
        cursor
            filtered documents.
        
        :Example:
        ---------
        >>> from pyg import *; import pymongo
        >>> table = pymongo.MongoClient()['test']['test']
        >>> table.insert_one(dict(name = 'alan', surname = 'abrahams', age = 39, marriage = dt(2000)))
        >>> table.insert_one(dict(name = 'barbara', surname = 'brown', age = 50, marriage = dt(2020)))
        >>> table.insert_one(dict(name = 'charlie', surname = 'cohen', age = 20))

        >>> t = mongo_reader(table)
        >>> assert len(t.exc(name = 'alan')) == 2        
        >>> assert len(t.exc(name = ['alan', 'barbara'])) == 1        
        >>> table.drop()
        """
        query = [getattr(q, key)!=value for key, value in kwargs.items()]
        return self.inc(*query)
    
    
    def __getitem__(self, item):
        if is_str(item) or isinstance(item, (list, tuple)):
            return self.project(as_list(item))
        else:
            return self.read(item)
    
    def __getattr__(self, key):
        if key.startswith('_'):
            return super(mongo_reader, self).__getattr__(key)
        else:            
            return self.distinct(key)
    
    def keys(self, item = 0):
        return self.read(item, reader = passthru).keys()
    
    def distinct(self, key):
        """
        returns the distinct values of the key

        Parameters
        ----------
        key : str
            a key in the documents.

        Returns
        -------
        list of strings
            distinct values

        :Example:
        ---------
        >>> from pyg import *; import pymongo
        >>> table = pymongo.MongoClient()['test']['test']
        >>> table.insert_one(dict(name = 'alan', surname = 'abrahams', age = 39, marriage = dt(2000)))
        >>> table.insert_one(dict(name = 'barbara', surname = 'brown', age = 50, marriage = dt(2020)))
        >>> table.insert_one(dict(name = 'charlie', surname = 'cohen', age = 20))

        >>> t = mongo_reader(table)
        >>> assert t.name == t.distinct('name') == ['alan', 'barbara', 'charlie']
        >>> table.drop()
        
        """
        res = self.cursor.distinct(key)
        try:
            return sort(res)
        except TypeError:
            return res
    
    def sort(self, *by):
        """
        sorting on server side, per key(s)

        Parameters
        ----------
        by : str/list of strs

        Returns
        -------
        sorted cursor.

        """
        by = as_list(by)
        self.cursor.sort(list(_dict1(by).items()))
        return self


    def docs(self, doc = _doc, *keys):
        """
        self[::] flattens the entire document.
        At times, we want to see the full documents, indexed by keys and docs does that.        
        returns a dictable with both keys and the document in the 'doc' column
        """
        keys = as_list(keys)
        docs = list(self)
        res = dictable([{key: d.get(key) for key in keys} for d in docs])
        res[doc] = docs
        return res

    def create_index(self, *keys):
        keys = as_list(keys)
        if len(keys):
            return self.collection.create_index([(key, 1) for key in keys])
        else:
            raise ValueError('please specify keys to create an index')

    def __repr__(self):
        n = len(self)
        if n>0:
            return '%(t)s for %(c)s \n%(s)s %(p)s\ndocuments count: %(n)i \n%(k)s'%dict(t = type(self), 
                                                                                          c = self.collection, 
                                                                                          s = self.spec, 
                                                                                          p = self.projection, 
                                                                                          n = n, 
                                                                                          k = self.keys())
        else:
            return '%(t)s for %(c)s \n%(s)s %(p)s\ndocuments count: 0'%dict(t = type(self), 
                                                                            c = self.collection, 
                                                                            s = self.spec, 
                                                                            p = self.projection)
    
    @property
    def raw(self):
        """
        returns an unfiltered mongo_reader
        """
        return mongo_reader(self.collection, writer = self._writer, reader = self._reader)
