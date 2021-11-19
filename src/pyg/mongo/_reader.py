from pyg.base import as_list, is_strs, is_str, is_dict, is_int, dictable, sort, passthru
from pyg.mongo._q import _id, _doc, q, _set, _deleted
from pyg.mongo._base_reader import mongo_base_reader, _items1
import datetime

__all__ = ['mongo_reader']


class mongo_reader(mongo_base_reader):

    def count(self):
        return self.collection.count_documents(self._spec)

    __len__ = count    

    def _assert_one_or_none(self):
        n = self.count()
        if n>1:
            pk = self._pk
            if pk:
                for key in pk:
                    multiple_values = self.distinct(key)
                    if len(multiple_values) > 1:
                        raise KeyError('too many %s = %s found'%(key, multiple_values))
                self = self.dedup()
                n = self.count()
                if n <= 1:
                    return n
            raise ValueError('%s\nNon-unique %i documents %s... e.g. \n%s'%(self.collection, n, self._spec, self.read(slice(None,3,None))))
        else:
            return n

    def _assert_unique(self):
        n = self._assert_one_or_none()
        if n == 0:
            raise ValueError('%s\nNo documents %s'%(self.collection, self._spec))
        return self

    def find_one(self, doc = None, *args, **kwargs):
        res = self.find(*args, **kwargs)
        if doc:
            res = res.find(self._id(doc))
        return res._assert_unique()

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
            
        item = 0

        """
        cursor = self.cursor
        if is_int(item):
            if item < 0:
                item = self.count() + item
            if item > 0:
                cursor = cursor.skip(item)
            doc = cursor.next()
            return self._read(doc, reader = reader)
        elif is_dict(item):
            res = self.find_one(item)
            return self._read(res.cursor.next(), reader = reader)
        elif isinstance(item, slice):
            return dictable([self._read(i, reader = reader) for i in self.cursor[self._item(item)]])
        elif isinstance(item, (list, range, tuple)):
            return [self.read(i, reader = reader) for i in item]

    
    def __getitem__(self, item):
        if is_str(item):
            return self.distinct(item)
        elif is_strs(as_list(item)):
            return self(projection = as_list(item))
        else:
            return self.read(item)
            
    
    def distinct(self, key):
        """
        returns the distinct cursor values of the key        
        """
        res = self.cursor.distinct(key)
        try:
            return sort(res)
        except TypeError:
            return res

    def docs(self, doc = _doc, *keys):
        """
        self[::] flattens the entire document.
        At times, we want to see the full documents, indexed by keys and docs does that.        
        returns a dictable with both keys and the document in the 'doc' column
        """
        keys = self._pk + as_list(keys)
        docs = [self._read(doc) for doc in self.cursor]
        res = dictable([{key: d.get(key) for key in keys} for d in docs])
        res[doc] = docs
        return res


    def create_index(self, *keys):
        keys = as_list(keys) or self._pk
        if len(keys) > 0:
            return self.collection.create_index(_items1(keys))
        return self

    def keys(self, item = 0):
        return self.read(item).keys()

    def __repr__(self):
        n = len(self)
        if n>0:
            return '%(t)s for %(c)s \n%(s)s %(p)s\ndocuments count: %(n)i \nfirst doc %(k)s'%dict(t = type(self), 
                                                                                          c = self.collection, 
                                                                                          s = self._spec, 
                                                                                          p = self._projection, 
                                                                                          n = n, 
                                                                                          k = self.keys())
        else:
            return '%(t)s for %(c)s \n%(s)s %(p)s\ndocuments count: 0'%dict(t = type(self), 
                                                                            c = self.collection, 
                                                                            s = self._spec, 
                                                                            p = self._projection)
            
                
        
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
        _ids = self.collection.distinct(_id, self._spec)
        for i in _ids:
            yield self.inc({_id : i}).read(0)
        
            
    def __getattr__(self, key):
        if key.startswith('_'):
            return super(mongo_reader, self).__getattr__(key)
        else:            
            return self.distinct(key)
    

    def dedup(self):
        """
        Although in principle, if a single process reads/writes to Mongo, we should not get duplicates. 
        In practice, when multiple clients access the database, we occasionally get multiple records with the same primary keys.
        When this happens, we also end up with poor mongo _ids 

        Returns
        -------
        mongo_pk_cursor
            Hopefully, a table with unique keys.

        """
        if self.pk:
            pk = self._pk
            bad = self(projection = pk)[::].sort(_id).listby(pk).inc(lambda _id: len(_id)>1) 
            if len(bad):
                self.collection.update_many(q._id == sum(bad[lambda _id: _id[:-1]], []), {_set: {_deleted : datetime.datetime.now()}})
        return self
        
