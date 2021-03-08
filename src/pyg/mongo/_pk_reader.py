from pyg.base import ulist, as_list, logger, decode, dt
from pyg.mongo._q import q, _deleted, _doc, _set, _id
from pyg.mongo._reader import mongo_reader, clone_cursor, passthru, _id
import re

_pk = '_pk'
_id = '_id'

def _pkq(pk):
    """
    :Parameters:
    ----------------
    pk : list 
        list of primary keys

    :Returns:
    -------
    dict 
        mongo query filtering for table

    :Example:
    --------------
    >>> pk = ['world', 'hello']
    >>> query = q.a == 1
    >>> q(query, _pkq(pk))
    """
    
    pk = sorted(set(as_list(pk)))
    return q(q[_deleted].not_exists, q[_pk] == [pk])

class mongo_pk_reader(mongo_reader):
    """
    we set up a system in Mongo to ensure we can mimin tables with primary keys. 
    The way we do this is two folds:
        - At document insertion, we mark as _deleted old documents sharing the same keys by adding a key _deleted to the old doc
        - At reading, we filter for documents where q._deleted.not_exists.

    """    
    def __init__(self, cursor, pk, writer = None, reader = None, query = None, **_):
        pk = ulist(sorted(set(as_list(pk))))
        if len(pk) == 0 and hasattr(cursor, 'pk'):
            pk = cursor.pk            
        super(mongo_pk_reader, self).__init__(cursor, writer = writer, reader = reader, query = q(query, _pkq(pk)))
        self.pk = pk
    
    def create_index(self, *keys):
        """
        creates a sorted index on the collection

        :Parameters:
        ----------
        *keys : strings
            if misssing, use the primary keys.

        """
        if len(keys) == 0:
            keys = self.pk
        return super(mongo_pk_reader, self).create_index(*keys)

            
    
    def clone(self, **kwargs):
        result = clone_cursor(self._cursor, **kwargs)
        return type(self)(result, pk = self.pk, reader = self._reader, writer = self._writer)

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
        # bad_ids = self.raw.inc(_id = re.compile('{'))   ## This happens but should be handled elsewhere
        # if len(bad_ids):
        #     logger.warning('WARN: removing %i documents with badly coded id'%len(bad_ids))
        #     bad_ids.drop()
        bad = self[self.pk][::].sort(_id).listby(self.pk).inc(lambda _id: len(_id)>1) 
        if len(bad):
            self.collection.update_many(q._id == sum(bad[lambda _id: _id[:-1]], []), {_set: {_deleted : dt()}})
        return self
        
    def _assert_one_or_none(self):
        n = len(self)
        if n>1:
            for key in self.pk:
                multiple_values = self.distinct(key)
                if len(multiple_values) > 1:
                    raise KeyError('too many %s = %s found'%(key, multiple_values))
            self = self.dedup()
            n = len(self)
            if n > 1:
                raise ValueError('%s\n%inon unique documents %s... e.g \n%s'%(self.collection, n, self.spec, self.read(slice(None, 3, None), reader = passthru)))
            else:
                return n
        else:
            return n

    def _id(self, doc):
        if _id in doc:
            return q[_id] == decode(doc[_id])
        else:
            return q(q[_deleted].not_exists, q[_pk] == [self.pk], **{key : doc[key] for key in self.pk if key in doc})

    def _write(self, doc, writer = None):
        res = super(mongo_pk_reader, self)._write(doc, writer)
        missing = sorted(set(self.pk) - set(doc.keys()))
        if len(missing):
            raise ValueError('trying to write a document with missing primary keys %s'%missing)
        res.update({_pk: self.pk})
        return res
    
    def docs(self, doc = _doc, *keys):
        return super(mongo_pk_reader, self).docs(doc, ulist(self.pk) + as_list(keys))
        
