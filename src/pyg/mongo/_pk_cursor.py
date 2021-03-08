from pyg.base import tree_update, dt, is_int, is_dict,  is_strs, ulist, as_list, dictable, Dict
from pyg.mongo._q import _set, _unset, _id, _data, _deleted
from pyg.mongo._pk_reader import mongo_pk_reader, passthru
from pyg.mongo._reader import _dict1
from pyg.mongo._cursor import mongo_cursor
from copy import copy


class mongo_pk_cursor(mongo_pk_reader, mongo_cursor):

    def delete_one(self, doc = {}):
        return self.find_one(doc).delete_many()

    def insert_one(self, doc):
        """
        we maintain the old id when we insert the new document, 

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
            old = c.read(0, reader = False)
            i = old.pop(_id)
            unset = _dict1(list(old))
            c.collection.update_one({_id : i}, {_unset: unset})
            c.collection.update_one({_id : i}, {_set: new})
            new[_id] = i
            old[_deleted] = dt()
            self.collection.insert_one(old)
        else:
            missing_keys = [key for key in self.pk if key not in doc]
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
            old[_deleted] = dt()
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
            new[_id] = c.collection.insert_one(new).inserted_id
            return new ## always returns the encoded cell rather than the original
            
    def update_many(self, update, upsert = True):
        return type(update)([self.update_one(doc, upsert = upsert) for doc in update])

    def __setitem__(self, key, value):
        if isinstance(key, dict):
            doc = copy(key)
            doc.update(value if isinstance(value, dict) else {_data: value})
            self.update_one(doc, upsert = True)
        else:
            super(mongo_pk_cursor, self).__setitem__(key, value)
    
    def __delitem__(self, item):            
        if is_int(item):
            self.delete_one(self[item])
        elif is_strs(item):
            items = [i for i in as_list(item) if not i.startswith('_')]
            cannot_drop = ulist(self.pk) & items
            if len(cannot_drop) > 0:
                raise ValueError('cannot drop primary keys %s'%cannot_drop)
            deleted = dt()
            for doc in self:
                self.collection.insert_one((doc - _id) + {_deleted: deleted})
            self.collection.update_many(self.spec, {_unset: _dict1(item)})
        elif isinstance(item, dict):
            self.delete_one(item)
    
    def delete_many(self):
        self.collection.update_many(self.spec, {_set: {_deleted : dt()}})

    drop = delete_many

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
    def raw(self):
        return mongo_cursor(self.collection, writer = self._writer, reader = self._reader)

                
            
        