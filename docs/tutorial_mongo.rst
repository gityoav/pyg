pyg.mongo
*********
MongoDB has replaced our SQL databases as it is just too much fun to use. MongoDB does have its little quirks:

* The MongoDB 'query document' that replaces the SQL WHERE statements is very powerful but you need a PhD for even the simplest of queries.
* too many objects we use (specifically, numpy and pandas objects) cannot be pushed directly easily into Mongo.
* Mongo lacks the concept of a table with primary keys. Unstructured data is great but much of how we think of data is structured. 

pyg.mongo addresses all three issues:

* **q** is a much easier way to generate Mongo queries. We are happy to acknowledge TinyDB <https://tinydb.readthedocs.io/en/latest/usage.html#queries> for the idea.
* **mongo_cursor** is a super-charged cursor and in particular, it handles encoding and decoding of objects seemlessly in a way that allows us to store all that we want in Mongo.
* **mongo_pk_cursor** manages a table with primary keys and full history audit

q
==
The MongoDB interface for query of a collection (table) is via a creation of a query document <https://docs.mongodb.com/manual/tutorial/query-documents/>. 
This is rather complicated for the average use. For example, if you wanted to locate James Bond in the collection, you would use:

>>>  {"$and": [{"name": {"$eq": "James"}}, {"surname": {"$eq": "Bond"}}]}

It's doable, but not much fun. Luckily... you can write this instead:

>>> q(name = 'James', surname = 'Bond')

What about all the James who are not Bond?

>>> {"$and": [{"name": {"$eq": "James"}}, {"surname": {"$ne": "Bond"}}]}

Again, q comes to the rescue:

>>> (q.surname!='Bond') & (q.name == 'James')

+-------------------+----------------------------------------------------+---------------------------+
| query             | using query dict                                   | using q                   |
+===================+====================================================+===========================+
| a in [1,2,3]      | {"a": {"$in": [1, 2, 3]}}                          | q(a = [1,2,3])            |
+-------------------+----------------------------------------------------+---------------------------+
| a = 1 and b = 2   | {"$and": [{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]}   | q(a = 1, b = 2)           |
+-------------------+----------------------------------------------------+---------------------------+
| a = 1 or  b = 2   | {"$or": [{"a": {"$eq": 1}}, {"b": {"$eq": 2}}]}    | (q.a == 1) | (q.b == 2)   |
+-------------------+----------------------------------------------------+---------------------------+
| a > 1             | {"a": {"$gt": 1}}                                  | q.a > 1                   |
+-------------------+----------------------------------------------------+---------------------------+
| a within [5,10]   | {"$and": [{"a": {"$gte": 5}}, {"a": {"$lt": 10}}]} | q.a[5:10]                 |
+-------------------+----------------------------------------------------+---------------------------+
| a !=1             | {"a": {"$ne": 1}}                                  | q.a != 1                  |
+-------------------+----------------------------------------------------+---------------------------+
| key 'a' exists    | {"a": {"$exists": true}}                           | q.a.exists (or just +q.a) |
+-------------------+----------------------------------------------------+---------------------------+
| 'a' not exists    | {"a": {"$exists": false}}                          | q.a.not_exists  (or -q.a) |
+-------------------+----------------------------------------------------+---------------------------+
| not expression    | {"$not": {"a": {"$eq": 1}}}                        | ~q(a = 1)                 |
+-------------------+----------------------------------------------------+---------------------------+
| regex             | {"a": {"regex": "^txt"}}                           | q.a == re.compile('^txt') |
+-------------------+----------------------------------------------------+---------------------------+

As you can see, q is callable and you can put expressions inside it, or you can use the q.key method.
We end with a fun James Bond query. If we want to find the documents with actors who played James Bond:

>>> rs = dictable(name = ['Daniel', 'Sean', 'Roger', 'Timothy'], surname = ['Craig', 'Connery', 'Moore', 'Dalton'])
>>> dictable[4 x 2]
>>> name   |surname
>>> Daniel |Craig  
>>> Sean   |Connery
>>> Roger  |Moore  
>>> Timothy|Dalton 

And turning this into a query, q of a list of dicts are translated into the or expression so...

>>> q(list(rs))
>>> $or:
>>>     {"$and": [{"name": {"$eq": "Daniel"}}, {"surname": {"$eq": "Craig"}}]}
>>>     {"$and": [{"name": {"$eq": "Roger"}}, {"surname": {"$eq": "Moore"}}]}
>>>     {"$and": [{"name": {"$eq": "Sean"}}, {"surname": {"$eq": "Connery"}}]}
>>>     {"$and": [{"name": {"$eq": "Timothy"}}, {"surname": {"$eq": "Dalton"}}]}

mongo_cursor
============
The mongo cursor:

* enables saving seemlessly objects and data in MongoDB
* simplifies filtering
* simplifies projecting onto certain keys in document


general objects insertion into documents
----------------------------------------
pymongo.Collection supports insertion of documents into it:

>>> from pyg import *; import pymongo as pym; import pytest
>>> c = pym.MongoClient()['test']['test']
>>> c.drop()                                    # drop all documents
>>> c.insert_one(dict(a = 1, b = 2))            # insert a document
>>>     <pymongo.results.InsertOneResult at 0x261d1b20a80>
>>> assert c.count_documents({}) == 1           # in order to count documents, must apply the empty filter {}

We can do similar stuff with a mongo_cursor

>>> t = mongo_table(table = 'test', db = 'test')
>>> t.drop()
>>> t.insert_one(dict(a = 1, b = 2))
>>>     <class 'pyg.mongo._cursor.mongo_cursor'> for Collection(Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'test'), 'test') # collection
>>>     M{} None                      # current filter
>>>     documents count: 1            # length
>>>     dict_keys(['_id', 'a', 'b'])  # keys of the first document
>>> assert len(t) == 1                # no need to specify the filter, mongo_cursor keeps track of the current filter

Annoyingly, raw pymongo.Collection cannot encode for lots of existing objects.

>>> ts = pd.Series([1.,2.], drange(-1))
>>> a = np.arange(3)
>>> f = np.float32(32.0)
>>> with pytest.raises(Exception):
>>>     c.insert_one(dict(a = a))
>>> with pytest.raises(Exception):
>>>     c.insert_one(dict(f = f))
>>> with pytest.raises(Exception):
>>>     c.insert_one(dict(ts = ts))  

Further, unless we define the encoding, new classes do not work either

>>> class NewClass():
>>>     def __init__(self, n):
>>>         self.n = n
>>>     def __eq__(self, other):
>>>         return type(other) == type(self) and self.n == other.n
>>> n = NewClass(1)
>>> with pytest.raises(Exception):
>>>     c.insert_one(dict(n = n))  

The mongo_cursor t can insert all these happily:

>>> t.drop()
>>> t.insert_one(dict(a = a, f = f, ts = ts, n = n))
>>> assert len(t) == 1

document reading
----------------
What is nice is that you get back the **object** you saved, not just the data.

>>> doc = t[0]
>>> assert eq(doc['ts'], ts)
>>> assert eq(doc['a'], a)
>>> assert doc['n'] == n

Is this magic? Not really...Let us explain. We first read the doc using the Collection:

>>> raw_doc = c.find_one({})
>>> assert raw_doc['n'] == '{"py/object": "__main__.NewClass", "n": 1}'
>>> assert encode(n) == '{"py/object": "__main__.NewClass", "n": 1}'
>>> assert decode('{"py/object": "__main__.NewClass", "n": 1}') == n

The mongo_cursor encodes the object pre-saving. It then we decode it post loading. This is done transparently but the user does have full control of write/read decode protocol:

>>> assert t.writer == encode
>>> assert t.reader == decode
>>> # for example, if we want the timeseries to be saved to parquet files:
>>> t2 = mongo_table('test', 'test', writer = '.parquet')
>>> assert t2.writer == [parquet_write, encode]

Similarly, we encode the array into data, and tell decoder to use bson2np to decode it:

>>> raw_doc['a']
>>> {'data': b'\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00',
>>> 'shape': [3],
>>>  'dtype': '{"py/reduce": [{"py/type": "numpy.dtype"}, {"py/tuple": ["i4", false, true]}, {"py/tuple": [3, "<", null, null, null, -1, -1, 0]}]}',
>>> '_obj': '{"py/function": "pyg.base._encode.bson2np"}'}
>>> assert eq(decode(raw_doc['a']), a)

:Note: This works with the assumption that the person loading and person saving share the library. 
If, when loading the data from MongoDB, the user does not have 'pyg.base._encode.bson2np' installed, then he will receive the undecoded object. 
On the plus side, the user will receive a message saying pyg.base._encode.bson2np is missing and needs to be installed.

document writing to files
-------------------------
MongoDB is great for manipulating/searching dict keys/values. 
The actual dataframes in each doc, we may want to save in a file system because:

* DataFrames are stored as bytes in MongoDB anyway, so they are not searchable
* Storing in files allows other non-python/non-MongoDB users easier access, allowing data to be detached from app
* MongoDB free version has limitations on size of document
* For data licensing issues, data must not sit on servers but needs to be stored on local computer

>>> from pyg import *; import pymongo as pym
>>> doc = dict(stock = 'APPL', price = pd.Series(np.random.normal(0,1, 100), drange(-99)), 
>>>                other_stuff = dict(other = pd.Series(np.random.normal(0,1, 100), drange(-99)), 
>>>                stuff = pd.Series(np.random.normal(0,1, 100), drange(-99))))
>>> t = mongo_table('test', 'test', writer = 'c:/temp/%stock.parquet')
>>> c = pym.MongoClient()['test']['test']
>>> t.drop()
>>> t.insert_one(doc)

What is stored in MongoDB is the path to the file, which treat 'c:/temp/%stock' as the root. %stock is replaced with AAPL from the document and we get:

>>> c.find_one({})
>>> {'_id': ObjectId('6025207925f7cceaef545905'),
>>>  'stock': 'APPL',
>>>  'price': {'_obj': '{"py/function": "pyg.base._parquet.pd_read_parquet"}', 'path': 'c:/temp/APPL/price.parquet'},
>>>  'other_stuff': {'other': {'_obj': '{"py/function": "pyg.base._parquet.pd_read_parquet"}', 'path': 'c:/temp/APPL/other_stuff/other.parquet'},
>>>                  'stuff': {'_obj': '{"py/function": "pyg.base._parquet.pd_read_parquet"}','path': 'c:/temp/APPL/other_stuff/stuff.parquet'}
>>>                 }
>>> }

So without access to MongoDB, we can still read the data:

>>> other = pd_read_parquet('c:/temp/APPL/other_stuff/other.parquet')
>>> assert eq(other, doc['other_stuff']['other'])

Or just read the data using mongo_cursor seemlessly:

>>> loaded_doc = t[0]
>>> assert eq(loaded_doc['other_stuff']['other'], doc['other_stuff']['other'])

Finally, one can override 'c:/temp/%stock' by adding a 'root' to the document we are saving:

>>> t.drop()
>>> doc['root'] = 'c:/temp/my_own_place'
>>> t.insert_one(doc)
>>> assert eq(pd_read_parquet('c:/temp/my_own_place/other_stuff/other.parquet'), doc['other_stuff']['other'])

document access
---------------
We start by pushing a 10x10 times table into t

>>> t.drop()
>>> rs = (dictable(a = range(10)) * dictable(b = range(10)))(c = lambda a, b: a*b)
>>> t.insert_many(rs)

We now examine how we drill down to the document(s) we want: 

>>> assert len(t.inc(a = 1)) == 10
>>> assert len(t.exc(a = 1)) == 90
>>> assert isinstance(t.inc(a = 1), mongo_cursor) ## it is chain-able
>>> assert len(t.find(a = 1).find(b = [1,2,3,4])) == 4

You can use the original collection too but it is much clunkier

>>> spec = {'$and': [{"a": {"$eq": 1}}, {"b": {"$in": [1, 2, 3, 4]}}]}
>>> assert c.count_documents(spec) == 4
>>> c.find(spec) # That is OK
>>> with pytest.raises(AttributeError):  # not OK, cannot chain queries
>>>     c.find({"a": {"$eq": 1}}).find({"b": {"$in": [1, 2, 3, 4]}})

Just like a mongo.Cursor, c.find(spec), t is also iterable over the documents:

>>> sum([doc for doc in t.find(a = 1).find(b = [1,2,3,4])], dictable())
>>> dictable[4 x 4]
>>> _id                     |a|b|c
>>> 60244a9a57099f75f3d8ad9b|1|1|1
>>> 60244a9a57099f75f3d8ad9c|1|2|2
>>> 60244a9a57099f75f3d8ad9d|1|3|3
>>> 60244a9a57099f75f3d8ad9e|1|4|4

The mongo_cursor also supports:

>>> t.find(a = 1).find(b = [1,2,3,4])[::]
>>> dictable[4 x 4]
>>> _id                     |a|b|c
>>> 60244a9a57099f75f3d8ad9b|1|1|1
>>> 60244a9a57099f75f3d8ad9c|1|2|2
>>> 60244a9a57099f75f3d8ad9d|1|3|3
>>> 60244a9a57099f75f3d8ad9e|1|4|4

as well as sorting, e.g.

>>> t.sort('c', 'a')

Finally, we can access specific documents using a dict in the getitem:

>>> assert t[dict(a = 7, b = 8)].c == 56

This will throw an exception if more than one document matches the criteria.

column access
-------------
What values can 'b' take within our documents? 

>>> assert t.b == t.distinct('b') == c.distinct('b') == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

Can we read just a and b? In MongoDB this requires us generating a 'projection'. Luckily the mongo_cursor makes light work of this:

>>> t.inc(a = [1,2,3], b = [1,2,3])['a']
>>> {'$and': [{'a': {'$in': [1, 2, 3]}}, {'b': {'$in': [1, 2, 3]}}]} {'a': 1}  ##<----- the last bit is the projection
>>> documents count: 9 
>>> dict_keys(['_id', 'a']) # <---- since we projected to see 'a' only, that is the only key that is available 

And here is how we grab just all 'a' and 'b':

>>> t.inc(a = 1).inc(q.b>6)[['a', 'b']][::]
>>> dictable[3 x 3]
>>> _id                     |a|b
>>> 60244a9a57099f75f3d8ada1|1|7
>>> 60244a9a57099f75f3d8ada2|1|8
>>> 60244a9a57099f75f3d8ada3|1|9

add/remove column
-------------
>>> del t['c']

adding new columns:
-------------------
>>> t.set(c = 5)
>>> t.set(c = lambda a, b: a*b) # yes, this works. The mongo_cursor will iterate over the documents and save the result

mongo_reader vs mongo_cursor
-----------------------------
Because it is very easy to do stuff in MongoDB that can cause damage (e.g. t.drop()), we also introduce a mongo_reader that lacks the 'write' functionality of the mongo_cursor. 
Similarly, the mongo_pk_reader is the read-only version of the mongo_pk_cursor we shall meet shortly.

>>> t = mongo_table('test', 'test', mode ='w') # mongo_cursor
>>> r = mongo_table('test', 'test', mode ='r') # mongo_reader
>>> with pytest.raises(AttributeError):
>>>     r.drop()


mongo_pk_table
==============
Suppose we want to have a table of people

>>> from pyg import *; import pymongo as pym; import pytest
>>> t = mongo_table(table = 'test', db = 'test')
>>> c = pym.MongoClient()['test']['test']
>>> pk = mongo_table(table = 'test', db = 'test', pk = ['name', 'surname'])

>>> t.drop()
>>> d = dictable(name = ['alan', 'alan', 'barbara', 'chris'], surname = ['adams', 'jones', 'brown', 'jones'], age = [1,2,3,4])
>>> pk.insert_many(d)
>>> assert len(pk) == len(t) == 4

A year has passed... time to update the records

>>> d = d(age = lambda age: age + 1)
>>> pk.insert_many(d)
>>> assert len(pk) == 4
>>> assert len(t) == 8

What??? 

>>> pk
>>> <class 'pyg.mongo._pk_cursor.mongo_pk_cursor'> for Collection(Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'test'), 'test') 
>>> M{'$and': [{"_deleted": {"$exists": false}}, {"_pk": {"$eq": ["name", "surname"]}}]} None #<---- filter
>>> documents count: 4 
>>> dict_keys(['_id', '_obj', '_pk', 'age', 'name', 'surname'])

What we see is that pk only looks at a subset of the total documents in the table: those undeleted and those with _pk equal to [name,surname]. pk provides full audit:

>>> t[::]
>>> dictable[8 x 6]
>>> _pk                |name   |age|_deleted                  |_id                     |surname
>>> ['name', 'surname']|alan   |2  |None                      |60245a5f0466be18483c4ad2|adams  
>>> ['name', 'surname']|alan   |3  |None                      |60245a5f0466be18483c4ad3|jones  
>>> ['name', 'surname']|barbara|4  |None                      |60245a5f0466be18483c4ad4|brown  
>>> ...8 rows...
>>> ['name', 'surname']|alan   |2  |2021-02-10 22:13:35.242000|60245a8f0466be18483c4ad7|jones  
>>> ['name', 'surname']|barbara|3  |2021-02-10 22:13:35.252000|60245a8f0466be18483c4ad8|brown  
>>> ['name', 'surname']|chris  |4  |2021-02-10 22:13:35.260000|60245a8f0466be18483c4ad9|jones  

There are obvioursly some small differences on how pk works but broadly, it is just like a normal mongo_cursor with an added filter to zoom onto the records that maintain the primary-key table.

* you cannot insert docs without primary keys all present:
* the drop() command does not actually delete the documents, they are simply 'marked' as deleted.
* to get from a mongo_pk_cursor to mongo_cursor, simply go pk.raw

>>> with pytest.raises(ValueError):
>>>     pk.insert_one(dict(no_name_or_surname = 'James')) # cannot insert with no PK
>>> pk.drop()
>>> assert len(pk) == 0 and len(t) == len(pk.raw) == 8 
