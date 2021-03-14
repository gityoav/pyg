*********
pyg.mongo
*********

A few words on MongoDB, a no-SQL database versus SQL:

- Mongo has 'collections' that are the equivalent of tables 
- Mongo will refer to 'documents' instead of traditional records. Those records are unstructured and look like trees: dicts of dicts. They contain arbitary objects as well as just the primary types a SQL database is designed to support.
- Mongo collections do not have the concept of primary keys
- Mongo WHERE SQL clause is replaced by a query in a form of a dict "presented" to the collection object.
- Mongo SELECT SQL clause is replaced by a 'projection' on the cursor, specifying what fields are retrieved.

Query generator
================
We start by simplifying the way we generate mongo query dictionaries.

q and Q
--------
.. autoclass:: pyg.mongo._q.Q
   :members: 


Tables in Mongo
================

mongo_cursor
------------
mongo_cursor has hybrid functionality of a Mongo cursor and Mongo collection objects. 

.. autoclass:: pyg.mongo._cursor.mongo_cursor
   :members: 

mongo_reader
------------
mongo_reader is a read-only version of the cursor to avoid any unintentional damage to database.

.. autoclass:: pyg.mongo._reader.mongo_reader
   :members: 

mongo_pk_reader
------------
mongo_pk_reader extends the standard reader to handle tables with primary keys (pk) while being read-only.

.. autoclass:: pyg.mongo._pk_reader.mongo_pk_reader
   :members:

mongo_pk_cursor
---------------
mongo_pk_cursor is our go-to object and it manages all our primary-keyed tables.
.. autoclass:: pyg.mongo._pk_cursor.mongo_pk_cursor
   :members:

encoding docs before saving to mongo
====================================
Before we save data to Mongo, we may need to transform it, especially if we are to save pd.DataFrame. By default, we encode them into bytes and push to mongo. 
You can choose to save pandas dataframes/series as .parquet files and numpy arrays into .npy files.

parquet_write
-------------
.. autofunction:: pyg.mongo._encoders.parquet_write


csv_write
----------
.. autofunction:: pyg.mongo._encoders.csv_write


cells in Mongo
==============
Now that we have a database, we construct cells that can load/save data to collections.

db_cell
-------
.. autoclass:: pyg.mongo._db_cell.db_cell
   :members:

periodic_cell
-------------
.. autoclass:: pyg.mongo._periodic_cell.periodic_cell
   :members:


get_cell
--------
.. autofunction:: pyg.mongo._cell_get.get_cell

db_save
-------
.. autofunction:: pyg.mongo._db_cell.db_save

db_load
-------
.. autofunction:: pyg.mongo._db_cell.db_load
