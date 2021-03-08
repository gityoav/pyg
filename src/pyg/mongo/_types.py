import pymongo as pym

__all__ = ['is_collection', 'is_cursor']

# def is_client(value):
#     """is the value a pymongo.MongoClient"""
#     return isinstance(value, pym.mongo_client.MongoClient)

# def is_db(value):
#     """is the value a pymongo.Database"""
#     return isinstance(value, pym.database.Database)

def is_collection(value):
    """ is the value a pymongo.Collection (equivalent of a table)"""
    return isinstance(value, pym.collection.Collection)

def is_cursor(value):
    """ is the value a pymongo.Cursor"""
    return isinstance(value, pym.cursor.Cursor)


    