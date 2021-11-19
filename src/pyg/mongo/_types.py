from pyg.base import passthru, decode, encode, is_str, as_list
import pymongo as pym
import motor
from pyg.mongo._encoders import csv_write, parquet_write, _csv, _parquet
from functools import partial

__all__ = ['is_collection', 'is_cursor', 'as_collection']

# def is_client(value):
#     """is the value a pymongo.MongoClient"""
#     return isinstance(value, pym.mongo_client.MongoClient)

# def is_db(value):
#     """is the value a pymongo.Database"""
#     return isinstance(value, pym.database.Database)

def is_collection(value):
    """ is the value a pymongo.Collection (equivalent of a table)"""
    return isinstance(value, (pym.collection.Collection, motor.MotorCollection))

def is_cursor(value):
    """ is the value a pymongo.Cursor"""
    return isinstance(value, (pym.cursor.Cursor, motor.motor_tornado.MotorCursor))


def as_collection(collection):
    if is_collection(collection):
        return collection
    elif is_cursor(collection):
        return collection.collection
    

