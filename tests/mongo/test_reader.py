from pyg import clone_cursor, mongo_table, mongo_reader, mongo_cursor, passthru
import pytest

def test_clone_cursor():
    reader = mongo_table('test', 'test', mode = 'r')
    assert mongo_reader(reader) == reader
    cursor = mongo_cursor(reader)
    assert isinstance(cursor, mongo_cursor)


def test_cursor_reader_writer():
    cursor = mongo_table('test', 'test', mode = 'r', writer = False, reader = False)
    assert cursor._writer == passthru
    assert cursor._reader == passthru


def test_mongo_reader_spec_projection():
    reader = mongo_table('test', 'test', mode = 'r')
    assert reader.projection is None    
    assert reader[['a','b']].projection == {'a': 1, 'b': 1}   
    assert reader.spec == {}
    assert reader.find(a = 1).spec == {"a": {"$eq": 1}}  
    assert reader.specify() == reader
    assert reader.project() == reader
    assert reader.exc(a = 1).spec == {"a": {"$ne": 1}}
    assert reader.find(a = 1).spec == {"a": {"$eq": 1}}
    

def test_reader_is_no_writer():
    reader = mongo_table('test', 'test', mode = 'r')
    with pytest.raises(AttributeError):
        reader.insert_one(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.insert_many(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.delete_one(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.delete_many(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.set(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.drop(dict(a =1))    


