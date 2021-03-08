from pyg import Q, q
import re

def D(value):
    if isinstance(value, dict):
        return {x : D(y) for x, y in value.items()} ### this converts mdict to normal dict
    elif isinstance(value, list):
        return [D(y) for y in value]
    else:
        return value
    

def test_q():
    assert D(q.a == 1) == {'a': {'$eq': 1}}
    assert D(q.a != 1) == {'a': {'$ne': 1}}
    assert D(q.a <= 1) == {'a': {'$lte': 1}}
    assert D(q.a >= 1) == {'a': {'$gte': 1}}
    assert D(q.a < 1) == {'a': {'$lt': 1}}
    assert D(q.a > 1) == {'a': {'$gt': 1}}
    assert D((q.a == 1) + (q.b == 2)) == {'$and': [{'a': {'$eq': 1}}, {'b': {'$eq': 2}}]}
    assert D(-(q.b == 2)) == {"$not": {"b": {"$eq": 2}}}
    assert D(q.a % 2 == 1) == {'a': {'$mod': [2, 1]}}
    assert D(-(q.a % 2 == 1)) == {'$not': {'a': {'$mod': [2, 1]}}}
    assert D((q.a ==1) + (q.b == 1)) == {'$and': [{'a': {'$eq': 1}}, {'b': {'$eq': 1}}]}
    assert D((q.a ==1) & (q.b == 1)) == {'$and': [{'a': {'$eq': 1}}, {'b': {'$eq': 1}}]}
    assert D((q.a ==1) - (q.b == 1)) == {'$and': [{'$not': {'b': {'$eq': 1}}}, {'a': {'$eq': 1}}]}
    
    assert D((q.a % 2 == 1)|(q.b == 1)) == {'$or': [{'a': {'$mod': [2, 1]}}, {'b': {'$eq': 1}}]}
    assert D((q.a % 2 == 1) & (q.b == 1)) == {'$and': [{'a': {'$mod': [2, 1]}}, {'b': {'$eq': 1}}]}
    assert D((q.a % 2 == 1) + (q.b == 1)) == {'$and': [{'a': {'$mod': [2, 1]}}, {'b': {'$eq': 1}}]}
    assert D((q.a % 2 == 1) - (q.b == 1)) == {'$and': [{'$not': {'b': {'$eq': 1}}}, {'a': {'$mod': [2, 1]}}]}
    assert D(q.a.isinstance(float)) == {"a": {"$type": [1, 19]}}

    assert D(-(q.a % 2 == 1)) == {'$not': {'a': {'$mod': [2, 1]}}}
    assert D(+(q.a % 2 == 1)) == {'a': {'$mod': [2, 1]}}

    assert D(q.a % 2 + q.b == 1) == {'$and': [{'a': {'$not': {'$mod': [2, 0]}}}, {'b': {'$eq': 1}}]}
    assert D(q.a % 2 + (q.b == 1)) == {'$and': [{'a': {'$not': {'$mod': [2, 0]}}}, {'b': {'$eq': 1}}]}
    assert D(q.a % 2 & (q.b == 1)) == {'$and': [{'a': {'$not': {'$mod': [2, 0]}}}, {'b': {'$eq': 1}}]}
    assert D(q.a % 2 - (q.b == 1)) == {'$and': [{'$not': {'b': {'$eq': 1}}}, {'a': {'$not': {'$mod': [2, 0]}}}]}
    assert D(~(q.a % 2)) == {'a': {'$mod': [2, 0]}}
    assert D(+(q.a % 2)) == {'a': {'$not': {'$mod': [2, 0]}}}

    
    assert D(q['some text'] == 1) == {'some text': {'$eq': 1}}
    assert D(q.a == re.compile('^test')) == {'a': {'regex': '^test'}}
    assert D(q.a != re.compile('^test')) == {'a': {'$not': {'regex': '^test'}}}
    assert D(q.a != [1]) == {'a': {'$ne': [1]}}
    assert D(q.a == [1]) == {'a': {'$eq': 1}}
    assert D(q.a == [[1]]) == {'a': {'$eq': [1]}}

    assert D(q.a['b'] == 1) == {'a.b': {'$eq': 1}}
    assert D(q.a.b == 1) == {'a.b': {'$eq': 1}}
    assert D(+q.a) == {'a': {'$exists': True}}
    assert D(q.a.exists) == {'a': {'$exists': True}}
    assert D(-q.a) == {'a': {'$exists': False}}
    assert D(q.a.not_exists) == {'a': {'$exists': False}}
    assert D(q.a == True) == {'a': {'$in': [True, 1]}}
    assert D(q.a == False) == {'a': {'$in': [False, 0]}}
    assert D(q.a == dict(b=1, c=2)) == {'$and': [{'a.b': {'$eq': 1}}, {'a.c': {'$eq': 2}}]}
    
    assert D(q.a.exists & q.b == 2) == {'$and': [{'a': {'$exists': True}}, {'b': {'$eq': 2}}]}
    assert D(q.a.exists | q.b == 2) == {'$or': [{'a': {'$exists': True}}, {'b': {'$eq': 2}}]}
    
    assert D(q.a & q.b == 1) == {'$and': [{'a': {'$exists': True}}, {'b': {'$eq': 1}}]}
    assert D(q.a + q.b == 1) == {'$and': [{'a': {'$exists': True}}, {'b': {'$eq': 1}}]}
    assert D(q.a | (q.b == 1)) == {'$or': [{'a': {'$exists': True}}, {'b': {'$eq': 1}}]}
    assert D(q.a - q.b) == {'$and': [{'a': {'$exists': True}}, {'b': {'$exists': False}}]}
    assert D(q.a + q.b) == {'$and': [{'a': {'$exists': True}}, {'b': {'$exists': True}}]}
    assert D(q.a - (q.b == 1)) == {'$and': [{'$not': {'b': {'$eq': 1}}}, {'a': {'$exists': True}}]}
    
    assert D(q.a[5:10]) == {'$and': [{'a': {'$gte': 5}}, {'a': {'$lt': 10}}]}
    assert D(q.a[5:]) == {'a': {'$gte': 5}}
    assert D(q.a[:10]) == {'a': {'$lt': 10}}
    assert D(q.a[::] == 1) == {'a': {'$eq': 1}}
    
    assert D(q.a[2]) ==  {'a': {'$eq': 2}}
    assert D(+q.a.b) ==  {'a.b': {'$exists': True}}

    assert D((q.a == 1) | q.b > 1) == {'$or': [{'a': {'$eq': 1}}, {'b': {'$gt': 1}}]}
    assert D((q.a == 1) | q.b >= 1) == {'$or': [{'a': {'$eq': 1}}, {'b': {'$gte': 1}}]}
    assert D((q.a == 1) | q.b != 1) == {'$or': [{'a': {'$eq': 1}}, {'b': {'$ne': 1}}]}
    assert D((q.a == 1) | q.b == 1) == {'$or': [{'a': {'$eq': 1}}, {'b': {'$eq': 1}}]}
    assert D((q.a == 1) | q.b <= 1) == {'$or': [{'a': {'$eq': 1}}, {'b': {'$lte': 1}}]}
    assert D((q.a == 1) | q.b < 1) == {'$or': [{'a': {'$eq': 1}}, {'b': {'$lt': 1}}]}


def test_Q_with_keys():
    x = Q(['Adam Aaron', 'Beth Brown', 'James Joyce'])
    assert D((x.adam_aaron == 1) & (x.JAMES_JOYCE == 2)) == {'$and': [{'Adam Aaron': {'$eq': 1}}, {'James Joyce': {'$eq': 2}}]}
    assert sorted(dir(x)) == sorted(['ADAM_AARON',
                                       'Adam Aaron',
                                         'Adam_Aaron',
                                         'BETH_BROWN',
                                         'Beth Brown',
                                         'Beth_Brown',
                                         'JAMES_JOYCE',
                                         'James Joyce',
                                         'James_Joyce',
                                         'adam_aaron',
                                         'beth_brown',
                                         'james_joyce'])

    x = Q(['@@Adam%+Aaron', '++Beth---Brown++', '---James%%%   Joyce%'])
    assert D((x.adam_aaron == 1) & (x.JAMES_JOYCE == 2)) == {'$and': [{'---James%%%   Joyce%': {'$eq': 2}}, {'@@Adam%+Aaron': {'$eq': 1}}]}
                        
def test_q_callable():
    assert D(q(a = 1, b = 2)) == {'$and': [{'a': {'$eq': 1}}, {'b': {'$eq': 2}}]}
    assert D(q(a = 1, b = 2)) == {'$and': [{'a': {'$eq': 1}}, {'b': {'$eq': 2}}]}
    assert D(q([q.a == 1, q.b == 2])) == {'$or': [{'a': {'$eq': 1}}, {'b': {'$eq': 2}}]}
