from pyg import tree_to_items, tree_update, tree_to_table, items_to_tree, tree_repr, is_tree
import numpy as np

tree = dict(school = dict(name = 'Kings Primary', 
                          teachers = dict(id001 = dict(name = 'Abe', surname = 'Abrahams', subject = 'Art', status = 'active'),
                                          id002 = dict(name = 'Barbaran', surname = 'Barbarosa', subject = 'Biology', status = 'best'), 
                                          id003 = dict(name = 'Carl', surname = 'Carlos', subject = 'Chemistry', status = 'cautioned')),
                          students = dict(sid01 = dict(name = 'Zante', surname = 'Zimmerman', scores = dict(Art = 100, Biology = 90)),
                                          sid02 = dict(name = 'Xaviar', surname = 'Xantium', scores = dict(Art = 90, Chemistry = 50)),
                                          sid03 = dict(name = 'Walter', surname = 'Waltham', scores = dict(Biology = 60, Chemistry = 70)))))


big_tree = {'school': {'name': 'Kings Primary',
                      'teachers': {'id001': {'name': 'Abe', 'surname': 'Abrahams','subject': 'Art', 'status': 'active'},
                                   'id002': {'name': 'Barbaran', 'surname': 'Barbarosa', 'subject': 'Biology', 'status': 'best'},
                                   'id003': {'name': 'Carl', 'surname': 'Carlos', 'subject': 'Chemistry', 'status': 'cautioned'},
                                   'id004': {'name': 'Donald', 'surname': 'Davidson', 'subject': 'Drama', 'status': 'discharged'}},
                      'students': {'sid01': {'name': 'Zante', 'surname': 'Zimmerman', 'scores': {'Art': 100, 'Biology': 90, 'Drama': 30}},
                                   'sid02': {'name': 'Xaviar','surname': 'Xantium', 'scores': {'Art': 90, 'Chemistry': 50}},
                                   'sid03': {'name': 'Walter', 'surname': 'Waltham', 'scores': {'Biology': 60, 'Chemistry': 70, 'Drama': 40}},
                                   'sid04': {'name': 'Victoria', 'surname': 'Van Der Full', 'scores': {'Biology': 90, 'Drame': 100}}},
                      'address': '2 Queens Road'}}

items =  [('school', 'name', 'Kings Primary'),
         ('school', 'teachers', 'id001', 'name', 'Abe'),
         ('school', 'teachers', 'id001', 'surname', 'Abrahams'),
         ('school', 'teachers', 'id001', 'subject', 'Art'),
         ('school', 'teachers', 'id001', 'status', 'active'),
         ('school', 'teachers', 'id002', 'name', 'Barbaran'),
         ('school', 'teachers', 'id002', 'surname', 'Barbarosa'),
         ('school', 'teachers', 'id002', 'subject', 'Biology'),
         ('school', 'teachers', 'id002', 'status', 'best'),
         ('school', 'teachers', 'id003', 'name', 'Carl'),
         ('school', 'teachers', 'id003', 'surname', 'Carlos'),
         ('school', 'teachers', 'id003', 'subject', 'Chemistry'),
         ('school', 'teachers', 'id003', 'status', 'cautioned'),
         ('school', 'students', 'sid01', 'name', 'Zante'),
         ('school', 'students', 'sid01', 'surname', 'Zimmerman'),
         ('school', 'students', 'sid01', 'scores', 'Art', 100),
         ('school', 'students', 'sid01', 'scores', 'Biology', 90),
         ('school', 'students', 'sid02', 'name', 'Xaviar'),
         ('school', 'students', 'sid02', 'surname', 'Xantium'),
         ('school', 'students', 'sid02', 'scores', 'Art', 90),
         ('school', 'students', 'sid02', 'scores', 'Chemistry', 50),
         ('school', 'students', 'sid03', 'name', 'Walter'),
         ('school', 'students', 'sid03', 'surname', 'Waltham'),
         ('school', 'students', 'sid03', 'scores', 'Biology', 60),
         ('school', 'students', 'sid03', 'scores', 'Chemistry', 70)]


more_items = [('school', 'address', '2 Queens Road'),
            ('school', 'teachers', 'id004', 'name', 'Donald'),
            ('school', 'teachers', 'id004', 'surname', 'Davidson'),
            ('school', 'teachers', 'id004', 'subject', 'Drama'),
            ('school', 'teachers', 'id004', 'status', 'discharged'),
            ('school', 'students', 'sid01', 'scores', 'Drama', 30),
            ('school', 'students', 'sid03', 'scores', 'Drama', 40),
            ('school', 'students', 'sid04', 'name', 'Victoria'),
            ('school', 'students', 'sid04', 'surname', 'Van Der Full'),
            ('school', 'students', 'sid04', 'scores', 'Biology', 90),
            ('school', 'students', 'sid04', 'scores', 'Drame', 100)]

items_to_tree(more_items)

def test_tree_to_items():
    assert tree_to_items(tree) == items


def test_items_to_tree():
    assert items_to_tree(items) == tree

def test_items_to_tree_additional():
    assert items_to_tree(more_items, tree) ==  big_tree

def test_tree_update():
    res = tree_update(tree, dict(school = dict(name = "King's Primary", status = 'Fictionary')))    
    assert res['school']['name'] ==  "King's Primary"
    assert res['school']['status'] ==  "Fictionary"
    assert res['school']['students'] ==  tree['school']['students']
    assert res['school']['teachers'] ==  tree['school']['teachers']

def test_tree_update_with_ignore():
    update = dict(a = None, b = np.nan, c = 0)
    tree = dict(a = 1, b = 2, c = 3)
    assert tree_update(tree, update) == update
    assert tree_update(tree, update, ignore = [None]) == dict(a = 1, b = np.nan, c = 0)
    assert tree_update(tree, update, ignore = [None, np.nan]) == dict(a = 1, b = 2, c = 0)
    assert tree_update(tree, update, ignore = [None, np.nan, 0]) == tree


def test_tree_to_table():
    table = tree_to_table(tree, 'school/teachers/%id/name/%common_name')
    assert table == [{'common_name': 'Abe', 'id': 'id001'}, {'common_name': 'Barbaran', 'id': 'id002'}, {'common_name': 'Carl', 'id': 'id003'},{'common_name': 'Donald', 'id': 'id004'}]
    assert tree_to_table(tree, 'school/teacher/%id') == []
    assert tree_to_table(tree, 'nomatch') == []
    assert tree_to_table(tree, '') == []
    assert tree_to_table(tree, []) == [{}]

def test_tree_repr():
    assert tree_repr(items_to_tree(items)) == "dictattr\nschool:\n    dictattr\n    name:\n        Kings Primary\n    teachers:\n        dictattr\n        id001:\n            {'name': 'Abe', 'surname': 'Abrahams', 'subject': 'Art', 'status': 'active'}\n        id002:\n            dictattr\n            name:\n                Barbaran\n            surname:\n                Barbarosa\n            subject:\n                Biology\n            status:\n                best\n        id003:\n            dictattr\n            name:\n                Carl\n            surname:\n                Carlos\n            subject:\n                Chemistry\n            status:\n                cautioned\n    students:\n        dictattr\n        sid01:\n            {'name': 'Zante', 'surname': 'Zimmerman', 'scores': {'Art': 100, 'Biology': 90}}\n        sid02:\n            {'name': 'Xaviar', 'surname': 'Xantium', 'scores': {'Art': 90, 'Chemistry': 50}}\n        sid03:\n            dictattr\n            name:\n                Walter\n            surname:\n                Waltham\n            scores:\n                {'Biology': 60, 'Chemistry': 70}"
    assert tree_repr(dict(a = [dict(b = 1, c = 3)], b = dict(c = 5, d = dict(e = 4), e = 8), c = dict(a = 1, b = 2))) == "a:\n    [{'b': 1, 'c': 3}]\nb:\n    {'c': 5, 'd': {'e': 4}, 'e': 8}\nc:\n    {'a': 1, 'b': 2}"

def test_is_tree():
    assert not is_tree(5)
    assert not is_tree('test')
    assert not is_tree('test/what/is/going/on')
    assert is_tree('test/%what/%is/going/on')
    assert is_tree('%test/%what/%is/going/on')
    assert not is_tree('test/more/tests')
    assert not is_tree('test/%more%/%tests%')
    

def test_tree_to_table_empties():
    tree = 5
    pattern = 'reject/%because/%tree/not/a/tree'
    assert tree_to_table(tree, pattern) == []
    assert tree_to_table('hello', 'hello') == [{}]
