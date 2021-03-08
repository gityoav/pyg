from pyg import get_logger, read_csv

def test_logger():
    a = get_logger('test', file = 'c:/temp/temp.log')
    b = get_logger('test')
    assert b==a
    a.warning('testing')
    logs = read_csv('c:/temp/temp.log')
    assert 'test - WARNING - testing' in logs[-1][-1]

