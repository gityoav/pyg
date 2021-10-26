from pyg import get_DAG, add_edge, del_edge, topological_sort, descendants
from pyg.base._dag import DAGs

def test_get_DAG():
    DAGs[None] = {}
    dag = get_DAG()
    assert len(dag) == 0
    dag = add_edge('a', 'b')
    assert len(get_DAG()) == 1
    dag = add_edge('a', 'c')
    dag = add_edge('b', 'c', dag)
    assert len(get_DAG()) == 2
    assert len(get_DAG()['a']) == 2
    dag = del_edge('a', 'c')
    assert len(get_DAG()['a']) == 1
    assert topological_sort(None, 'a') == {'a': 0, 'b': 1, 'c': 2}
    assert descendants(None, 'a') == ['a', 'b', 'c']
    assert descendants(None, 'b') == ['b', 'c']
    assert descendants(None, 'a', exc = 0) == ['b', 'c']
