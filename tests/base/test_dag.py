from pyg import get_DAG, add_edge, del_edge, topological_sort, descendants, dict_invert
from pyg.base._dag import DAGs
import networkx as nx


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



def dag2G(dag):
    G = nx.DiGraph()
    for a in dag:
        for b in dag[a]:
            G.add_edge(a,b)
    return G


def compare_topological_sort_and_nx(x,y):
    i = 1    
    for g, nodes in sorted(dict_invert(x).items())[1:]:
        n = len(nodes)            
        if sorted(y[i:i+n]) != sorted(nodes):
            return False
        i+=n
    return True

def nx_topological_sort(dag, node):
   G = dag2G(dag)
   nodes = {node} | nx.descendants(G, node)
   return list(nx.algorithms.dag.topological_sort(G.subgraph(nodes)))

def test_topological_sort():
    dag = dict(a = dict(b = 0, c = 0), b = dict(c = 0, d = 0, e = 0), c = dict(e = 0), d = dict(e = 0))
    node = 'a'
    assert topological_sort(dag, node) == {'a': 0, 'b': 1, 'c': 2, 'e': 3, 'd': 2}
    x = topological_sort(dag, node)
    y = nx_topological_sort(dag, node)
    assert compare_topological_sort_and_nx(x,y)

    
    DAGs['test'] = {}
    dag = get_DAG('test')
    for b in range(100):
        for a in range(b-3,b+3):
            add_edge('a%i'%a, 'b%i'%b, dag)
    for c in range(100):
        for a in range(c-3,c+3):
            add_edge('a%i'%a, 'c%i'%c, dag)
        for b in range(c-3,c+3):
            add_edge('b%i'%b, 'c%i'%c, dag)
    for d in range(100):
        for c in range(d-10,d+10):
            add_edge('c%i'%c, 'd%i'%d, dag)
    for e in range(100):
        for c in range(e-10,e+10):
            add_edge('c%i'%c, 'e%i'%e, dag)
        for a in range(e-3,e+3):
            add_edge('a%i'%a, 'e%i'%e, dag)

    for node in ['a50', 'a10', 'b40', 'c13']:
        x = topological_sort(dag, node)
        y = nx_topological_sort(dag, node)
        assert compare_topological_sort_and_nx(x,y)
