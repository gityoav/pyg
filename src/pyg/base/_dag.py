from pyg.base._dict import dict_invert
from pyg.base._as_list import as_list
from _collections_abc import dict_keys, dict_values

"""
This module is designed to remove dependency on networkx as it is slow to load and run
Since we only use topological_sort, our implementation is more specialized and hence faster (we don't test for cyclic nature of graph etc.)
"""
DAGs = {}
GADs = {}


def topological_sort(dag, node):    
    """
    This is a breadth-first topological sort, returning 
    The breadth-first is faster for even medium-sized graphs and also needs no inversion.

    The directed acyclic graph is stored as a dict with edges that look like:
        dag[parent] = {child : None for child in (parent, child) set of edges}

    :Parameters:
    ----------
    dag : dict, a directed acyclic graph
        DESCRIPTION.
    node : a key
        a node in the graph acting as the top of the subgraph

    :Returns:
    -------
    a dict of dicts: 
        node2gen: from nodes to their generations
        gen2node: from a generation to a set of its node

    :Example: constructing a DAG:
    ---------
    >>> dag = dict(a = dict(b = 0, c = 0), b = dict(c = 0, d = 0, e = 0), c = dict(e = 0), d = dict(e = 0))
    >>> node = 'a'
    >>> assert topological_sort(dag, node)['node2gen'] == {'a': 0, 'b': 1, 'c': 2, 'e': 3, 'd': 2}


    :Example: multiple nodes descendants:
    --------------------------------------
    >>> assert descendants(dag, ['c', 'd']) == ['c', 'd', 'e'] 
    
    NOTE: we cannot provide the list as a tuple. A tuple is immutable in itself and will be considered to be a key in the dag.
    

    :Example: we compare to networkx alternative:
    ---------
    >>> dag = dict(a = dict(b = 0, c = 0), b = dict(c = 0, d = 0, e = 0), c = dict(e = 0), d = dict(e = 0))
    >>> node = 'a'
    >>> ts = topological_sort(dag, node)
    >>> assert ts['node2gen'] == {'a': 0, 'b': 1, 'c': 2, 'e': 3, 'd': 2}

    >>> import networkx as nx
    >>> G = nx.DiGraph()
    >>> for a in dag:
    >>>     for b in dag[a]:
    >>>         G.add_edge(a,b)

    >>> def nx_topological_sort(G, node):
    >>>    nodes = {node} | nx.descendants(G, node)
    >>>    return list(nx.algorithms.dag.topological_sort(G.subgraph(nodes)))


    >>> def compare_topological_sort_and_nx(gen2node,y):
    >>>     if 'gen2node' in gen2node:
    >>>         gen2node = gen2node['gen2node']
    >>>     i = 1    
    >>>     for g, nodes in sorted(gen2node.items())[1:]:
    >>>         n = len(nodes)            
    >>>         if sorted(y[i:i+n]) != sorted(nodes):
    >>>             return False
    >>>         i+=n
    >>>     return True

    >>> x = timer(topological_sort, n = 10000)(dag, node)
    >>> y = timer(nx_topological_sort, n = 10000)(G, node)
    >>> assert compare_topological_sort_and_nx(x,y)

    Just to compare speeds, our topological sort is approx 10 times faster...

    >>> x = timer(topological_sort, n = 10000, time = True)(dag, node)
    >>> y = timer(nx_topological_sort, n = 10000, time = True)(G, node)
    >>> assert x * 10 < y   
    
    
    >>> DAGs['test'] = {}
    >>> dag = get_DAG('test')
    >>> for b in range(100):
    >>>     for a in range(b-3,b+3):
    >>>         add_edge('a%i'%a, 'b%i'%b, dag)
    >>> for c in range(100):
    >>>     for a in range(c-3,c+3):
    >>>         add_edge('a%i'%a, 'c%i'%c, dag)
    >>>     for b in range(c-3,c+3):
    >>>         add_edge('b%i'%b, 'c%i'%c, dag)
    >>> for d in range(100):
    >>>     for c in range(d-10,d+10):
    >>>         add_edge('c%i'%c, 'd%i'%d, dag)
    >>> for e in range(100):
    >>>     for c in range(e-10,e+10):
    >>>         add_edge('c%i'%c, 'e%i'%e, dag)
    >>>     for a in range(e-3,e+3):
    >>>         add_edge('a%i'%a, 'e%i'%e, dag)

    >>> import networkx as nx
    >>> G = nx.DiGraph()
    >>> for a in dag:
    >>>     for b in dag[a]:
    >>>         G.add_edge(a,b)

    >>> node = 'a50'

    >>> x = timer(topological_sort, n = 1000)(dag, node)
    >>> y = timer(nx_topological_sort, n = 1000)(G, node)
    >>> assert compare_topological_sort_and_nx(x,y)

    >>> x = timer(topological_sort, n = 1000, time = True)(dag, node)
    >>> y = timer(nx_topological_sort, n = 1000, time = True)(G, node)
    >>> assert x * 10 < y   
    """
    dag = get_DAG(dag)
    nodes = set(node) if isinstance(node, (list, set, dict_keys, dict_values)) else set([node])
    gen2node = {0 : nodes}
    node2gen = {node : 0 for node in nodes}
    gen = 0
    while len(gen2node[gen]):
        next_gen = gen + 1
        gen2node[next_gen] = set()
        for node in gen2node[gen].copy():
            if node in dag:
                children = dag[node]
                for child in children:                
                    if child in node2gen:
                        cgen = node2gen[child]
                        if cgen < next_gen:
                            gen2node[cgen].remove(child)
                            gen2node[next_gen].add(child)
                            node2gen[child] = next_gen
                    else:
                        gen2node[next_gen].add(child)
                        node2gen[child] = next_gen
        gen = gen + 1
    del gen2node[gen]
    return dict(gen2node = gen2node, node2gen = node2gen)
 

    


def descendants(dag, node, exc = None):
    """
    returns the descendants of the node (with node itself as first element) sorted topologically

    Parameters
    ----------
    dag : DAG
        A dag or a reference to a DAG
    node : a key
        A key to a node in the graph.
    exc: None or int or list of ints
        Most useful is to use exc = 0 to exclude the zeroth generation from future calculations

    Returns
    -------
    list
        list of keys for children of node in the graph.
    """
    gen = topological_sort(dag, node)['gen2node']
    if len(gen) == 0:
        return []
    else:
        excs = as_list(exc)
        if len(excs):
            nodes = [n for g, n in sorted(gen.items()) if g not in excs]
        else:
            nodes = gen.values()
        return sum(map(list,nodes), [])


def get_DAG(dag = None):
    # from networkx import DiGraph
    # return DiGraph()
    if isinstance(dag, dict):
        return dag
    if dag not in DAGs:
        DAGs[dag] = {}
    return DAGs[dag]

def dag_inverse(dag = None):
    d = get_DAG(dag)
    GADs[dag] = g = {}
    for a in d:
        for b, value in d[a].items():
            if b not in g:
                g[b] = {}
            g[b][a] = value 
    GADs[dag] = g
    return g


def add_edge(parent, child, dag = None, value = None):
    dag = get_DAG(dag)
    if parent not in dag:
        dag[parent] = {child : value}
    else:
        dag[parent].update({child : value})
    return dag

def del_edge(parent, child, dag = None):
    dag = get_DAG(dag)
    if parent in dag and child in dag[parent]:
        del dag[parent][child]
    return dag

