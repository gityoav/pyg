from pyg.base._dict import dict_invert
from pyg.base._as_list import as_list
"""
This module is designed to remove dependency on networkx as it is slow to load and run
Since we only use topological_sort, our implementation is more specialized and hence faster (we don't test for cyclic nature of graph etc.)
"""
DAGs = {}

def topological_sort(dag, node, gen = None):
    """
    The directed acyclic graph is stored as a dict with edges that look like:
        dag[parent] = {child : None for child in (parent, child) set of edges}

    :Parameters:
    ----------
    dag : dict, a directed acyclic graph
        DESCRIPTION.
    node : a key
        a node in the graph acting as the top of the subgraph
    gen : int, optional
        output

    :Returns:
    -------
    a dict from nodes to their generations
    

    :Example: constructing a DAG:
    ---------
    >>> dag = dict(a = dict(b = 0, c = 0), b = dict(c = 0, d = 0, e = 0), c = dict(e = 0), d = dict(e = 0))
    >>> node = 'a'
    >>> assert topological_sort(dag, node) == {'a': 0, 'b': 1, 'c': 2, 'e': 3, 'd': 2}


    :Example: we compare to networkx alternative:
    ---------
    >>> dag = dict(a = dict(b = 0, c = 0), b = dict(c = 0, d = 0, e = 0), c = dict(e = 0), d = dict(e = 0))
    >>> node = 'a'
    >>> assert topological_sort(dag, node) == {'a': 0, 'b': 1, 'c': 2, 'e': 3, 'd': 2}

    >>> import networkx as nx
    >>> G = nx.DiGraph()
    >>> for a in dag:
    >>>     for b in dag[a]:
    >>>         G.add_edge(a,b)

    >>> def nx_topological_sort(G, node):
    >>>    nodes = {node} | nx.descendants(G, node)
    >>>    return list(nx.algorithms.dag.topological_sort(G.subgraph(nodes)))

    >>> def top_sort(dag, node): # we want to match output format of nx.topological_sort
    >>>    gen = topological_sort(dag, node)
    >>>    g, n = zip(*sorted(dict_invert(gen).items()))
    >>>    return sum(n, [])

    >>> x = timer(descendants, n = 10000)(dag, node)
    >>> y = timer(nx_topological_sort, n = 10000)(G, node)
    >>> assert x == y

    Just to compare speeds, our topological sort is approx 10 times faster...

    >>> x = timer(descendants, n = 10000, time = True)(dag, node)
    >>> y = timer(nx_topological_sort, n = 10000, time = True)(G, node)
    >>> assert x * 10 < y    

    """
    if gen is None:
        gen = {}
    if node not in gen:
        gen[node] = 0
    if node in dag:
        g = gen[node]
        children = dag[node]
        for child in children:
            if not (child in gen and gen[child] > g):
                gen[child] = g + 1
            gen = topological_sort(dag, child, gen)
    return gen

def descendants(dag, node):
    """
    returns the descendants of the node (with node itself as first element) sorted topologically

    Parameters
    ----------
    dag : DAG
        A dag.
    node : a key
        A key to a node in the graph.

    Returns
    -------
    list
        list of keys for children of node in the graph.
    """
    gen = topological_sort(dag, node)
    g, n = zip(*sorted(dict_invert(gen).items()))
    return sum(n, [])
    

def get_DAG(dag = None):
    # from networkx import DiGraph
    # return DiGraph()
    if dag not in DAGs:
        DAGs[dag] = {}
    return DAGs[dag]

def add_edge(parent, child, dag = None, value = None):
    if not isinstance(dag, dict):
        dag = get_DAG(dag)
    if parent not in dag:
        dag[parent] = {child : value}
    else:
        dag[parent].update({child : value})
    return dag

