import networkx as nx
import operator
import matplotlib.pyplot as plt
import matplotlib
from helper import TextHelper
from nltk import ngrams
import random
try:
    import pygraphviz
    from networkx.drawing.nx_agraph import graphviz_layout
except ImportError:
    try:
        import pydotplus
        from networkx.drawing.nx_pydot import graphviz_layout
    except ImportError:
        raise ImportError("This example needs Graphviz and either "
                          "PyGraphviz or PyDotPlus")

def longest_path(G):
    return nx.dag_longest_path(G)

def hasNode(G, nn):
    nodes = G.nodes()
    vals = {i:nodes[i] for i in nx.convert_node_labels_to_integers(G)}
    for i, n in enumerate(nodes):
        if len(n.split()) == len(nn.split()):
            continue

        if n == nn:
            nn = n
            #return G,nn
        elif nn in n :
            nn = n
            #print(nn , "in==>", n)
            #return G,n
        elif n in nn:
            #print(n, "in<==", nn)
            vals[i] = nn
            G = nx.relabel_nodes(G, vals)  # nodes 1..26
            #return G, nn
    return G, nn

def addEdge( G, u, v, tweet, entity=-1):
    if u == v or u in v or v in u or len(u)<2 or len(v) < 2:
        return
    u = TextHelper.lemmatize(u)
    v = TextHelper.lemmatize(v)
    #G = nx.DiGraph()

    if G.has_edge(u,v):
        if tweet not in G.get_edge_data(u, v)['id']:
            G.get_edge_data(u, v)['weight'] = G.get_edge_data(u, v)['weight'] + 1
            G.get_edge_data(u, v)['id'].append(tweet)

    elif G.has_edge(v, u):
        if tweet not in G.get_edge_data(v, u)['id']:
            G.get_edge_data(v, u)['weight'] = G.get_edge_data(v, u)['weight'] + 1
            G.get_edge_data(v, u)['id'].append(tweet)

    else:
        G.add_node(u, entity=entity==0 or entity==2)
        G.add_node(v, entity=entity==1 or entity ==2)
        G.add_edge(u,v,attr_dict={'weight':1, 'id':[tweet]})


"""
Merge edges which nodes co-occur in a single node
"""
def mergeNodes(initialGraph):
    nodes = initialGraph.nodes(data=True)
    nodes = [n for n in nodes if len(n[0].split()) > 1]
    for node in nodes:
        ng = ngrams(node[0].split(), 2)
        for n in ng:
            if initialGraph.has_edge(n[0], n[1]) or initialGraph.has_edge(n[1], n[0]):
                merge_nodes(initialGraph, n, node[0])
    #rename similar nodes, similarity is computed with Levinstein distance
    """nodes = initialGraph.nodes(data=True)
    for i,node1 in enumerate(nodes):
        toRename = set()
        for j in range(i+1,len(nodes)):
            node2 = nodes[j]
            if TextHelper.similarity(node1[0], node2[0])==1:
                toRename.add(node2[0])
        if toRename:
            print("To rename", node1[0], toRename)
            merge_nodes(initialGraph,toRename,node1[0])"""


def merge(vals):
    vals.sort(key=lambda tup: len(tup[0]), reverse=True)
    for i, f in enumerate(vals):
        for j in range(len(vals)):
            if i == j :
                continue

            if vals[j][0] in f[0]:
                vals[i] = (vals[i][0], vals[i][1]+vals[j][1])
    vals.sort(key=operator.itemgetter(1), reverse=True)

def clean(G, min_weight=2):
    toRem= []
    for n, nbrs in G.adjacency_iter():
        for nbr, eattr in nbrs.items():
            data = eattr['weight']
            if data < 2:
                toRem.append((n,nbr))

    G.remove_edges_from(toRem)
    if G and not nx.is_strongly_connected(G):
        G = sorted(nx.strongly_connected_component_subgraphs(G), key = len, reverse = True)

    return G

def merge_nodes(G, nodes, new_node):
    """
    Merges the selected `nodes` of the graph G into one `new_node`,
    meaning that all the edges that pointed to or from one of these
    `nodes` will point to or from the `new_node`.
    attr_dict and **attr are defined as in `G.add_node`.
    """

    G.add_node(new_node, entity=True)  # Add the 'merged' node

    for n1, n2, data in G.edges(data=True):
        # For all edges related to one of the nodes to merge,
        # make an edge going to or coming from the `new gene`.
        if n1 in nodes:
            G.add_edge(new_node, n2, data)
        elif n2 in nodes:
            G.add_edge(n1, new_node, data)

    for n in nodes:  # remove the merged nodes
        G.remove_node(n)

def topPred(node, G):
    predecessors = G.predecessors(node)
    if len(predecessors) == 0:
        return None
    predecessors = [(s, G.get_edge_data(s, node)['weight']) for s in predecessors]
    predecessors.sort(key=operator.itemgetter(1), reverse=True)
    #predecessors.sort(reverse=True)
    #merge(predecessors)
    return predecessors[0]



def highestPred(G, node, direct=-1):
    #G = nx.DiGraph()
    nodes = G.predecessors(node) if direct ==-1 else G.successors(node)
    edges = []
    for p in nodes:
        weight = G.get_edge_data(p, node) if direct==-1 else G.get_edge_data(node, p)
        #ed = [(node,p,edge['weight'])]
        _nodes = G.predecessors(p) if direct == -1 else G.successors(p)
        for pp in _nodes:
            ed = G.get_edge_data(pp, p) if direct == -1 else G.get_edge_data(p, pp)
            _weight = len(set(weight['id']).intersection(set(ed['id'])))
            if direct == -1:
                edges.append(((pp,ed['weight']),(p,weight['weight']), node,_weight))
            else:
                edges.append((node, (p,weight['weight']), (pp,ed['weight']), _weight))
    edges.sort(key=operator.itemgetter(3),reverse=True)
    return edges[0] if edges else edges



def topSucc(node, G):
    successors = G.successors(node)
    if len(successors) == 0:
        return None
    successors = [(s, G.get_edge_data(node, s)['weight']) for s in successors]
    successors.sort(key=operator.itemgetter(1), reverse=True)
    #successors.sort(reverse=True)
    #merge(successors)
    return successors[0]

def hierar(G,t, func, limit=2):
    predecessors = [t]
    current = t
    l = 0
    path = []
    while(l < limit):
        l+=1
        pred = func(current[0], G)
        if not pred or pred[0] + ' ' + current[0] in path:
            #if not pred or pred[0] in [p[0] for p in predecessors]:
            break
        if not TextHelper.isStopWord(pred[0]) or not TextHelper.isStopWord(current[0]) :
            path.append(pred[0] + ' ' + current[0])
        predecessors.append(pred)
        current = pred
    predecessors.remove(predecessors[0])
    return predecessors


def display(G, name='test'):

    """matplotlib.pyplot.figure(
        figsize=(25.0, 25.0))  # The size of the figure is specified as (width, height) in inches
    edge_labels = dict([((u, v,), d['weight'])
                                        for u, v, d in G.edges(data=True)])
    pos = nx.spring_layout(G)
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
    nx.draw_networkx(G, pos)
    #nx.draw_networkx(G,pos=nx.spring_layout(G))#
    #break
    plt.savefig(name+".png")
    plt.show()"""
    plt.figure(1, figsize=(20, 20))
    # layout graphs with positions using graphviz neato
    pos = graphviz_layout(G, prog="neato")
    c = [random.random()] * nx.number_of_nodes(G)  # random color...
    nx.draw(G,
            pos,
            node_size=80,
            node_color=c,
            vmin=1.0,
            vmax=3.0,
            with_labels=True
            )

    plt.show()


def degrees(G, nbunch=None):
    degree = G.degree(weight='weight', nbunch=nbunch)
    degree = [d for d in degree.items() if d[1] > 2]
    degree = [de for de in degree if not TextHelper.isStopWord(de[0])]
    degree.sort(key=operator.itemgetter(1), reverse=True)
    return degree

def removeEdgeWithEight(G,eight=2):
    toRems = []
    #G = nx.DiGraph()
    for n, nbrs in G.adjacency_iter():
        for nbr, eattr in nbrs.items():
            data = eattr['weight']
            if data <=eight:
                toRems.append((n,nbr))
                #print('(%s, %s, %s)' % (n, nbr, data))
    for edge in toRems:
        G.remove_edge(edge[0],edge[1])
    return clean(G)