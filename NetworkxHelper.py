import networkx as nx
import operator
import matplotlib.pyplot as plt
import matplotlib
from helper import TextHelper
from nltk import ngrams
import random
from itertools import cycle

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

def add_node(G, node, entity):
    """uu = [n for n in G.nodes(data=True) if n[0] == node]
    if uu and uu[0] == node:
        entity = uu[1]['entity'] or node in entity
        G.add_node(node, entity=entity)
    else:"""
    G.add_node(node, entity=node in entity)

def addEdge( G, u, v, tweet, entity=[]):
    if u == v or u in v or v in u or len(u)<2 or len(v) < 2:
        return

    entity = [TextHelper.lemmatize(e) for e in entity]
    u = TextHelper.lemmatize(u)
    v = TextHelper.lemmatize(v)
    weight = 2 if u in entity or v in entity else 1

    add_node(G,u,entity)
    add_node(G,v,entity)
    #G = nx.DiGraph()


    if G.has_edge(u,v):
        if tweet not in G.get_edge_data(u, v)['id']:
            G.get_edge_data(u, v)['weight'] = G.get_edge_data(u, v)['weight'] + weight
            G.get_edge_data(u, v)['id'].append(tweet)

    elif G.has_edge(v, u):
        if tweet not in G.get_edge_data(v, u)['id']:
            G.get_edge_data(v, u)['weight'] = G.get_edge_data(v, u)['weight'] + weight
            G.get_edge_data(v, u)['id'].append(tweet)
    else:
        G.add_edge(u,v,attr_dict={'weight':weight, 'id':[tweet]})


"""
Merge edges which nodes co-occur in a single node
"""
def mergeNodes(initialGraph):
    nodes = initialGraph.nodes(data=True)
    nodes = [n for n in nodes if len(n[0].split()) > 1]
    for node in nodes:
        deg = initialGraph.degree(node[0])
        ng = ngrams(node[0].split(), 2)
        for n in ng:
            weight,_from,to = None, None, None
            if initialGraph.has_edge(n[0], n[1]):
                weight, _from, to  = initialGraph.get_edge_data(n[0], n[1])['weight'],n[0],n[1]
            elif initialGraph.has_edge(n[1], n[0]):
                weight, _from, to = initialGraph.get_edge_data(n[1], n[0])['weight'], n[1], n[0]

            if weight:
                if weight > deg:
                    pass
                    #print("Higher", node[0], n, weight, deg)
                else:
                    predecessors = initialGraph.predecessors(_from)
                    successors = initialGraph.successors(to)
                    for pred in predecessors:
                        weight = initialGraph.get_edge_data(pred,_from)
                        if initialGraph.has_edge(pred, node[0]):
                            initialGraph.get_edge_data(pred, node[0])['weight'] = initialGraph.get_edge_data(pred, _from)['weight'] + weight['weight']
                            initialGraph.get_edge_data(pred, node[0])['id'].extend(weight['id'])
                        else:
                            initialGraph.add_edge(pred, node[0], attr_dict=weight)
                    for succ in successors:
                        weight = initialGraph.get_edge_data(to, succ)
                        if initialGraph.has_edge(node[0], succ):
                            initialGraph.get_edge_data(node[0], succ)['weight'] = initialGraph.get_edge_data(to, succ)['weight'] + weight['weight']
                            initialGraph.get_edge_data(node[0], succ)['id'].extend(weight['id'])
                        else:
                            initialGraph.add_edge(node[0],to, attr_dict=weight)
            else:
                pass
                #print("UNIQ",node[0], deg)

    nodes = initialGraph.nodes(data=True)


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
            if data < min_weight:
                toRem.append((n,nbr))
    G.remove_edges_from(toRem)
    graphs = [G]
    if G and not nx.is_strongly_connected(G):
        graphs = sorted(nx.strongly_connected_component_subgraphs(G), key = len, reverse = True)

    graphs = [g for g in graphs if nx.number_of_nodes(g) > 1]
    return graphs

def get_components(G):
    components = sorted(nx.strongly_connected_component_subgraphs(G), key = len, reverse = True)
    return components

def merge_nodes(G, nodes, new_node):

    """
    Merges the selected `nodes` of the graph G into one `new_node`,
    meaning that all the edges that pointed to or from one of these
    `nodes` will point to or from the `new_node`.
    attr_dict and **attr are defined as in `G.add_node`.
    """
    #for n in nodes:

    #G.add_node(new_node, entity=True)  # Add the 'merged' node

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
    nodes = G.predecessors(node) if direct ==-1 else G.successors(node)
    #G = nx.DiGraph()
    nodes = [(pred,G.get_edge_data(pred,node)['weight'] if direct == -1 else G.get_edge_data(node,pred)['weight']) for pred in nodes]
    nodes.sort(key=operator.itemgetter(1), reverse=True)
    return nodes
    """
    edges = []
    for p in nodes:
        weight = G.get_edge_data(p, node) if direct==-1 else G.get_edge_data(node, p)
        _nodes = G.predecessors(p) if direct == -1 else G.successors(p)
        for pp in _nodes:
            ed = G.get_edge_data(pp, p) if direct == -1 else G.get_edge_data(p, pp)
            _weight = len(set(weight['id']).intersection(set(ed['id'])))
            if direct == -1:
                edges.append(((pp,ed['weight']),(p,weight['weight']), node,_weight))
            else:
                edges.append((node, (p,weight['weight']), (pp,ed['weight']), _weight))
    edges.sort(key=operator.itemgetter(3),reverse=True)
    return edges[0] if edges else edges"""

"""def highestPred(G, node, direct=-1):
    nodes = G.predecessors(node) if direct ==-1 else G.successors(node)
    edges = []
    for p in nodes:
        weight = G.get_edge_data(p, node) if direct==-1 else G.get_edge_data(node, p)
        _nodes = G.predecessors(p) if direct == -1 else G.successors(p)
        for pp in _nodes:
            ed = G.get_edge_data(pp, p) if direct == -1 else G.get_edge_data(p, pp)
            _weight = weight['weight']
            if direct == -1:
                edges.append(((pp,ed['weight']),(p,weight['weight']), node,_weight))
            else:
                edges.append((node, (p,weight['weight']), (pp,ed['weight']), _weight))
    edges.sort(key=operator.itemgetter(3),reverse=True)
    return edges[0] if edges else edges"""

def topSucc(node, G):
    successors = G.successors(node)
    if len(successors) == 0:
        return None
    successors = [(s, G.get_edge_data(node, s)['weight']) for s in successors]
    successors.sort(key=operator.itemgetter(1), reverse=True)
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

def subgraphs(G):
    graphs = []
    for g in nx.strongly_connected_component_subgraphs(G):
        #remove isolated nodes
        if nx.number_of_nodes(g) < 2:
            continue
        graphs.append(g)
    return graphs

def createLayout(G):
    return graphviz_layout(G, prog="neato")
colors = cycle(['blue', 'green', 'brown', 'black','navy', 'turquoise', 'darkorange', 'cornflowerblue', 'teal'])

def display(G, pos=None):
    if not pos:
        pos = createLayout(G)
    plt.figure(1, figsize=(10, 10))
    if not type(G) is list:
        G = [G]
    # layout graphs with positions using graphviz neato
    for i, c in zip(range(len(G)), colors):
        g = G[i]
        nx.draw(g,
                pos,
                node_size=80,
                node_color=c,
                vmin=1.0,
                vmax=3.0,
                with_labels=True
                )

        labels = nx.get_edge_attributes(g, 'weight')
        nx.draw_networkx_edge_labels(g, pos, edge_labels=labels)

    plt.show()


def degrees(G, nbunch=None):
    degree = G.degree(weight='weight', nbunch=nbunch)
    degree = [d for d in degree.items()]
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

def getEntityNodes(nodes,elem):
    res = []
    p = []
    for i in range(len(elem['center'])):
        p.extend(elem['pred'][i] + [elem['center'][i]] + elem['succ'][i])
    for el in p:
        if el[0] in nodes:
            res.append(el[0])
    return res

"""
Appy graph cutting to split the event grah into subgraphs
Parameters
----------
G: A directed graph

Returns
-------
A list of subgraph

"""
def graph_cutting(G, iteration=5):
    parts = [G]
    for i in range(iteration):
        sub = []
        for g in parts:
            nodes = nx.minimum_node_cut(g)
            for n in nodes:
                print(n)
                g.remove_node(n)
            sb = subgraphs(g)
            sub.extend(sb)
        parts = sub
    return  parts

def graph_pruning(dGraph, seen, node, day, nodes):
    res = []
    predecessors = highestPred(dGraph, node[0])
    successors = highestPred(dGraph, node[0], direct=1)

    if not predecessors and not successors:
        return
    val = {'center': node, 'pound':node[1], 'tweets': []}
    if predecessors:
        pred = [t for t in predecessors[0:2]]
        val['pred'] = pred
        val['pound'] += sum([node[1] for t in pred])
    else:
        return
        # val['pred'] = []
    # val.append((*t,'center'))
    if successors:
        suc = [t for t in successors[1:3]]
        val['succ'] = suc
        val['pound'] += sum([t[1] for t in suc])
    else:
        # val['succ'] = []
        return

    for d in ngrams([e[0] for e in val['pred']] + [val['center'][0]] + [e[0] for e in val['succ']],
                    2):
        dd = dGraph.get_edge_data(d[0], d[1])
        val['tweets'].extend(dd['id'])
    val['succ'] = [val['succ']]
    val['pred'] = [val['pred']]
    val['center'] = [val['center']]
    res.append(val)

    # removed candidates that have a node corresponding to the center of an event
    for i, elem in enumerate(res):
        if 'exist' in elem or 'ignore' in elem:
            continue
        entities1 = getEntityNodes(nodes, elem)
        for s in seen:
            if s['center'][0][0] == elem['center'][0][0]:
                elem['exist'] = True
                elem['ignore'] = True
                elem['event'] = s['event']
                break

        if 'exist' in elem or 'ignore' in elem:
            continue

        for j in range(i + 1, len(res)):
            elem2 = res[j]

            entities2 = getEntityNodes(nodes, elem2)

            equal, edge = 0, 0
            for ent in entities1:

                for ent2 in entities2:
                    if ent == ent2:
                        equal += 1
                    if dGraph.has_edge(ent, ent2) or dGraph.has_edge(ent2, ent):
                        edge += 1
                        if edge >= 2:
                            break
                    if edge >= 2:
                        break
                if edge >= 1 and equal >= 1 or (edge == 2 or equal == 2):
                    elem2['ignore'] = True
                    elem['pred'].append(elem2['pred'])
                    elem['succ'].append(elem2['succ'])
                    elem['center'].append(elem2['center'])
                    if 'exist' in elem2:
                        elem['exist'] = True
                        elem['event'] = elem2['event']
                    break

    for i, r in enumerate(res):
        r['day'] = day
        r['tweets'] = list(set(r['tweets']))

    for i, r in enumerate(res):
        for j in range(i + 1, len(res)):
            k = res[j]
            if 'ignore' in k:
                continue
            intersect = set(r['tweets']).intersection(set(k['tweets']))
            if len(intersect) > 3:
                r['tweets'].extend(k['tweets'])
                k['ignore'] = True
    res = [elem for elem in res if 'ignore' not in elem or len(elem['center']) > 20 or len(elem['center']) < 3]
    return res