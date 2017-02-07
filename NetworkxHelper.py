import networkx as nx
import operator
import matplotlib.pyplot as plt
import matplotlib

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

def addEdge( G, u, v, tweet):
    if u == v or u in v or v in u or len(u)<2 or len(v) < 2:
        return

    if G.has_edge(u,v):
        G.get_edge_data(u,v)['weight'] =  G.get_edge_data(u,v)['weight'] + 1
        if tweet not in G.get_edge_data(u, v)['id']:
            G.get_edge_data(u, v)['id'].append(tweet)
    elif G.has_edge(v,u):
        G.get_edge_data(v, u)['weight'] = G.get_edge_data(v, u)['weight'] + 1
        if tweet not in G.get_edge_data(v, u)['id']:
            G.get_edge_data(v, u)['id'].append(tweet)
    else:
        G.add_edge(u,v,attr_dict={'weight':1, 'id':[tweet]})



def merge(vals):
    vals.sort(key=lambda tup: len(tup[0]), reverse=True)
    for i, f in enumerate(vals):
        for j in range(len(vals)):
            if i == j :
                continue

            if vals[j][0] in f[0]:
                vals[i] = (vals[i][0], vals[i][1]+vals[j][1])
    vals.sort(key=operator.itemgetter(1), reverse=True)


def merge_nodes(G, nodes, new_node, attr_dict=None, **attr):
    """
    Merges the selected `nodes` of the graph G into one `new_node`,
    meaning that all the edges that pointed to or from one of these
    `nodes` will point to or from the `new_node`.
    attr_dict and **attr are defined as in `G.add_node`.
    """
    print(new_node, nodes)

    G.add_node(new_node, attr_dict, **attr)  # Add the 'merged' node

    for n1, n2, data in G.edges(data=True):
        # For all edges related to one of the nodes to merge,
        # make an edge going to or coming from the `new gene`.
        if n1 in nodes:
            G.add_edge(new_node, n2, data)
        elif n2 in nodes:
            G.add_edge(n1, new_node, data)

    for n in nodes:  # remove the merged nodes
        G.remove_node(n)

def clean(G):

    if G and not nx.is_strongly_connected(G):
        G = sorted(nx.strongly_connected_component_subgraphs(G), key = len, reverse = True)

    return G

def topPred(node, G):
    predecessors = G.predecessors(node)
    if len(predecessors) == 0:
        return None
    predecessors = [(s, G.get_edge_data(s, node)['weight']) for s in predecessors]
    predecessors.sort(reverse=True)
    #merge(predecessors)
    return predecessors[0]

def topSucc(node, G):
    successors = G.successors(node)
    if len(successors) == 0:
        return None
    successors = [(s, G.get_edge_data(node, s)['weight']) for s in successors]
    successors.sort(reverse=True)
    #merge(successors)
    return successors[0]

def hierar(G,t, func, limit=2):
    predecessors = [t]
    current = t
    l = 0
    while(l < limit):
        l+=1
        pred = func(current[0], G)
        if not pred or pred[0] in [p[0] for p in predecessors]:
            break
        predecessors.append(pred)
        current = pred
    predecessors.remove(predecessors[0])
    return predecessors


def display(G):
    matplotlib.pyplot.figure(
        figsize=(50.0, 50.0))  # The size of the figure is specified as (width, height) in inches
    edge_labels = dict([((u, v,), d['weight'])
                                        for u, v, d in G.edges(data=True)])
    pos = nx.spring_layout(G)
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
    nx.draw_networkx(G, pos)
    #nx.draw_networkx(G,pos=nx.spring_layout(G))#
    #break
    plt.savefig("path.png")
    plt.show()

def degrees(G):
    degree = G.degree(weight='weight')
    degree = [d for d in degree.items() if d[1] > 2]
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