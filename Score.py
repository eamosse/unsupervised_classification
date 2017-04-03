import networkx as nx
from NetworkxHelper import *
from helper.TextHelper import *
from EventDefinition import *
from networkx.exception import NetworkXError

"""
Taken from nx.pagerank
"""
def pagerank(G, alpha=0.85, personalization=None,
             max_iter=100, tol=1.0e-6, nstart=None, weight='weight',
             dangling=None):

    if len(G) == 0:
        return {}

    if not G.is_directed():
        D = G.to_directed()
    else:
        D = G

    # Create a copy in (right) stochastic form
    W = nx.stochastic_graph(D, weight=weight)
    N = W.number_of_nodes()

    # Choose fixed starting vector if not given
    if nstart is None:
        x = dict.fromkeys(W, 1.0 / N)
    else:
        # Normalized nstart vector
        s = float(sum(nstart.values()))
        x = dict((k, v / s) for k, v in nstart.items())

    if personalization is None:
        # Assign uniform personalization vector if not given
        p = dict.fromkeys(W, 1.0 / N)
    else:
        missing = set(G) - set(personalization)
        if missing:
            for m in missing:
                personalization[m] = 1.0 / N
            """raise NetworkXError('Personalization dictionary '
                                'must have a value for every node. '
                                'Missing nodes %s' % missing)"""
        s = float(sum(personalization.values()))
        p = dict((k, v / s) for k, v in personalization.items())

    if dangling is None:
        # Use personalization vector if dangling vector not specified
        dangling_weights = p
    else:
        missing = set(G) - set(dangling)
        if missing:
            raise NetworkXError('Dangling node dictionary '
                                'must have a value for every node. '
                                'Missing nodes %s' % missing)
        s = float(sum(dangling.values()))
        dangling_weights = dict((k, v/s) for k, v in dangling.items())
    dangling_nodes = [n for n in W if W.out_degree(n, weight=weight) == 0.0]

    # power iteration: make up to max_iter iterations
    for _ in range(max_iter):
        xlast = x
        x = dict.fromkeys(xlast.keys(), 0)
        danglesum = alpha * sum(xlast[n] for n in dangling_nodes)
        for n in x:
            # this matrix multiply looks odd because it is
            # doing a left multiply x^T=xlast^T*W
            for nbr in W[n]:
                x[nbr] += alpha * xlast[n] * W[n][nbr][weight]
            x[n] += (danglesum * dangling_weights[n] + (1.0 - alpha)) * p[n]
        # check convergence, l1 norm
        err = sum([abs(x[n] - xlast[n]) for n in x])
        if err < N*tol:
            return x
    raise NetworkXError('pagerank: power iteration failed to converge '
                        'in %d iterations.' % max_iter)

def sumEdges(G, node, direct=1):
    nodes = G.predecessors(node) if direct ==-1 else G.successors(node)
    tot = sum([G.get_edge_data(n, node)['weight'] if direct == -1 else G.get_edge_data(node, n)['weight'] for n in nodes])
    return tot

previous = []
def getScore(G, degree, dangling=True):
    X = None
    N = G.number_of_nodes()
    if dangling:

        previous.append('=>'.join(['{}=>'.format(t[0]) * t[1] for t in degree]))
        X = buildTfIdf(previous[-48:]) if dangling else None
    #print(X)
    calculated_page_rank = pagerank(G,personalization=X)
    nodes = {key: N*calculated_page_rank[key] for key in calculated_page_rank.keys()}
    nodes = sorted(nodes.items(), key=operator.itemgetter(1), reverse=True)
    print(nodes)
    return nodes

def mGraph(tweets):
    # create a fake graph
    G = nx.DiGraph()
    for t in tweets:
        ann = AnnotationHelper.format(t)
        for a in ann:
            for l in a['edges']:
                if len(l[0].split()) < 3 and len(l[1].split()) < 3:
                    addEdge(G, l[0], l[1], t['id'], l[2])
                else:  # nodes that have more than two tokens are most likely wrong NE identified by the NER tool(e.g. sandusky sentenced jail)
                    # thus, we split both initial nodes to create new edges
                    parts = ngrams(l[0].split() + l[1].split(), 2)
                    for p in parts:
                        addEdge(G, p[0], p[1], t['id'])
    return G


if __name__ == '__main__':
    # get some examples tweets
    dangling = True
    db.connect("tweets_dataset")
    dirty = dirtyTweets(10000, shuffle=0)
    tweets = db.find(collection="annotation_unsupervised", query={'event_id': 383}) + dirty
    G = mGraph(tweets)
    mergeNodes(G)
    gs = clean(G, min_weight=3)
    scores1 = getScore(G, dangling=dangling)

    dirty = dirtyTweets(10000,shuffle=0, min=10000)
    tweets = db.find(collection="annotation_unsupervised", query={'event_id': 382}) + dirty
    G = mGraph(tweets)
    mergeNodes(G)
    clean(G, min_weight=3)
    scores2 = getScore(G , dangling=dangling)

    dirty = dirtyTweets(10000, min=20000, shuffle=0)
    tweets = db.find(collection="annotation_unsupervised", query={'event_id': 381}) + dirty
    G = mGraph(tweets)
    mergeNodes(G)
    clean(G, min_weight=3)
    scores3 = getScore(G , dangling=dangling)

    for t in scores1:
        for tt in scores2:
            if t[0] != tt[0]:
                continue
            for ttt in scores3:
                if t[0] == ttt[0]:
                    print(t,tt,ttt)



