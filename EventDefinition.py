from helper import MongoHelper as db, AnnotationHelper
from NetworkxHelper import *
import operator
import random

from networkx.algorithms.flow import shortest_augmenting_path
db.connect("tweets_dataset")


def topParts(G, node,ids=None,orient=-1):
    # 1 get the degree with the highest score
    # 2 get the the predecessors with the highes weight
    if orient==-1:
        pred = G.predecessors(node)
    else:
        pred = G.successors(node)

    predecessors = []
    for s in pred:
        if orient == -1:
            edge = G.get_edge_data(s,node)
        else:
            edge = G.get_edge_data(node,s)
        predecessors.append((s,edge['weight'],edge['id']))
    #pred = [(s, G.get_edge_data(s, deg[0])['weight'], G.get_edge_data(s, deg[0])['id']) for s in pred]
    if ids:
        predecessors = [p for p in predecessors if set(p[2]).intersection(set(ids))]
        predecessors.sort(key=operator.itemgetter(1), reverse=True)
    return predecessors


def top(G,node,ids,orient=-1):
    res = []
    visited = []
    while True:
        p = topParts(G, node, ids=ids, orient=orient)
        if not p:
            break
        if visited:
            if (node,p[0][0]) in visited:
                visited.append((node,p[0][0]))
                break
        visited.append((node,p[0][0]))
        node = p[0][0]
        if not ids:
            ids = p[0][2]
        res.append(p[0][0])
    if orient==-1 :
        res.reverse()
    return res,ids

def dirtyTweets(nb):
    non_events = db.find("non_event")
    non_events = [event for event in non_events if not event['text'].startswith('rt')]
    random.shuffle(non_events)
    random.shuffle(non_events)
    random.shuffle(non_events)
    return non_events[0:nb]

def generateDefinition(ids):
    tweets = db.find(collection="all_tweets", query={'id':{'$in':ids}})
    random.shuffle(tweets)
    tweets = tweets[0:15]
    G = nx.DiGraph()
    for tweet in tweets:
        edges = AnnotationHelper.getNodes(text=tweet['text'])
        for e in edges:
            addEdge(G,e[0],e[1],tweet['id'])
    deg = degrees(G)
    ids = None
    if deg:
        node = deg[0][0]
        pred, ids = top(G,node,ids,orient=-1)
        succ,_ = top(G,node,ids,orient=1)
        return '{} {} {}'.format(' '.join(pred), node, ' '.join(succ))
    else:
        return tweets[0]['text']

def flow_func(G,node1,node2):
    return G.get_edge_data(node1, node2)['weight']

if __name__ == '__main__':
    tweets = db.find(collection="annotation_unsupervised", query={'event_id':383}) + dirtyTweets(10000)
    texts = ' ' .join(t['text'] for t in tweets)
    from summa.summarizer import summarize
    summary = summarize(texts)
    print(summary)

    """G = nx.DiGraph()
    for tweet in tweets:
        ann = AnnotationHelper.format(tweet)
        for a in ann:
            for l in a['edges']:
                addEdge(G, l[0], l[1], tweet['id'], l[2])
    #deg = degrees(G)
    pos = createLayout(G)
    sub = []
    G = clean(G)
    display(G, pos=pos)"""
    """pos = createLayout(G)
    nx.draw(G, pos)
    labels = nx.get_edge_attributes(G, 'weight')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=labels)
    #edge_labels = nx.draw_networkx_edge_labels(G, pos=nx.spring_layout(G))
    plt.show()"""

    """for d in deg:
        pred = highestPred(G, d[0])
        succ = highestPred(G, d[0],direct=1)
        print(pred, succ)


    for i in range(5):
        G = G if not sub else sub
        sub = []
        for g in G:
            nodes = nx.minimum_node_cut(g)
            for n in nodes:
                print(n)
                g.remove_node(n)
            sb = subgraphs(g)
            print(sb)
            sub.extend(sb)
        G = sub
    display(G,pos=pos)"""