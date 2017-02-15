from helper import MongoHelper as db, AnnotationHelper
from NetworkxHelper import *
import operator

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


def generateDefinition(ids):
    print(ids)

    tweets = db.find(collection="all_tweets", query={'id':{'$in':ids}})
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


if __name__ == '__main__':
    ids = ['255917019113873409', '255914464761749504', '255902057825976320', '255917010582650882', '255914464585588737', '255917681709035520', '255917396328603648', '255914464661078017', '255917820326588417', '255917715464785920', '255917631478054912', '255916994069676033', '255908026626752512', '255898102530207744', '255916993721556993', '255917300090302464', '255914464854032385', '255914464598167552', '255914464895963136', '255914464908554240', '255917790844833792', '255917010582650882', '255917396328603648', '255917820326588417', '255917631478054912', '255917715464785920', '255917300090302464', '255916994069676033', '255917790844833792']
    tweets = db.find(collection="all_tweets", limit=200)
    G = nx.DiGraph()
    for tweet in tweets:
        ann = AnnotationHelper.format(tweet)
        for a in ann:
            for l in a['edges']:
                addEdge(G, l[0], l[1], tweet['id'], l[2])
    deg = degrees(G)

    for d in deg:
        pred = highestPred(G, d[0])
        succ = highestPred(G, d[0],direct=1)
        print(pred, succ)
    #display(G)
