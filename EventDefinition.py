from helper import MongoHelper as db, AnnotationHelper
from NetworkxHelper import *

db.connect("tweets_dataset")

def generateDefinition(ids):

    tweets = db.find(collection="annotation_unsupervised", query={'id':{'$in':ids}})
    initialGraph = nx.DiGraph()
    for tweet in tweets:
        edges = AnnotationHelper.getNodes(text=tweet['text'])
        for e in edges:
            addEdge(initialGraph,e[0],e[1],tweet['id'])
    G = initialGraph#removeEdgeWithEight(initialGraph,eight=0)[0]
    #display(G)
    degree = degrees(G)
    if len(degree) > 0:
        d = degree[0]
        pred = hierar(G,d,topPred,limit=15)
        pred.reverse()
        succ = hierar(G,d,topSucc,limit=15)
        p = [t[0] for t in pred]
        p.append("-"+d[0]+"-")
        p.extend([t[0] for t in succ])
        return ' '.join(p)
    else:
        return tweets[0]['text']
        #display(G)

if __name__ == '__main__':
    ids = ['255888992740995073', '255835792092561409', '255920223423696896', '255880431927443456', '255849486423842817']
    generateDefinition(ids)

