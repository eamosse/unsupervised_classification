from audioop import reverse

import networkx as nx
import matplotlib.pyplot as plt
from helper import MongoHelper as db, TextHelper, AnnotationHelper
import operator
from nltk import ngrams
import copy
import helper
collection = "annotation_unsupervised"
log = helper.enableLog()

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
    G, u = hasNode(G,u)
    G, v = hasNode(G,v)
    #print(nodes)
    if u == v or u in v or v in u:
        return
    if G.has_edge(u=u,v=v):
        G.get_edge_data(u,v)['weight'] =  G.get_edge_data(u,v)['weight'] + 1
        if tweet not in G.get_edge_data(u, v)['id']:
            G.get_edge_data(u, v)['id'].append(tweet)
    elif G.has_edge(v,u):
        G.get_edge_data(v, u)['weight'] = G.get_edge_data(v, u)['weight'] + 1
        if tweet not in G.get_edge_data(v, u)['id']:
            G.get_edge_data(v, u)['id'].append(tweet)
    else:
        G.add_edge(u,v,attr_dict={'weight':1, 'id':[tweet]})

    #updateNode(G,u)
db.connect("tweets_dataset")

def update():
    data = []
    added = []
    tweets = db.find("annotation_purge")
    for t in tweets:
        if t['id'] not in added:
            added.append(t['id'])
            val = db.findOneByKey("tweet", 'tweet_id', t['id'])
            if val:
                if 'date' in val :
                    t['date'] = val['date']
                else:
                    print(t['id'], 'has no date')
                t['dataset'] = val['dataset']
                db.insert("annotation_updated", t)

query = [ { "$group" : { "_id" : { "hour" : { "$hour" : "date"} , "minute" : { "$minute" : "date"}} ,
                                                                   "count" : { "$sum" : 1} , "data" : { "$addToSet" : "$_id"}}} , { "$match" : { "$and" : [ { "_id.hour" : 13} , { "_id.minute" : { "$gte" : 0 , "$lt" : 2}}]}} ,
                                                    { "$sort" : { "_id.hour" : 1 , "_id.minute" : 1}}]
def longest_path(G):
    return nx.dag_longest_path(G)


def merge(vals):
    vals.sort(key=lambda tup: len(tup[0]), reverse=True)
    for i, f in enumerate(vals):
        for j in range(len(vals)):
            if i == j :
                continue

            if vals[j][0] in f[0]:
                vals[i] = (vals[i][0], vals[i][1]+vals[j][1])
    vals.sort(key=operator.itemgetter(1), reverse=True)

def clean(G):
    rems = []
    for n, nbrs in G.adjacency_iter():
        for nbr, eattr in nbrs.items():
            data = eattr['weight']
            if data <= 1:
                rems.append((n,nbr))

    G.remove_edges_from(rems)

    if G and not nx.is_strongly_connected(G):
        G = sorted(nx.strongly_connected_component_subgraphs(G), key = len, reverse = True)

    return G

visited = set()

def topPred(node, G):
    predecessors = G.predecessors(node)
    if len(predecessors) == 0:
        return None
    predecessors = [(s, G.get_edge_data(s, node)['weight']) for s in predecessors]
    merge(predecessors)
    return predecessors[0]

def topSucc(node, G):
    successors = G.successors(node)
    if len(successors) == 0:
        return None
    successors = [(s, G.get_edge_data(node, s)['weight']) for s in successors]
    merge(successors)
    return successors[0]

def hierar(G,t, func):
    predecessors = []

    pred1 = func(t[0], G)
    if not pred1:
        return  None
    predecessors.append(pred1)
    pred2 = func(pred1[0], G)
    if pred2 and pred2[0] != t[0]:
        predecessors.append(pred2)
        pred3 = func(pred2[0], G)
        if pred3 and pred3[0] != t[0] and pred2[0] != pred3[0]:
            predecessors.append(pred3)
    return predecessors
def mSum(arr):
    return sum([t[1] for t in arr])
seen = []

def process():
    days = db.intervales(collection)
    initialGraph = nx.DiGraph()
    for day in days:
        tweets = db.aggregateDate(collection, day)#[0]['data']
        if len(tweets) > 0:
            tweets = tweets[0]
        data = tweets['data']

        log.debug("###########################################")
        log.debug("Processing event on day {}".format(tweets['_id']['day']))
        log.debug("Tweets to process {}".format(len(data)))
        log.debug("###########################################")


        initialGraph.clear()
        log.debug("Building the graph")
        for t in data:
            ann = AnnotationHelper.format(t)
            for a in ann:
                for l in a['edges']:
                    addEdge(initialGraph, l[0], l[1], t['id'])


        log.debug("Retrievving subgraphs")
        GG = clean(initialGraph)


        log.debug("Processing subgraphs")
        for G in GG:
            res = []
            degree = G.degree(weight='weight')
            #G.remove_nodes_from([d[0] for d in degree.items() if d[1] <= 2])
            degree = [d for d in degree.items() if d[1] > 2]
            degree.sort(key=operator.itemgetter(1),reverse=True)
            #degree = sorted(degree.items(), key=operator.itemgetter(1),reverse=True)
            if len(degree) == 0:
                continue

            for t in degree:
                predecessors = hierar(G,t,topPred)
                predecessors.reverse()
                successors = hierar(G,t,topSucc)

                if not predecessors and not successors:
                    continue
                val = {'center' : t, 'pound' : t[1], 'tweets' : []}
                if predecessors:
                    pred = [t for t in predecessors[0:3]]
                    val['pred'] = pred
                    val['pound'] += sum([t[1] for t in pred])
                #val.append((*t,'center'))
                if successors:
                    suc = [t for t in successors[0:3]]
                    val['succ'] = suc
                    val['pound'] += sum([t[1] for t in suc])

                for d in ngrams([e[0] for e in val['pred']] + [val['center'][0]] + [e[0] for e in val['succ']],
                                2):
                    dd = initialGraph.get_edge_data(d[0], d[1])
                    val['tweets'].extend(dd['id'])
                res.append(val)

            #removed candidates that have a node corresponding to the center of an event
            log.debug("Pruning subgraphs")
            for i,elem in enumerate(res):
                for s in seen:
                    if elem['center'][0] in s['center'][0]:
                        elem['ignore'] = True
                        #s['tweets'].extend(elem['tweets'])

                for j,elem2 in enumerate(res):
                    if i == j or 'ignore' in elem:
                        continue

                    if elem['center'][0] in [t[0] for t in elem2['pred']+elem2['succ']]:
                        elem2['ignore'] = True
                        elem['tweets'].extend(elem2['tweets'])
                        continue

                    for t in elem['succ'] + elem['pred']:
                        if t[0] in [k[0] for k in elem2['succ'] + elem2['pred']]:
                            if elem['pound'] >= elem2['pound']:
                                elem2['ignore'] = True
                                elem['tweets'].extend(elem2['tweets'])
                            else:
                                elem['ignore'] = True
                                elem2['tweets'].extend(elem['tweets'])

            res = [elem for elem in res if 'ignore' not in elem]
            log.debug("Pruning detected events")
            for r in res:
                f = {}
                for t in r['tweets']:
                    if t in f :
                        f[t] += 1
                    else:
                        f[t] = 1
                r['tweets'] = f

            for i, r in enumerate(res):
                for j,k in enumerate(res):
                    tweets = copy.deepcopy(r['tweets'])
                    if i == j :
                        continue
                    for t in tweets.keys():
                        if t in k['tweets']:
                            if tweets[t] > k['tweets'][t]:
                                del k['tweets'][t]
                            else:
                                del r['tweets'][t]

                                #r['tweets'] = list(set(r['tweets']))
            seen.extend(res)

            log.debug("==========EVENT==========")
            for r in res:
                print (r)

            #print("LP",longest_path(G))
            """edge_labels = dict([((u, v,), d['weight'])
                                for u, v, d in G.edges(data=True)])
            pos = nx.spring_layout(G)
            nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
            nx.draw_networkx(G, pos)
            #nx.draw_networkx(G,pos=nx.spring_layout(G))#
            #break
        #plt.show()"""

if __name__ == '__main__':
    #res = db.intervales("annotation_unsupervised")
    #print(res)
    process()