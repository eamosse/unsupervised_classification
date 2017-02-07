from audioop import reverse

import networkx as nx
import matplotlib.pyplot as plt
from helper import MongoHelper as db, TextHelper, AnnotationHelper
import operator
from nltk import ngrams
import copy
import csv
import helper
from tabulate import tabulate
from NetworkxHelper import  *
from EventDefinition import *
collection = "annotation_unsupervised"
log = helper.enableLog()
helper.disableLog(log)



ignored = []
added = []

    #updateNode(G,u)
db.connect("tweets_dataset")


visited = set()




def mSum(arr):
    return sum([t[1] for t in arr])

seen = []
final = []


def process():
    total = 0
    days = db.intervales(collection)
    initialGraph = nx.DiGraph()


    myfile=open('events.csv', 'w')
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(["Day", "Keywords", "#tweets", "Type"])

    for day in days:
        tweets = db.aggregateDate(collection, day)#[0]['data']
        if len(tweets) > 0:
            tweets = tweets[0]
        data = tweets['data']
        total+= len(data)

        log.debug("###########################################")
        log.debug("Processing event on day {}".format(tweets['_id']['day']))
        log.debug("Tweets to process {}".format(len(data)))
        log.debug("###########################################")

        initialGraph.clear()
        log.debug("Building the graph")
        for t in data:
            ann = AnnotationHelper.format(t)
            if len(ann) == 0:
                ignored.append(t['id'])
            for a in ann:
                for l in a['edges']:
                    addEdge(initialGraph, l[0], l[1], t['id'])

        log.debug("Retrievving subgraphs")
        GG = clean(initialGraph)
        final = []

        log.debug("Processing subgraphs")
        for G in GG:
            res = []
            degree = degrees(G)

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
                    pred = [t for t in predecessors[0:2]]
                    val['pred'] = pred
                    val['pound'] += sum([t[1] for t in pred])
                #val.append((*t,'center'))
                if successors:
                    suc = [t for t in successors[0:2]]
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
                        elem['exist'] = True
                        #s['tweets'].extend(elem['tweets'])

                for j,elem2 in enumerate(res):
                    if i == j or 'ignore' in elem:
                        continue

                    if elem['center'][0] in [t[0] for t in elem2['pred']+elem2['succ']]:
                        elem2['ignore'] = True
                        if 'exist' in elem2:
                            elem['exist'] = True

                        elem['tweets'].extend(elem2['tweets'])
                        continue

                    for t in elem['succ'] + elem['pred']:
                        if t[0] in [k[0] for k in elem2['succ'] + elem2['pred']]:
                            if elem['pound'] >= elem2['pound']:
                                elem2['ignore'] = True
                                elem['tweets'].extend(elem2['tweets'])
                                if 'exist' in elem2:
                                    elem['exist'] = True
                            else:
                                elem['ignore'] = True
                                elem2['tweets'].extend(elem['tweets'])
                                if 'exist' in elem:
                                    elem2['exist'] = True

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


            #log.debug("==========EVENT==========")
            for r in res:
                tweets = [t for t in r['tweets'].keys()]
                text = generateDefinition(tweets) #"{} {} {}".format(' '.join([l[0] for l in r['pred']]) , r['center'][0] , ' '.join([l[0] for l in r['succ']]))
                final.append([day,text,len(r['tweets']), 'Old' if 'exist' in r else 'New'])
                #print (r['tweets'].keys())
                # print("LP",longest_path(G))


        print()
        print(tabulate(final, headers=["Day", "Keywords", "#tweets", "Type"]))
        wr.writerow(final)
        #break

    print("Tweets" , sum([len(r['tweets']) for r in seen]), "out of", total)
    print("Detected Events", len([ s for s in seen if 'exist' not in s]))
    print("Igndored", len(ignored))
    print("Added", len(added))


        #break

        #tabulate(final,)



if __name__ == '__main__':
    #res = db.intervales("annotation_unsupervised")
    #print(res)
    process()