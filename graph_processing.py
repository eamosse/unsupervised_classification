import csv
import helper
from tabulate import tabulate
from EventDefinition import *
import random
import utils
from Score import *
collection = "annotation_unsupervised"
log = helper.enableLog()
#helper.disableLog(log)
non_events =[]


ignored = []
added = []

    #updateNode(G,u)
db.connect("tweets_dataset")


visited = set()


def dirtyTweets(nb):
    global non_events
    non_events = db.find("non_event")
    non_events = [event for event in non_events if not event['text'].startswith('rt')]
    random.shuffle(non_events)
    random.shuffle(non_events)
    random.shuffle(non_events)
    return non_events[0:nb]


def mSum(arr):
    return sum([t[1] for t in arr])

seen = []
final = []

def getEntityNodes(nodes,elem):
    res = []
    p = []
    for i in range(len(elem['center'])):
        p.extend(elem['pred'][i] + [elem['center'][i]] + elem['succ'][i])
    for el in p:
        if el[0] in nodes:
            res.append(el[0])
    return res


fOld = open('old.txt','w')

ne = 10000
tmin = 30
divergence = [3,20]
min_weight = 1

def process():
    total = 0
    gts = utils.gtEvents(limit=tmin)
    days = db.intervales(collection)
    initialGraph = nx.DiGraph()

    myfile=open('results_{}_{}_{}_{}_{}.csv'.format(ne,tmin,min(divergence), max(divergence), min_weight), 'w')
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(["GT", "#tweets", "Detected"])

    for day in days:
        #fNew = open('{}.txt'.format(day), 'w')
        # get the tweets published dring this day
        tweets = db.aggregateDate(collection=collection, day=day)
        if tweets:
            tweets = tweets[0]['data']
        else:
            continue
        # randomly select nb non event tweets to simulate a real scenario
        non_events = dirtyTweets(ne)
        # merge the event and non_event tweets
        data = tweets + non_events
        random.shuffle(data)
        total += len(data)

        """fNew.write("###########################################")
        fNew.write("Processing event on day {}".format(day))
        fNew.write("Tweets to process {}".format(len(data)))
        fNew.write("###########################################")"""

        initialGraph.clear()
        log.debug("Building the graph")
        for t in data:
            ann = AnnotationHelper.format(t)
            if len(ann) == 0:
                ignored.append(t['id'])
            for a in ann:
                for l in a['edges']:
                    if len(l[0].split()) < 3 and len(l[1].split()) < 3:
                        addEdge(initialGraph, l[0], l[1], t['id'], l[2])
                    else:#nodes that have more than two tokens are most likely wrong NE identified by the NER tool(e.g. sandusky sentenced jail)
                        #thus, we split both initial nodes to create new edges
                        parts = ngrams(l[0].split() + l[1].split(), 2)
                        for p in parts:
                            addEdge(initialGraph,p[0],p[1],t['id'])

        #Rename similar nodes
        log.debug("Cleaning the graph")
        mergeNodes(initialGraph)
        clean(initialGraph, min_weight=min_weight)
        news = []
        olds = []
        res = []
        _nodes = initialGraph.nodes(data=True)
        #nodes = [n[0] for n in _nodes]
        #print("In wordnet",nn)
        nodes = [node[0] for node in _nodes if 'entity' in node[1] and node[1]['entity']]

        #degree = degrees(initialGraph, nbunch=nodes)
        degree = getScore(initialGraph)

        degree = [dg for dg in degree if dg[1]>0.15 and dg[0] in nodes]

        if len(degree) == 0:
            continue


        for t in degree:

            predecessors = highestPred( initialGraph,t[0])
            successors = highestPred(initialGraph,t[0],direct=1)

            if not predecessors and not successors:
                continue
            val = {'center' : t, 'pound' : t[1], 'tweets' : []}
            if predecessors:
                pred = [t for t in predecessors[0:2]]
                val['pred'] = pred
                val['pound'] += sum([t[1] for t in pred])
            else:
                continue
                #val['pred'] = []
            #val.append((*t,'center'))
            if successors:
                suc = [t for t in successors[1:3]]
                val['succ'] = suc
                val['pound'] += sum([t[1] for t in suc])
            else:
                #val['succ'] = []
                continue

            for d in ngrams([e[0] for e in val['pred']] + [val['center'][0]] + [e[0] for e in val['succ']],
                            2):
                dd = initialGraph.get_edge_data(d[0], d[1])
                val['tweets'].extend(dd['id'])
            val['succ'] = [val['succ']]
            val['pred'] = [val['pred']]
            val['center'] = [val['center']]
            res.append(val)

        #removed candidates that have a node corresponding to the center of an event
        log.debug("Pruning the graph")
        for i,elem in enumerate(res):
            if 'exist' in elem or 'ignore' in elem:
                continue
            entities1 = getEntityNodes(nodes, elem)
            exist = False
            #ngg = ngrams([g[0] for g in elem['pred'][0] + elem['center'][0:1] + elem['succ'][0]], 2)
            #print("####",[n for n in ngg])
            for s in seen:
                if s['center'][0][0] == elem['center'][0][0]:
                    exist = True
                    break

            if exist:
                elem['exist'] = True
                elem['ignore'] = True
                elem['event'] = s['event']
                #break

            #s['tweets'].extend(elem['tweets'])
                    #s['tweets'].extend(elem['tweets'])
            if 'exist' in elem or 'ignore' in elem:
                continue

            for j in range(i+1, len(res)):
                elem2 = res[j]

                entities2  = getEntityNodes(nodes, elem2)

                equal,edge = 0,0
                for ent in entities1:

                    for ent2 in entities2:
                        if ent == ent2:
                            equal+=1
                        if initialGraph.has_edge(ent, ent2) or initialGraph.has_edge(ent2,ent):
                            edge +=1
                            if edge >=2:
                                break
                        if edge >=2:
                            break
                    if edge >= 1 and equal >= 1 or (edge == 2 or equal == 2):
                        elem2['ignore'] = True
                        elem['pred'].append( elem2['pred'])
                        elem['succ'].append(elem2['succ'])
                        elem['center'].append(elem2['center'])
                        if 'exist' in elem2:
                            elem['exist'] = True
                            elem['event'] = elem2['event']
                        break

        res = [elem for elem in res if 'ignore' not in elem or len(elem['center']) > max(divergence) or len(elem['center']) < min(divergence)]
        log.debug("Pruning detected events")

        for i, r in enumerate(res):
            r['day'] = day
            r['tweets'] = list(set(r['tweets']))

        for i, r in enumerate(res):
            for j in range(i + 1, len(res)):
                k = res[j]
                if 'ignore' in k:
                    continue
                intersect= set(r['tweets']).intersection(set(k['tweets']))
                if len(intersect) > 3:
                    r['tweets'].extend(k['tweets'])
                    k['ignore'] = True

        events = [e for e in res if 'ignore' not in e]
        # log.debug("==========EVENT==========")

        for i,r in enumerate(events):
            """tweets = [t for t in r['tweets'].keys()]
            if not tweets or len(tweets) < 3:
                continue"""
            tweets = r['tweets']
            log.debug (tweets)
            if len(tweets) < 4: # or len(r['pred']) <=2 or (len(r['succ']) <=2)) and len(r['tweets']) < 10:
                continue

            #print(day, tweets)
            text = generateDefinition(tweets) #
            event = AnnotationHelper.groundTruthEvent(tweets)
            if not 'exist' in r:
                if event :
                    for e in event:
                        news.append([day,text,e, len(r['tweets']),r['pound'], len(r['pred']),len(r['succ']), r['center'], "Yes" if r['center'][0] in nodes else 'No'])
                        for g in gts:
                            if int(g[0]) == int(e):
                                g.append(e)
                                break
                else:
                    news.append(
                        [day, text, "-1", len(r['tweets']), r['pound'], len(r['pred']), len(r['succ']),
                         r['center'], "Yes" if r['center'][0] in nodes else 'No'])
                    gts.append(['-1'])
            else:
                olds.append([day,text,r['event'], len(tweets)])

            r['event'] = event[0] if event else -1


            """
             if r['event'] == -1:
                gts.append([r['event']])
            else:
                for g in gts:
                    if int(g[0]) == int(r['event']):
                        g.append(r['event'])
                        break
            """
            seen.append(r)

        print("New Events")
        tt = tabulate(news, headers=["Day", "Keywords", 'Ground Truth',"#tweets", "pound", "pred","succ", "CALC", "center", "ENT"])
        print(tt)
        #fNew.write(tt)
        #fNew.close()
        """if olds:
            print("Paseed Events")
            tt = tabulate(olds, headers=["Day", "Keywords", 'Ground Truth',"#tweets"])
            print(tt)
            fOld.write(tt)
        for f in news:
            wr.writerow(f)
        #break"""
    for gt in gts:
        wr.writerow(gt)
    print("Tweets" , sum([len(r['tweets']) for r in seen]), "out of", total)
    print("Detected Events", len([ s for s in seen if 'exist' not in s]))
    print("Igndored", len(ignored))
    print("Added", len(added))
    #fOld.close()
    #fNew.close()
    #Computing the evaluation
    utils.evaluation()
        #break
        #tabulate(final,)


if __name__ == '__main__':
    #res = db.intervales("annotation_unsupervised")
    #print(res)
    process()
