import csv
import helper
from helper import  TextHelper
from scipy.spatial.tests.test_kdtree import two_trees_consistency
from tabulate import tabulate
import utils
from optparse import OptionParser
from Score import *
from textrank import  *
collection = "events_annotated"
log = helper.enableLog()
#helper.disableLog(log)
non_events =[]
#previous = []

ignored = []
added = []

    #updateNode(G,u)
db.connect("tweets_dataset")


visited = set()

dirty = []
max = 60400
current = 0

def dirtyTweets(nb):
    global current
    global max
    if current >= max:
        current = 0
    non_events = db.find("nevents", limit=nb, skip=current)
    current = nb + current
    return non_events

def mSum(arr):
    return sum([t[1] for t in arr])

def getEntityNodes(nodes,elem):
    res = []
    p = []
    for i in range(len(elem['center'])):
        p.extend(elem['pred'][i] + [elem['center'][i]] + elem['succ'][i])
    for el in p:
        if el[0] in nodes:
            res.append(el[0])
    return res

def getEntityNodes2(elem):
    p = []
    for i in range(len(elem['center'])):
        p.extend(elem['pred'][i] + [elem['center'][i]] + elem['succ'][i])

    return p


fOld = open('old.txt','w')

divergence = [3,50]
def process(opts):
    seen = []
    ne = opts.ne
    tmin = opts.tmin
    min_weight = opts.wmin
    smin = opts.smin
    total = 0
    gts = utils.gtEvents(limit=1)
    groups = db.intervales(collection, param="hour", interval=2)
    initialGraph = nx.DiGraph()

    myfile=open('results_{}_{}_{}.csv'.format(tmin, min_weight,smin), 'w')
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(["GT", "#tweets", "Detected"])

    for group in groups:
        day = group['day']
        log.debug(str(group['day']) + " - " + str(group['interval']))
        #day =
        # get the tweets published dring this day
        #print(day, group[day])
        #continue
        #tweets = db.aggregateDate(collection=collection, day=day)
        tweets = db.find(collection,query={"id":{"$in":group['data']}})

        # randomly select nb non event tweets to simulate a real scenario
        non_events = dirtyTweets(ne)
        # merge the event and non_event tweets
        data = tweets + non_events

        total += len(data)
        nodes = initialGraph.nodes(data=True)
        for node in nodes:
            node[1]['iteration'] = 1 if not 'iteration' in node[1] else node[1]['iteration'] + 1
        initialGraph.clear()
        log.debug("Building the graph")
        for t in data:
            ann = AnnotationHelper.format(t)
            if len(ann) == 0:
                ignored.append(t['id'])
            labels = [a['label'] for a in ann if 'label' in a]
            for a in ann:
                for l in a['edges']:
                    if len(l[0].split()) < 3 and len(l[1].split()) < 3:
                        addEdge(initialGraph, l[0], l[1], t['id'], labels)
                    else:  # nodes that have more than two tokens are most likely wrong NE identified by the NER tool(e.g. sandusky sentenced jail)
                        # thus, we split both initial nodes to create new edges
                        parts = ngrams(l[0].split() + l[1].split(), 2)
                        for p in parts:
                            addEdge(initialGraph, p[0], p[1], t['id'], labels)
        del data
        #Rename similar nodes
        log.debug("Merging nodes")
        mergeNodes(initialGraph)
        log.debug("Cleaning the graph")
        gg = clean(initialGraph, min_weight=min_weight)
        #gg = [initialGraph]
        #display(initialGraph)
        __nodes = degrees(initialGraph, nbunch=initialGraph.nodes())
        __nodes = [d if d[1] > 0 else (d[0], 1) for d in __nodes]

        for graph in gg :
            news = []
            olds = []
            res = []
            log.debug("Retrieving nodes")
            nodes = graph.nodes(data=True)
            nodes= [n[0] for n in nodes if 'entity' in n[1] and n[1]['entity']]
            degree = getScore(graph,__nodes, dangling=True)
            degree =[d for d in degree if d[0] in nodes and d[1] >= smin]

            if len(degree) == 0:
                continue
            log.debug("Ranking nodes")
            while degree:
                t = degree[0]
                predecessors = highestPred( graph,t[0])
                successors = highestPred(graph,t[0],direct=1)
                if not predecessors and not successors:
                    degree = [d for d in degree if d[0]!=t[0]]
                    continue
                toRem = [(pred[0],t[0]) for pred in predecessors]
                toRem.extend([(t[0], succ[0]) for succ in successors])
                vals = [t[0] for t in predecessors] + [t[0]]
                vals = vals + [t[0] for t in successors]

                # remove the pred and the succ in the list
                degree = [d for d in degree if d[0] not in vals]

                val = {'keys' : vals, 'center' : t, 'tweets' : []}
                #print(vals)
                for d in toRem:
                    dd = graph.get_edge_data(d[0], d[1])
                    val['tweets'].extend(dd['id'])
                for d in predecessors + successors:
                    if graph.degree(d[0]) == 1:
                        toRem.append((d[0], t[0]) if graph.has_edge(d[0], t[0]) else (t[0], d[0]))
                        val['tweets'].extend(graph.get_edge_data(d[0], t[0])['id'] if graph.has_edge(d[0], t[0]) else  graph.get_edge_data(t[0], d[0])['id'])
                res.append(val)
                graph.remove_edges_from(toRem)


            #removed candidates that have a node corresponding to the center of an event
            log.debug("Pruning the graph")
            for i,elem in enumerate(res):
                exist = False

                for s in seen:
                    if len(set(elem['keys']).intersection(s['keys'])) > 2:
                        elem['ignore'] = True
                        s['tweets'].extend(elem['tweets'])
                        exist = True
                        break

                if exist:
                    continue

                for j in range(i+1, len(res)):
                    elem2 = res[j]
                    k1 = [e for e in elem['keys'] if e in nodes]
                    k2 = [e for e in elem2['keys'] if e in nodes]
                    if len ((set(k1)).intersection(set(k2))) > 1:
                        elem2['ignore'] = True
                        elem['tweets'].extend(elem2['tweets'])
                        #elem['keys'].extend(elem2['keys'])
                        break
            res = [elem for elem in res if 'ignore' not in elem]
            log.debug("Pruning detected events")


            events = [e for e in res if 'ignore' not in e]

            for i, r in enumerate(res):
                r['day'] = day
                r['tweets'] = list(set(r['tweets']))
                r['keys'] = list(set([rr for rr in r['keys'] if rr in nodes]))
            # log.debug("==========EVENT==========")

            for i,r in enumerate(events):
                """tweets = [t for t in r['tweets'].keys()]
                if not tweets or len(tweets) < 3:
                    continue"""
                tweets = r['tweets']
                #log.debug (tweets)
                if len(tweets) < 10 or len(r['keys']) < 2: # or len(r['pred']) <=2 or (len(r['succ']) <=2)) and len(r['tweets']) < 10:
                    continue

                #print(day, tweets)
                log.debug("Generating event description")
                text = generateDefinition(tweets) #
                log.debug("Getting GT")
                event = AnnotationHelper.groundTruthEvent(collection,tweets)
                #r['keys'] = [(rr, graph.degree(rr)) for rr in r['keys']]
                #log.debug(event)
                if not 'exist' in r:
                    if event and len(event) == 1:
                        news.append([day, text, event[0], len(r['tweets']), r['keys']])
                        for g in gts:
                            if int(g[0]) == int(event[0]):
                                g.append(event[0])
                                break
                    else:
                        news.append(
                            [day, text, "-1", len(r['tweets']), r['keys']])
                        gts.append(['-1'])
                else:
                    olds.append([day,text,r['event'], len(tweets)])

                r['event'] = event[0] if event else -1

                initialGraph.remove_nodes_from(r['keys'])
                seen.append(r)

            toDelete = []
            nodes = initialGraph.nodes(data=True)
            for node in nodes:
                if 'iteration' in node[1] and node[1]['iteration'] > 2:
                    toDelete.append(node[0])
            print("New Events")
            tt = tabulate(news, headers=["Day", "Description", "Ground Truth", "#tweets", "Keywords"])
            print(tt)

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


    parser = OptionParser('''%prog -o ontology -t type -f force ''')
    parser.add_option('-n', '--negative', dest='ne', default=1000, type=int)
    parser.add_option('-t', '--tmin', dest='tmin', default=1, type=int)
    parser.add_option('-w', '--wmin', dest='wmin', default=2, type=int)
    parser.add_option('-s', '--smin', dest='smin', default=0.005, type=float)
    #print(res)
    opts, args = parser.parse_args()
    process(opts)