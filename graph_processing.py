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
        tweets = db.find(collection,query={"id":{"$in":group['data']}})
        non_events = dirtyTweets(ne)
        data = tweets + non_events

        nodes = initialGraph.nodes(data=True)
        for node in nodes:
            node[1]['iteration'] = 1 if not 'iteration' in node[1] else node[1]['iteration'] + 1
        initialGraph.clear()
        log.debug("Building the graph")
        initialGraph.clear()
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
            res = []
            log.debug("Retrieving nodes")
            nodes = graph.nodes(data=True)
            nodes= [n[0] for n in nodes if 'entity' in n[1] and n[1]['entity'] and  not TextHelper.isStopWord(n[0])]
            degree = getScore(graph,__nodes, dangling=True)
            degree =[d for d in degree if d[1] >= smin] # if d[0] in nodes and d[1] >= smin]

            if len(degree) == 0:
                continue
            log.debug("Ranking nodes")
            while degree:
                t = degree[0]
                predecessors = highestPred(graph,t[0])
                successors = highestPred(graph,t[0],direct=1)
                if not predecessors and not successors:
                    degree = [d for d in degree if d[0]!=t[0]]
                    continue
                toRem = [(pred[0],t[0]) for pred in predecessors[0:1]]
                toRem.extend([(t[0], succ[0]) for succ in successors[0:1]])
                vals = [t[0] for t in predecessors[0:1]] + [t[0]]
                vals = vals + [t[0] for t in successors[0:1]]

                # remove the pred and the succ in the list
                degree = [d for d in degree if d[0] not in vals]

                val = {'keys' : set([v for v in vals if v in nodes]), 'center' : t, 'tweets' : []}
                #print(vals)
                for d in toRem:
                    dd = graph.get_edge_data(d[0], d[1])
                    val['tweets'].extend(dd['id'])
                for d in predecessors[1:] + successors[1:]:
                    if graph.degree(d[0]) == 1:
                        toRem.append((d[0], t[0]) if graph.has_edge(d[0], t[0]) else (t[0], d[0]))
                        val['tweets'].extend(graph.get_edge_data(d[0], t[0])['id'] if graph.has_edge(d[0], t[0]) else  graph.get_edge_data(t[0], d[0])['id'])

                graph.remove_edges_from(toRem)

                toRem = list(set([r[0] for r in toRem]).union(set([r[1] for r in toRem])))
                if not nx.is_strongly_connected(graph):
                    #log.debug("Graph beccame disconnected beacause of nodes removal")
                    components = get_components(graph)
                    for c in components:
                        _nodes = c.nodes()
                        if len(_nodes) <=2:

                            for n in _nodes:
                                for t in toRem:
                                    if initialGraph.has_edge(n, t) :
                                        val['tweets'].extend(initialGraph.get_edge_data(n, t)['id'])
                                    if initialGraph.has_edge(t,n):
                                        val['tweets'].extend(initialGraph.get_edge_data(t, n)['id'])
                            graph.remove_nodes_from(_nodes)
                            degree = [d for d in degree if d[0] not in _nodes]
                if val['keys']:
                    res.append(val)


            #removed candidates that have a node corresponding to the center of an event
            log.debug("Pruning the graph")
            #events = []
            for i,elem in enumerate(res):
                if 'ignore' in elem or not elem['keys']:
                    elem['ignore'] = True
                    continue

                for j in range(i+1, len(res)):
                    elem2 = res[j]
                    if 'ignore' in elem2 or not elem2['keys']:
                        elem2['ignore'] = True
                        continue

                    if elem2['keys'].issubset(elem['keys']) or elem['keys'].issubset(elem2['keys']) or len (elem2['keys'].intersection(elem['keys'])) >= 1:
                        elem2['ignore'] = True
                        elem['tweets'].extend(elem2['tweets'])
                        #break

            for elem in res:
                if not elem['keys']:
                    elem['ignore'] = True
                    continue
                for s in seen:
                    if elem['keys'].issubset(s['keys']) or s['keys'].issubset(elem['keys']) or len(elem['keys'].intersection(s['keys'])) >= 2 :
                        elem['ignore'] = True
                        s['tweets'].extend(elem['tweets'])
                        s['keys'] = elem['keys'].union(s['keys'])
                        break

            events = [elem for elem in res if 'ignore' not in elem and len(elem['tweets']) > 5 and  len(elem['keys']) >= 2]
            log.debug("Pruning detected events")

            # log.debug("==========EVENT==========")
            news = []
            log.debug("Generating Description for {} candidates".format(len(events)))
            for i,r in enumerate(events):
                r['day'] = day
                r['tweets'] = list(set(r['tweets']))
                tweets = r['tweets']
                #log.debug (tweets)
                if len(tweets) < 5 or len(r['keys']) < 2 : # or len(r['pred']) <=2 or (len(r['succ']) <=2)) and len(r['tweets']) < 10:
                    continue

                text = generateDefinition(tweets) #
                event = AnnotationHelper.groundTruthEvent(collection,tweets)
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

    print('Done')



if __name__ == '__main__':


    parser = OptionParser('''%prog -o ontology -t type -f force ''')
    parser.add_option('-n', '--negative', dest='ne', default=10000, type=int)
    parser.add_option('-t', '--tmin', dest='tmin', default=1, type=int)
    parser.add_option('-w', '--wmin', dest='wmin', default=2, type=int)
    parser.add_option('-s', '--smin', dest='smin', default=0.001, type=float)
    #print(res)
    opts, args = parser.parse_args()
    process(opts)