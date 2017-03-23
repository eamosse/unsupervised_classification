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
dists = []
tweetsSeen = set()
def process(opts):
    seen = []
    ne = opts.ne
    tmin = opts.tmin
    min_weight = opts.wmin
    smin = opts.smin
    total = 0
    gts = utils.gtEvents(limit=1)
    groups = db.intervales(collection, param="hour", interval=1)
    initialGraph = nx.DiGraph()

    myfile=open('results_{}_{}_{}.csv'.format(tmin, min_weight,smin), 'w')
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(["GT", "#tweets", "Detected"])

    for group in groups:
        day = group['day']
        log.debug(str(group['day']) + " - " + str(group['interval']))
        tweets = db.find(collection,query={"id":{"$in":group['data']}})

        # randomly select nb non event tweets to simulate a real scenario
        non_events = dirtyTweets(ne)
        # merge the event and non_event tweets
        data = tweets + non_events

        total += len(data)
        nodes = initialGraph.nodes(data=True)
        for node in nodes:
            node[1]['iteration'] = 1 if not 'iteration' in node[1] else node[1]['iteration'] + 1
        #initialGraph.clear()
        log.debug("Building the graph")
        for t in data:
            tweetsSeen.add(t['text'])
            ann = AnnotationHelper.format(t)
            if len(ann) == 0:
                ignored.append(t['id'])
            labels = [a['label'] for a in ann if 'label' in  a]
            for a in ann:
                for l in a['edges']:
                    if len(l[0].split()) < 3 and len(l[1].split()) < 3:
                        addEdge(initialGraph, l[0], l[1], t['id'], labels)
                    else:#nodes that have more than two tokens are most likely wrong NE identified by the NER tool(e.g. sandusky sentenced jail)
                        #thus, we split both initial nodes to create new edges
                        parts = ngrams(l[0].split() + l[1].split(), 2)
                        for p in parts:
                            addEdge(initialGraph,p[0],p[1],t['id'],labels)
        del data
        #Rename similar nodes
        log.debug("Merging nodes")
        #mergeNodes(initialGraph)
        log.debug("Cleaning the graph")
        #score = getScore(initialGraph, dangling=False)
        #lowest = [d[0] for d in score if d[1] <smin]
        #initialGraph.remove_nodes_from(lowest)
        gg = clean(initialGraph)
        #display(initialGraph)
        for graph in gg :
            news = []
            olds = []
            res = []
            log.debug("Retrieving nodes")
            degree = graph.degree(weight='weight')
            degree = [d for d in degree.items()]
            degree.sort(key=operator.itemgetter(1), reverse=True)
            """for i,d in enumerate(degree):
                for j in range(i+1, len(degree)):
                    dd = degree[j]
                    #if the link between d and dd is higher than any other nodes connetected to dd
                    if graph.has_edge(d[0], dd[0]) or graph.has_edge(dd[0], d[0]):
                        weight = graph.get_edge_data(d[0], dd[0])['weight'] if graph.has_edge(d[0], dd[0]) else graph.get_edge_data(dd[0], d[0])['weight']"""
            candidates = []
            while degree:
                deg = degree.pop(0)[0]
                succs = highestPred(graph, deg, direct=1)[0:2]
                preds = highestPred(graph, deg)[0:2]

                candidate = {
                    'center' : deg,
                    'predecessors' : preds,
                    'successors' : succs,
                    'tweets' : []
                }

                degree = [d for d in degree if d[0] not in preds+succs]
                keys = candidate['predecessors'] + [candidate['center']] + candidate['successors']
                if(len(keys) > 2):
                    candidates.append(candidate)
                    for c in succs:
                        candidate['tweets'].extend(graph.get_edge_data(deg, c)['id'])
                    for c in preds:
                        candidate['tweets'].extend(graph.get_edge_data(c, deg)['id'])
                    print(candidate)

                    log.debug("Generating event description")
                    text = generateDefinition(candidate['tweets']) #
                    log.debug("Getting GT")
                    event = AnnotationHelper.groundTruthEvent("all_tweets",candidate['tweets'])

                    if event and len(event) == 1:
                        news.append([day, text, event[0], len(candidate['tweets']), ' '.join(keys)])
                        for g in gts:
                            if int(g[0]) == int(event[0]):
                                g.append(event[0])
                                break
                    else:
                        news.append(
                            [day, text, "-1", len(candidate['tweets']), ' '.join(keys)])
                        gts.append(['-1'])

                graph.remove_nodes_from(preds + succs)
                initialGraph.remove_nodes_from(keys)
                seen.append(candidate)

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
    parser.add_option('-n', '--negative', dest='ne', default=10000, type=int)
    parser.add_option('-t', '--tmin', dest='tmin', default=1, type=int)
    parser.add_option('-w', '--wmin', dest='wmin', default=2, type=int)
    parser.add_option('-s', '--smin', dest='smin', default=0.001, type=float)
    #print(res)
    opts, args = parser.parse_args()
    process(opts)
