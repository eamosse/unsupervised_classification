import csv
import helper
from helper import  TextHelper
from tabulate import tabulate
import utils
from optparse import OptionParser
from Score import *
from textrank import  *
collection = "events_annotated"
log = helper.enableLog()
#helper.disableLog(log)
non_events =[]
previous = []

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

def getEntityNodes2(elem):
    p = []
    for i in range(len(elem['center'])):
        p.extend(elem['pred'][i] + [elem['center'][i]] + elem['succ'][i])

    return p


fOld = open('old.txt','w')

divergence = [3,50]
dists = []
tweetsSeen = set()
def process(opts):
    ne = opts.ne
    tmin = opts.tmin
    min_weight = opts.wmin
    smin = opts.smin
    total = 0
    gts = utils.gtEvents(limit=1)
    groups = db.intervales(collection)
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
        """if tweets:
            tweets = tweets[0]['data']
        else:
            continue"""

        # randomly select nb non event tweets to simulate a real scenario
        non_events = dirtyTweets(ne)
        # merge the event and non_event tweets
        data = tweets + non_events

        random.shuffle(data)
        total += len(data)
        dist = []

        initialGraph.clear()
        log.debug("Building the graph")
        for t in data:
            if t['text'] in tweetsSeen:
                continue
            tweetsSeen.add(t['text'])
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
        log.debug("Merging nodes")
        mergeNodes(initialGraph)
        log.debug("Cleaning the graph")
        clean(initialGraph, min_weight=min_weight)
        news = []
        olds = []
        res = []
        log.debug("Retrieving nodes")
        _nodes = initialGraph.nodes(data=True)
        previous.append('=>'.join([n[0] for n in _nodes]))
        nodes = [node[0] for node in _nodes if 'entity' in node[1] and node[1]['entity']]

        degree = getScore(initialGraph)
        degree = [dg for dg in degree if dg[1]>=smin and dg[0] in nodes and dg[0] not in [d[0] for d in dist]]

        if len(degree) == 0:
            continue

        log.debug("Ranking nodes")
        while degree:
            t = degree[0]
            predecessors = highestPred( initialGraph,t[0])
            successors = highestPred(initialGraph,t[0],direct=1)
            if not predecessors and not successors:
                degree = [d for d in degree if d[0]!=t[0]]
                continue

            vals = [t[0] for t in predecessors[0:2]] + [t[0]]
            vals = vals + [t[0] for t in successors[1:3]]

            # remove the pred and the succ in the list
            degree = [d for d in degree if d[0] not in vals]

            val = {'keys' : vals, 'center' : t, 'tweets' : []}

            for d in ngrams(vals,2):
                dd = initialGraph.get_edge_data(d[0], d[1])
                val['tweets'].extend(dd['id'])
            res.append(val)

        #removed candidates that have a node corresponding to the center of an event
        log.debug("Pruning the graph")
        for i,elem in enumerate(res):
            entities1 = initialGraph.neighbors(elem['center'][0])
            if 'exist' in elem or 'ignore' in elem or not entities1:
                elem['ignore'] = True
                elem['exist'] = True
                continue
            exist = False

            for s in seen:
                if s['center'][0] == elem['center'][0] or len(set(s['keys']).intersection(set(elem['keys']))) > 2:
                    elem['exist'] = True
                    elem['ignore'] = True
                    s['tweets'].extend(elem['tweets'])
                    exist = True
                    break

            if exist:
                continue

            for j in range(i+1, len(res)):
                elem2 = res[j]
                if len(set(elem['keys']).intersection(elem2['keys'])) > 1:
                    elem2['ignore'] = True
                    elem2['exist'] = True
                    elem['tweets'].extend(elem2['tweets'])
                    elem['keys'].extend(elem2['keys'])

                """entities2  = initialGraph.neighbors(elem2['center'][0])
                if not entities2:
                    elem2['ignore'] = True
                    elem['exist'] = True
                    continue
                exist = False
                count = 0
                for e1 in entities1:
                    for e2 in entities2:
                        if initialGraph.has_edge(e1, e2) or initialGraph.has_edge(e2, e1) or e1 == e2:
                            elem['tweets'].extend(elem2['tweets'])
                            exist = True
                            break

                    if exist:
                        elem2['ignore'] = True
                        if 'exist' in elem2:
                            elem['exist'] = True
                        break"""

        res = [elem for elem in res if 'ignore' not in elem]
        log.debug("Pruning detected events")

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

        for i, r in enumerate(res):
            r['day'] = day
            r['tweets'] = list(set(r['tweets']))
        # log.debug("==========EVENT==========")

        for i,r in enumerate(events):
            """tweets = [t for t in r['tweets'].keys()]
            if not tweets or len(tweets) < 3:
                continue"""
            tweets = r['tweets']
            log.debug (tweets)
            if len(tweets) < 10: # or len(r['pred']) <=2 or (len(r['succ']) <=2)) and len(r['tweets']) < 10:
                continue

            #print(day, tweets)
            log.debug("Generating event description")
            text = generateDefinition(tweets) #
            log.debug("Getting GT")
            event = AnnotationHelper.groundTruthEvent(collection,tweets)
            if not 'exist' in r:
                if event :
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
    parser.add_option('-w', '--wmin', dest='wmin', default=3, type=int)
    parser.add_option('-s', '--smin', dest='smin', default=0.02, type=float)
    #print(res)
    opts, args = parser.parse_args()
    process(opts)
