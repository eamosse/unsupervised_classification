import csv
import helper
import StreamManager
from helper import  TextHelper
from tabulate import tabulate
import utils
from optparse import OptionParser
from Score import *
from textrank import  *
collection = "fsd_tweets"
collectionDef = "all_tweets"
overall = []
col = {
    'fsd' : 'fsd_tweets',
    'event_2012' :'events_annotated',
    'event_purge' :'events_annotated_purge'
}

coldef = {
    'fsd' : 'fsd_tweets',
    'event_2012' :'all_tweets',
    'event_purge' :'all_tweets'
}



log = helper.enableLog()

seen = []
texts = []
seen_terms = set()
visited = []
day = 0
initialGraph = None
toConfirm = []

nes = []
def build_graph(G, data):
    ignored = 0
    for t in data:
        if not 'id' in t or not t['annotations']:
            ignored+=1
            continue

        ann = TextHelper.extract_entity_context(t)
        labels = [a['label'] for a in ann if 'label' in a]
        if not ann:
            ignored += 1
        for a in ann:
            if 'type' in a and a['label'].lower() not in nes and ('location' in a['type'] or 'person' in a['type'] or 'organisation' in a['type']):
                nes.append(a['label'].lower())

            for l in a['edges']:
                if len(l[0].split()) < 3 and len(l[1].split()) < 3:
                    addEdge(G, l[0], l[1], t['id'], labels)
                else:  # nodes that have more than two tokens are most likely wrong NE identified by the NER tool(e.g. sandusky sentenced jail)
                    # thus, we split both initial nodes to create new edges
                    parts = ngrams(l[0].split() + l[1].split(), 2)
                    for p in parts:
                        addEdge(G, p[0], p[1], t['id'], labels)

def extract_event_candidates(degree, graph, initialGraph, nodes):
    log.debug("Extracting events candidate...")
    res = []
    degree = [d[0] for d in degree]
    #deg = degree[:]
    while degree:
        t = degree.pop(0)
        predecessors = [(p, t, graph.get_edge_data(p, t)['weight']) for p in graph.predecessors(t)]
        successors = [(t, s, graph.get_edge_data(t, s)['weight']) for s in graph.successors(t)]

        if not predecessors and not successors:
            continue

        predecessors.sort(key=operator.itemgetter(2),reverse=True)  #extractKeyphrases() = itemgetter.ge
        successors.sort(key=operator.itemgetter(2),reverse=True)  #extractKeyphrases() = itemgetter.ge

        toRem = set()
        for p in predecessors:
            toRem.add(p[0:2])
            for n in graph.neighbors(p[0]):
                if graph.neighbors(n) == 1:
                    toRem.add((n, p[0]) if graph.has_edge(n,p[0]) else (p[0],n))
                for path in nx.all_simple_paths(graph,n, t, cutoff=4):
                    if len(path) <=2:
                        for d in ngrams(path, 2):
                            toRem.add(d)

                for path in nx.all_simple_paths(graph, t, n,cutoff=4):
                    if len(path) <= 2:
                        for d in ngrams(path, 2):
                            toRem.add(d)

            #break

        index = 0
        for p in successors:
            for n in graph.neighbors(p[1]):
                if graph.neighbors(n) == 1:
                    toRem.add((n, p[1]) if graph.has_edge(n, p[1]) else (p[1], n))
                for path in nx.all_simple_paths(graph, n, t,cutoff=4):
                    if len(path) <= 2:
                        for d in ngrams(path, 2):
                            toRem.add(d)

                for path in nx.all_simple_paths(graph, t, n,cutoff=4):
                    if len(path) <= 2:
                        for d in ngrams(path, 2):
                            toRem.add(d)
        vals = [l[0] for l in toRem] + [l[1] for l in toRem]
        """for p in graph.predecessors(t):
            toRem.add((p,t))
            for n in graph.neighbors(p):
                if has_edge(graph, n, t) or graph.neighbors(n) == 1:
                    toRem.add((n, p) if graph.has_edge(n, p) else (p, n))

        for p in graph.successors(t):
            toRem.add((t,p))
            for n in graph.neighbors(p):
                if has_edge(graph, n, t) or graph.neighbors(n) == 1:
                    toRem.add((n, p) if graph.has_edge(n, p) else (p, n))

        vals = [l[0] for l in toRem] + [l[1] for l in toRem]"""



        #predecessors = highestPred(graph, t, deg) #[p for p in degree  if p in graph.predecessors(t)] #highestPred(graph, t[0], deg)
        #successors = highestPred(graph, t, deg, direct=1) #[p for p in degree if p in graph.successors(t)] #highestPred(graph, t[0], deg)
        #successors = highestPred(graph, t[0], deg, direct=1)

        #predecessors = predecessors[0]
        #successors = successors[0]
        """toRem = [pred[0:2] for pred in predecessors[0:1]]
        toRem.extend([succ[0:2] for succ in successors[0:1]])
        vals = [predecessors[0][0] , t , successors[0][1]]"""
        #vals = set(toRem)
        """toRem = [p[0] for p in predecessors[0:1]]
        toRem.append(t[0])
        vals.extend(toRem)
        toRem = list(ngrams(toRem,2))
        succ = [t[0]]
        succ.extend([p[0] for p in successors[1:2]])
        vals.extend(succ)
        toRem.extend(list(ngrams(succ,2)))"""

        #vals = set(vals)
        # remove the pred and the succ in the list
        degree = [d for d in degree if d not in vals]

        val = {'keys': set(vals), 'keyss' : set(vals[:]),  'center': t, 'tweets': set()}
        #print(toRem)
        mnodes = []
        for d in toRem:
            mnodes.extend([d[0],d[1]])
            dd = graph.get_edge_data(d[0], d[1])
            val['tweets']= val['tweets'].union(set(dd['id']))

        #graph.remove_edges_from(toRem)

        #display(initialGraph)

        toRem = list(set([r[0] for r in toRem]).union(set([r[1] for r in toRem])))
        if len(graph) > 0 and not nx.is_strongly_connected(graph):
            components = get_components(graph)
            for c in components:
                _nodes = c.nodes()
                if len(_nodes)<= 1:
                    for n in _nodes:
                        for t in toRem:
                            if graph.has_edge(n, t):
                                val['tweets']= val['tweets'].union(graph.get_edge_data(n, t)['id'])
                            if graph.has_edge(t, n):
                                val['tweets']= val['tweets'].union(graph.get_edge_data(t, n)['id'])
                    graph.remove_nodes_from(_nodes)
                    degree = [d for d in degree if d not in _nodes]
        if val['keys']:
            res.append(val)
    return res

def merge(elem, elem2):
    elem2['ignore'] = True
    elem['tweets'] = elem['tweets'].union(elem2['tweets'])
    if not 'keyss' in elem:
        elem['keyss'] = set(list(elem['keys'])[:])
    elem['keyss'] = elem['keyss'].union(elem2['keyss'])


def has_edge(graph, node1, node2):
    #origin, destination = None, None
    if graph.has_edge(node1, node2) :
        origin,destination = node1,node2
    elif graph.has_edge(node2, node1):
        origin, destination = node2, node1
    else:
        return False
    edges = graph.edges(nbunch=[origin], data='weight', default=1)
    edges.sort(key=operator.itemgetter(2), reverse = True)
    return edges[0][0] == destination or edges[0][1] == destination


def merge_duplicate_events(graph, res):
    hasMerged = True
    global toConfirm
    log.debug("Merge duplicated events...")
    round = 1

    for elem in res:
        elem['ents'] = elem['keyss'].intersection(set(nes))
        for i,t in enumerate(toConfirm):
            if not t['ents']:
                continue
            if len(elem['ents'].intersection(t['ents'])) >= 1:
                merge(elem, t)
                toConfirm.pop(i)
                break
            if not toConfirm:
                break
    toConfirm = [t for t in toConfirm if day - t['day'] < 2]

    res = [elem for elem in res if 'ignore' not in elem and len(elem['tweets']) > 0 and len(elem['keys']) > 1*round]
    for i, elem in enumerate(res):
        if 'ignore' in elem:
            continue

        elem['ents'] = elem['keyss'].intersection(set(nes))

        if not elem['ents']:
            log.debug("#1" + str(elem['keys']))
            elem['ignore'] = True
            continue

        if len(seen_terms.intersection(elem['ents'])) >= 2:
            elem['ignore'] = True
            continue

        for j in range(i + 1, len(res)):
            elem2 = res[j]
            if 'ignore' in elem2 or not elem2['keys']:
                elem2['ignore'] = True
                log.debug("#2" + str(elem2['keys']))
                continue
            elem2['ents'] = elem2['keyss'].intersection(set(nes))
            common_ents = len(elem2['ents'].intersection(elem['ents']))
            if elem2['keys'].issubset(elem['keys']) or elem['keys'].issubset(elem2['keys']) or common_ents >= 1*round :
                merge(elem,elem2)
                log.debug("#3" + str(elem2['keys']))
                continue


    for elem in res:
        if not elem['keys'] or 'ignore' in elem:
            elem['ignore'] = True
            continue

        for s in seen:
            if elem['keys'].issubset(s['keys']) or s['keys'].issubset(elem['keys']) or len(
                elem['ents'].intersection(s['ents'])) > 1:
                elem['ignore'] = True
                log.debug("#6" + str(elem['keys']))
                s['tweets'] = s['tweets'].union(elem['tweets'])
                if not 'keyss' in elem:
                    elem['keyss'] = set(list(elem['keys'])[:])
                elem['keyss'] = elem['keyss'].union(s['keys'])
                break

    return [elem for elem in res if 'ignore' not in elem and len(elem['tweets']) > 0 and len(elem['keys']) >= 1]

def process(opts):
    global initialGraph
    global  toConfirm
    collection = col[opts.dataset]
    collectionDef = coldef[opts.dataset]
    StreamManager.ne = opts.ne
    StreamManager.interval = opts.int
    StreamManager.init(opts.int,collection)
    tmin = opts.tmin
    min_weight = opts.wmin
    smin = opts.smin
    gts = StreamManager.gtEvents(limit=1)

    initialGraph = nx.DiGraph()

    myfile=open('results_{}_{}.csv'.format(collection,smin), 'w')
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(["GT", "#tweets", "Detected"])

    global seen
    seen = []
    global seen_terms
    while True:
        global  day
        group = StreamManager.nextBatch()
        if not group:
            break
        day = group['day']
        #seen = [s for s in seen if day - s['day'] <=2]
        log.debug(str(group['day']) + " - " + str(group['interval']))

        """nodes = initialGraph.nodes(data=True)
        for node in nodes:
            node[1]['iteration'] = 1 if not 'iteration' in node[1] else node[1]['iteration'] + 1"""
        initialGraph.clear()
        log.debug("Building the graph")
        #build the graph from tweets
        build_graph(initialGraph, group['data'])
        del group

        log.debug("Cleaning the graph")
        __nodes = degrees(initialGraph, nbunch=initialGraph.nodes())
        __nodes = [d if d[1] > 0 else (d[0], 1) for d in __nodes]

        _degree = getScore(initialGraph, __nodes, dangling=False)
        _degree = [d for d in _degree if d[1] >= smin]
        gg = clean(initialGraph, min_weight=min_weight)

        for graph in gg :
            log.debug("Retrieving nodes")
            nodes = graph.nodes(data=True)

            nodes= [n[0] for n in nodes if n[1]['entity']]
            degree = [d for d in _degree if d[0] in nodes]

            log.debug("Ranking nodes")
            res = extract_event_candidates(degree, graph, initialGraph, nodes)

            log.debug("Pruning the graph")
            events = merge_duplicate_events(graph, res)
            if not events:
                log.debug("No event found...")
                continue
            news = []
            log.debug("Generating Description for {} candidates".format(len(events)))
            for r in events:
                r['day'] = day
                tweets = list(r['tweets'])
                if len(r['tweets']) < opts.mtweet :
                    toConfirm.append(r)
                    continue

                event = AnnotationHelper.groundTruthEvent(collection,tweets)
                if event :
                    for e in event:
                        text = generateDefinition(collectionDef, e['tweets'])  #
                        news.append([day, text, e['id'], len(e['tweets']), r['keyss']])

                        for g in gts:
                            if int(g[0]) == int(e['id']):
                                g.append(e)
                                break
                else:
                    text = generateDefinition(collectionDef, tweets)  #
                    news.append(
                        [day, text, "-1", len(r['tweets']), r['keyss']])
                    gts.append(['-1'])
                seen_terms = seen_terms.union(r['keyss'])
                seen.append(r)

            overall.extend(news)
            tt = tabulate(news, headers=["Day", "Description", "Ground Truth", "#tweets", "Keywords"])
            print(tt)
            #display(initialGraph)

    for gt in gts:
        wr.writerow(gt)

    print("=====================Overall Evaluation=====================")
    oo = []
    for i, o in enumerate(overall):
        print(o)
        if len(o) > 4:
            continue
        for j in range(i + 1, len(overall)):
            if o[2] == overall[j][2]:
                overall[j].append('ignore')
        oo.append(o)
    tt = tabulate(oo, headers=["Day", "Description", "Ground Truth", "#tweets", "Keywords"])
    print(tt)

if __name__ == '__main__':
    ##print(TextHelper.stops("go helll fuck this shiet damn it lmfao loll "))

    parser = OptionParser('''%prog -o ontology -t type -f force ''')
    parser.add_option('-n', '--negative', dest='ne', default=1, type=int)
    parser.add_option('-t', '--tmin', dest='tmin', default=1, type=int)
    parser.add_option('-w', '--wmin', dest='wmin', default=0, type=int)
    parser.add_option('-i', '--int', dest='int', default=24, type=int)
    parser.add_option('-s', '--smin', dest='smin', default=0.5, type=float)
    parser.add_option('-m', '--mtweet', dest='mtweet', default=5, type=int)
    parser.add_option('-d', '--dataset', dest='dataset', default='fsd', type=str)
    #print(res)
    opts, args = parser.parse_args()
    process(opts)