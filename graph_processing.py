import csv
import helper
import StreamManager
from helper import  TextHelper
from tabulate import tabulate
import utils
from optparse import OptionParser
from Score import *
from textrank import  *
collection = "events_annotated"
log = helper.enableLog()

seen = []
texts = []

visited = []
day = 0
initialGraph = None

nes = []
def build_graph(G, data):
    for t in data:
        if not 'id' in t:
            continue

        """if not t['id'] in visited:
            visited.append(t['id'])
        else:
            continue

        if not t['text'] in texts:
            texts.append(t['text'])
        else:
            continue

        if len(TextHelper.tokenize(t['text'])) < 2:
            continue"""

        ann = TextHelper.extract_entity_context(t)
        labels = [a['label'] for a in ann if 'label' in a]

        for a in ann:

            if 'type' in a and a['label'].lower() not in nes and ('location' in a['type'] or 'person' in a['type']):
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
    deg = degree[:]
    degree = [d[0] for d in degree]
    while degree:
        t = degree.pop(0)
        predecessors = [p for p in graph.predecessors(t) if p in degree] #highestPred(graph, t[0], deg)
        successors = [p for p in graph.successors(t) if p in degree] #highestPred(graph, t[0], deg)
        #successors = highestPred(graph, t[0], deg, direct=1)

        if not predecessors and not successors:
            continue

        toRem = [(pred,t) for pred in predecessors]
        toRem.extend([(t, succ) for succ in successors])
        vals = predecessors + [t] + successors
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

        graph.remove_nodes_from(mnodes)

        toRem = list(set([r[0] for r in toRem]).union(set([r[1] for r in toRem])))
        if len(graph) > 0 and not nx.is_strongly_connected(graph):
            # log.debug("Graph beccame disconnected beacause of nodes removal")
            components = get_components(graph)
            for c in components:
                _nodes = c.nodes()
                if len(_nodes) <= 2:

                    for n in _nodes:
                        for t in toRem:
                            if initialGraph.has_edge(n, t):
                                val['tweets']= val['tweets'].union(initialGraph.get_edge_data(n, t)['id'])
                            if initialGraph.has_edge(t, n):
                                val['tweets']= val['tweets'].union(initialGraph.get_edge_data(t, n)['id'])
                    graph.remove_nodes_from(_nodes)
                    degree = [d for d in degree if d[0] not in _nodes]
        if val['keys']:
            res.append(val)
    print([{'keys': r['keys'], 'tweets' : len(r['tweets'])} for r in res])
    return res

def merge(elem, elem2):
    elem2['ignore'] = True
    elem['tweets'] = elem['tweets'].union(elem2['tweets'])
    if not 'keyss' in elem:
        elem['keyss'] = set(list(elem['keys'])[:])
    elem['keyss'] = elem['keyss'].union(elem2['keyss'])


def has_edge(node1, node2):
    initialGraph = nx.DiGraph()
    #origin, destination = None, None
    if initialGraph.has_edge(node1, node2) :
        origin,destination = node1,node2
    elif initialGraph.has_edge(node2, node1):
        origin, destination = node2, node1
    else:
        return False
    #edges = initialGraph.edges([origin], data='weight')
    edges = list(initialGraph.edges_iter(nbunch=[origin], data='weight', default=1))
    return edges[0][0] == destination or edges[0][1] == destination


def merge_duplicate_events(res):
    hasMerged = True
    log.debug("Merge duplicated events...")
    round = 1
    res =  [r for r in res if len(r['tweets']) >= 5]
    #res = sorted(res, key=lambda k: len(k['tweets']), reverse=True)

    while hasMerged:
        hasMerged = False
        res = [elem for elem in res if 'ignore' not in elem and len(elem['tweets']) > 0 and len(elem['keys']) >= 1*round]
        for i, elem in enumerate(res):
            if 'ignore' in elem or not elem['keys'].intersection(set(nes)):
                print("#1", elem['keys'])
                elem['ignore'] = True
                continue

            elem['ents'] = elem['keyss'].intersection(set(nes))
            for j in range(i + 1, len(res)):
                elem2 = res[j]
                if 'ignore' in elem2 or not elem2['keys']:
                    elem2['ignore'] = True
                    print("#2", elem2['keys'])
                    continue
                elem2['ents'] = elem2['keyss'].intersection(set(nes))
                common_ents = len(elem2['ents'].intersection(elem['ents']))
                #common_keys = len(elem2['keyss'].intersection(elem['keyss']))
                if elem2['keyss'].issubset(elem['keyss']) or elem['keyss'].issubset(elem2['keyss']) or common_ents > 1*round :
                    merge(elem,elem2)
                    hasMerged = True
                    print("#3", elem2['keys'])
                    continue
                    # break
                #initialGraph = nx.DiGraph()
                if initialGraph.has_edge(elem2['center'], elem['center']) or initialGraph.has_edge(elem['center'], elem2['center']):
                    merge(elem, elem2)
                    hasMerged = True
                    continue
                index = 0
                for _e in elem['keyss']:
                    for _ee in elem2['keyss']:
                        if has_edge(_e, _ee):
                            index+=1
                        if index > 1*round:
                            merge(elem, elem2)
                            print("#4", elem2['keys'])
                            hasMerged = True
                    if index > 1:
                        break
        round += 1


    for i, elem in enumerate(res):
        for j in range(i+1, len(res)):
            if len(elem['tweets'].intersection(res[j])) > 2:
                print("#5", res[j]['keys'])
                merge(elem,res[j])

    for elem in res:
        if not elem['keys'] or 'ignore' in elem:
            elem['ignore'] = True
            continue
        for s in seen:
            if elem['keys'].issubset(s['keys']) or s['keys'].issubset(elem['keys']) or len(
                elem['ents'].intersection(s['ents'])) >= 1:
                elem['ignore'] = True
                print("#6", elem['keys'])
                s['tweets'] = s['tweets'].union(elem['tweets'])
                if not 'keyss' in elem:
                    elem['keyss'] = set(list(elem['keys'])[:])
                elem['keyss'] = elem['keyss'].union(s['keys'])
                break



    return [elem for elem in res if 'ignore' not in elem and len(elem['tweets']) > 0 and len(elem['keys']) >= 1]

def process(opts):
    global initialGraph
    StreamManager.ne = opts.ne
    StreamManager.interval = opts.int
    tmin = opts.tmin
    min_weight = opts.wmin
    smin = opts.smin
    gts = StreamManager.gtEvents(limit=1)

    initialGraph = nx.DiGraph()

    myfile=open('results_{}_{}_{}.csv'.format(tmin, min_weight,smin), 'w')
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(["GT", "#tweets", "Detected"])

    global seen
    seen = []
    while True:
        global  day
        group = StreamManager.nextBatch()
        if not group:
            break
        day = group['day']
        seen = [s for s in seen if day - s['day'] <=2]
        log.debug(str(group['day']) + " - " + str(group['interval']))

        nodes = initialGraph.nodes(data=True)
        for node in nodes:
            node[1]['iteration'] = 1 if not 'iteration' in node[1] else node[1]['iteration'] + 1
        #initialGraph.clear()
        log.debug("Building the graph")
        #build the graph from tweets
        build_graph(initialGraph, group['data'])
        del group

        #Rename similar nodes
        #log.debug("Merging nodes")

        log.debug("Cleaning the graph")
        __nodes = degrees(initialGraph, nbunch=initialGraph.nodes())
        __nodes = [d if d[1] > 0 else (d[0], 1) for d in __nodes]

        _degree = getScore(initialGraph, __nodes, dangling=False)
        _degree = [d for d in _degree if d[1] >= smin]
        #nodeg = [d[0] for d in _degree if d[1] < smin]

        #initialGraph.remove_nodes_from(nodeg)

        clean(initialGraph, min_weight=min_weight)

        ggg = [initialGraph]
        """for g in gg:
            ggg.extend(clean(g, min_weight=0))"""

        for graph in ggg :
            log.debug("Retrieving nodes")
            nodes = graph.nodes(data=True)

            nodes= [n[0] for n in nodes if n[1]['entity']]
            #degree = [d for d in _degree if d[0] in nodes]

            log.debug("Ranking nodes")
            res = extract_event_candidates(_degree, graph, initialGraph, nodes)

            log.debug("Pruning the graph")
            events = merge_duplicate_events(res)

            log.debug("Pruning detected events")

            news = []
            log.debug("Generating Description for {} candidates".format(len(events)))
            for r in events:
                r['day'] = day
                tweets = list(r['tweets'])
                """if len(r['tweets']) < 10 :
                    continue"""
                text = generateDefinition(tweets) #
                event = AnnotationHelper.groundTruthEvent(collection,tweets)
                if event and len(event) == 1:
                    news.append([day, text, event[0], len(r['tweets']), r['keyss']])
                    for g in gts:
                        if int(g[0]) == int(event[0]):
                            g.append(event[0])
                            break
                else:
                    news.append(
                        [day, text, "-1", len(r['tweets']), r['keyss']])
                    gts.append(['-1'])

                initialGraph.remove_nodes_from(list(r['keyss']))
                seen.append(r)

            toDelete = []
            nodes = initialGraph.nodes(data=True)
            for node in nodes:
                if 'iteration' in node[1] and node[1]['iteration'] > 2:
                    toDelete.append(node[0])
            initialGraph.remove_nodes_from(toDelete)
            print("New Events")
            tt = tabulate(news, headers=["Day", "Description", "Ground Truth", "#tweets", "Keywords"])
            print(tt)

    for gt in gts:
        wr.writerow(gt)

    print('Done')


if __name__ == '__main__':
    ##print(TextHelper.stops("go helll fuck this shiet damn it lmfao loll "))

    parser = OptionParser('''%prog -o ontology -t type -f force ''')
    parser.add_option('-n', '--negative', dest='ne', default=50000, type=int)
    parser.add_option('-t', '--tmin', dest='tmin', default=1, type=int)
    parser.add_option('-w', '--wmin', dest='wmin', default=3, type=int)
    parser.add_option('-i', '--int', dest='int', default=1, type=int)
    parser.add_option('-s', '--smin', dest='smin', default=0.00001, type=float)
    #print(res)
    opts, args = parser.parse_args()
    process(opts)