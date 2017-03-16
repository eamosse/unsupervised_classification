"""
#get all the nodes
nodes = G.nodes()
#create a dictionnary with nodes as keys and a default value for all keys
nDict = {n:{'score':0.1, 'done':False} for n in nodes}
#set a threshold
threshold = 0.0001
d = 0.85
shouldIterate = True
while shouldIterate:
    for node in nodes:
        #predecessors of the node
        predecessors = G.predecessors(node)
        #current score for the node
        shouldIterate = not nDict[node]['done']
        if not shouldIterate:
            continue
        previousScore = nDict[node]['score']
        currentScore = 0
        #iterate over the predecessors of the current node
        for pred in predecessors:
            #get the weight of the current predecessor
            weight = G.get_edge_data(pred, node)['weight']
            #get the successors of the current predecessor of the current node
            successors = G.successors(pred)
            #compute sum of the successors of the current predecessor of a node
            pound_out = sum([sumEdges(G, succ, direct=1) for succ in successors])
            if pound_out == 0 :
                continue
            #ratio between the weight of the edge between the current node and the current predecessor
            ratio = weight/pound_out
            #get the score of the current predecessor
            score_pred = nDict[pred]['score']
            #mult the score by the ratio
            score_ratio = ratio*score_pred
            #add the score ratio to the current score of the node
            currentScore += score_ratio
        #at this stage, we have the sum of all the ratio for the current node
        currentScore = (1-d) + d*currentScore
        #update the value of the node in the dict
        nDict[node]['score'] = currentScore
        if abs(currentScore-previousScore) <= threshold:
            nDict[node]['done'] = True

nodes = {key:nDict[key]['score'] for key in nDict.keys()}
nodes = sorted(nodes.items(), key=operator.itemgetter(1),reverse=True)

"""