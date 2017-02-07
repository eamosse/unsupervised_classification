from helper import MongoHelper as db, AnnotationHelper
from NetworkxHelper import *

db.connect("tweets_dataset")

def generateDefinition(ids):
    tweets = db.find(collection="annotation_unsupervised", query={'id':{'$in':ids}})
    initialGraph = nx.DiGraph()

    for tweet in tweets:
        edges = AnnotationHelper.getNodes(text=tweet['text'])
        for e in edges:
            addEdge(initialGraph,e[0],e[1],tweet['id'])
    degree = degrees(initialGraph)
    print(degree)
    #display(initialGraph)


if __name__ == '__main__':
    ids = ['255868574692941825', '255887289710940160', '255910819748007936', '255922299696463872', '255886094531448833', '255910408903352320', '255896295078776832', '255923608482881536', '255849486612561920', '255845158090862594', '255902196581945344', '255907514825183232', '255874597931593728', '255902225786863618', '255867689954848768', '255842566090670081', '255854549150089217', '255885482221780992', '255917086356938752', '255863319506866178', '255842985302978560', '255902079007211520', '255873188431556608', '255884089754783744', '255908949155524608', '255908978733752320', '255844579356594176', '255854549049421824', '255863537379983361', '255910987629228032', '255821967670779904', '255916826242985984', '255886526276317184', '255913940331147264', '255921859382624256']
    generateDefinition(ids)
