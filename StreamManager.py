from helper import MongoHelper as db
import csv
import random
db.connect("tweets_dataset")

collection = "events_annotated_purge"

dirty = []
max = 1233200
current = 0
ne = 10000
interval = 1

groups = db.intervales(collection, param="hour", interval=interval)


def dirtyTweets(nb):
    global current
    if current >= max:
        current = 0
    non_events = db.find("nevents", limit=ne, skip=current)
    current = nb + current
    """random.shuffle(non_events)
    random.shuffle(non_events)
    random.shuffle(non_events)
    random.shuffle(non_events)"""
    return non_events

def nextBatch():
    if groups:
        group = groups.pop(0)
        tweets = db.find(collection, query={"id": {"$in": group['data']}})
        non_events = dirtyTweets(ne)
        data = tweets + non_events
        group['data'] = data
        return group
    return None

def generateGTData(limit=3):
    myfile = open('groundtruth.csv', 'w')
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(["Event", '#Tweets'])

def gtEvents(limit=1):
    res = []
    stat = db.stat(collection)
    for s in stat:
        if len(s['data']) >= limit:
            res.append([s['_id']['event_id'], len(s['data'])])
    return res


def perCategory(corrects):
    corrects = set(corrects)
    pipeline = [
        {"$group": {"_id": {"category": "$categorie_text"},
                    "data": {"$addToSet": {'event_id': '$event_id'}}}}]
    res = db.aggregate("category", pipeline)
    gts = gtEvents()
    gts = set([gt[0] for gt in gts])
    print("GTS", len(gts))
    res = {r["_id"]["category"]:set([rr['event_id'] for rr in r["data"]]).intersection(gts) for r in res}
    final = []
    keys = sorted(res.keys(), reverse=False)
    for key in keys:
        correct, total = len(res[key].intersection(corrects)), len(res[key])
        recall = "%.3f" % (correct/total) if total > 0 else 0
        d = [key,"E: {} \linebreak[1] R: {}".format(correct,recall) ]
        final.append(d)
    return final

if __name__ == '__main__':

    while True:
        data = nextBatch()
        if not  data:
            break

        print(data['day'])

