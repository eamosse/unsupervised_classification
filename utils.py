from helper import MongoHelper as db
import csv
from tabulate import tabulate
db.connect("tweets_dataset")
collection= "events_annotated"
"""
Do not considere events with less than limit tweets
"""
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


def statCategory():
    rrr = gtEvents()
    rrr = {rr[0]:rr[1] for rr in rrr}
    print(rrr)
    pipeline = [
        {"$group": {"_id": "$categorie_text", "count": {"$sum":1}}}]
    res = db.aggregate("annotation_unsupervised", pipeline)
    res = {r['_id']:{'tweets' :r['count']} for r in res}

    pipeline = [
        {"$group": {"_id": "$categorie_text",
                    "data": {"$addToSet": {'event_id': '$event_id'}}}}]
    res2 = db.aggregate(collection, pipeline)
    for r in res2:
        res[r['_id']]['events'] = len(r['data'])

    print(res)

    keys = sorted(res.keys())
    rr = []
    for k in keys:
        rr.append([k, res[k]['events'],res[k]['tweets']])
    tEvent = sum([t[1] for t in rr])
    rr.append(["Total", sum([t[1] for t in rr]),sum([t[2] for t in rr])])
    print(
        tabulate(rr, headers=["Category", "Events", "Tweets"], tablefmt=_format))




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

def evaluation():



    scores = [0.001,0.0015,0.0020,0.0025]
    results = []
    categories = []
    headers = [['_']]
    for score in scores:
        corrects = set()
        gt, predicted, correct = 506, 0, 0
        headers[0].append("alpha={}".format(score))
        headers[0].append('')
        with open("results_1_2_{}.csv".format(score), encoding="utf8") as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            index = 0

            for r in reader:
                index+=1
                if index == 1:
                    continue

                if len(r) >= 3:
                    predicted+=1
                    correct+=1
                    corrects.add(int(r[0]))
                if len(r) == 1:
                    predicted+=1
        precision = correct/predicted
        recall = correct/gt
        fscore = 2*precision*recall/(precision+recall)
        results.append(["alpha={}".format(score), gt, predicted, correct,"%.3f" %precision,"%.3f" %recall,"%.3f" %fscore])
        cat = perCategory(list(corrects))
        if not categories:
            categories.extend(cat)
        else:
            for i in range(len(cat)):
                categories[i].extend(cat[i][1:])

    print("Overall evaluation")
    print(tabulate(results, headers=["Score","Truth", "Predicted", "Correct", "Prec.", "Rec.", "F1"],tablefmt=_format))
    print("Evaluation per category")
    categories = headers + categories
    print(tabulate(categories, tablefmt=_format))


_format = "grid"
if __name__ == '__main__':
    evaluation()
    #statCategory()
