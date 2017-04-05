from helper import MongoHelper as db
import csv
from tabulate import tabulate
from helper import AnnotationHelper, TextHelper
from StreamManager import  *
db.connect("tweets_dataset")
collection= "events_annotated_purge"
"""
Do not considere events with less than limit tweets
"""

def statCategory():
    rrr = gtEvents()
    rrr = {rr[0]:rr[1] for rr in rrr}
    print(rrr)
    pipeline = [
        {"$group": {"_id": "$categorie_text", "count": {"$sum":1}}}]
    res = db.aggregate(collection, pipeline)
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


def evaluation():

    scores = ['0.2']
    results = []
    categories = []
    headers = [['_']]
    for score in scores:
        corrects = set()
        gt, predicted, correct = 506, 0, 0
        headers[0].append("alpha={}".format(score))
        headers[0].append('')
        with open("results_{}_{}.csv".format(collection,score), encoding="utf8") as csvfile:
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
        cat = perCategory(collection,list(corrects))
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
