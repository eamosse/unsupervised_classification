from helper import MongoHelper as db
import csv
from tabulate import tabulate
db.connect("tweets_dataset")
"""
Do not considere events with less than limit tweets
"""
def generateGTData(limit=3):
    myfile = open('groundtruth.csv', 'w')
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(["Event", '#Tweets'])



def gtEvents(limit=30):
    res = []
    stat = db.stat('annotation_unsupervised')
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
    for key in res.keys():
        correct, total = len(res[key].intersection(corrects)), len(res[key])
        d = [key,total,correct,"%.3f" % (correct/total) if total > 0 else 0]
        final.append(d)

    print(tabulate(final, headers=["Category", "Events in Dataet", "Detected", "Recall"],tablefmt=_format))

def evaluation():

    gt,predicted,correct = 0,0,0
    corrects = []

    with open("results_10000_30_3_20_2.csv", encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        index = 0
        for r in reader:
            index+=1
            if index == 1:
                continue

            if len(r) >= 3:
                gt+=1
                predicted+=1
                correct+=1
                corrects.append(int(r[0]))
            if len(r) == 2:
                gt +=1
            if len(r) == 1:
                predicted+=1
    precision = correct/predicted
    recall = correct/gt
    fscore = 2*precision*recall/(precision+recall)
    print("Overall evaluation")
    print(tabulate([[gt, predicted, correct,"%.3f" %precision,"%.3f" %recall,"%.3f" %fscore]], headers=["Truth", "Predicted", "Correct", "Precision", "Recall", "F1"],tablefmt=_format))
    print("Evaluation per category")
    perCategory(corrects)



_format = "grid"
if __name__ == '__main__':
    evaluation()
