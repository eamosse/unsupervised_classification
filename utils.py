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

def evaluation():
    gt,predicted,correct = 0,0,0

    with open("results.csv", encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for r in reader:
            if len(r) >= 3:
                gt+=1
                predicted+=1
                correct+=1
            if len(r) == 2:
                gt +=1
            if len(r) == 1:
                predicted+=1
    precision = correct/predicted
    recall = correct/gt
    fscore = 2*precision*recall/(precision+recall)
    print(tabulate([[gt, predicted, correct]], headers=["Truth", "Predicted", "Correct"]))
    print(tabulate([[precision,recall,fscore]], headers=["Precision", "Recall", "F1"]))


if __name__ == '__main__':
    evaluation()