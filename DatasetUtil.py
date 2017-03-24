import csv
from datetime import datetime, date
from helper import  MongoHelper as db
from dateutil import parser as pp

import helper
from optparse import OptionParser
#locale.setlocale(locale.LC_ALL, "en_EN")
log = helper.enableLog()
import os
import time
#sys.stdout = open('output.csv', "w",encoding="utf8")

db.connect("tweets_dataset")
ids = {}

def duplicate(_from,to,what) :
    print(_from, to, what)
    db.connect(_from)
    data = db.find(what)
    print(data)
    db.connect(to)
    db.insert(what,data)

def snowflake2utc(sf):
    return ((sf >> 22) + 1288834974657) / 1000.0

def loadIs(file):
    global ids
    ids ={}
    with open(file, encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for t in reader:
            ids[t[1]] = t[0]
    return ids


def reconciliate():
    maps = {}
    with open("relevant_tweets.tsv", encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for r in reader:
            maps[r[1]] = r[0]

    with open("all_tweets.txt", encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        tweets = []
        for t in reader:
            res = snowflake2utc(int(t[1]))
            if t[1] in maps:
                event = db.find("category", query={"event_id": int(maps[t[1]])})[0]
                tweet = {'text' : t[4],
                         'id': t[1],
                         'date' : pp.parse(time.ctime(int(res))),
                         'dataset' : 'event 2012',
                         'event_id' : event['event_text'],
                         'event_text': event['event_text'],
                         'categorie_text': event['categorie_text']
                         }
                tweets.append(tweet)

    db.insert("events", tweets)

def update():
    db.remove("all_tweets", {'event_id':-1})
    db.connect("event_2012")
    tweets  = db.find("annotation_unsupervised")
    db.connect("tweets_dataset")
    db.insert("all_tweets", tweets)
    db.connect("tweets_dataset")

def saveRelevent(file):
    with open(file, encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for t in reader:
            event = db.find("category", query={"event_id":int(t[0])})
            if len(event) >0 :
                del event[0]['_id']
                db.update("annotation_unsupervised",condition={"id":t[1]}, value=event[0])

def clean():
    db.connect("tweets_dataset")
    limit, skip = 400, 0
    while True:
        res = list(db.find("annotation_python", limit=limit, skip=skip))
        if not res:
            break
        skip+=limit
        for r in res:
            annoations = r['annotations']
            if not annoations:
                continue
            ann = []
            found = False
            for i, a in enumerate(annoations):
                for j, b in enumerate(annoations):
                    if j==i:
                        continue
                    if (a['startChar'] >= b['startChar'] and a['endChar'] <= b['endChar']):
                        found = True
                        break
                if not found:
                    ann.append(a)
                found = False
            r['annotations'] = ann
        db.insert("annotation_purge", res)

def removeDupllicate():
    observed = []
    tweets = db.find("non_event")
    tweets = sorted(tweets, key=lambda k: len(k['text']), reverse=True)
    for t in tweets:
        vals = [i for i in observed if i['text'] == t['text'] or t['text'] in i['text']]
        if not vals:
            observed.append(t)
    db.insert("non_event_tweets",observed)

def parseFile(folder):
    for f in os.listdir(folder):
        log.debug("Parsing file {}".format(f))
        ignored = 0
        data = []
        with open(folder+'/'+f, 'r', encoding="utf8") as csvfile:
            spamreader = csv.reader(csvfile, delimiter='\t')
            for s in spamreader:
                if '200	false' in s[8] or str(s[0]).startswith('400') or '-' not in s[9]:
                    ignored+=1
                    continue
                s[9] = s[9].replace('avr.', 'avril')
                s[9] = s[9].replace('Oct', 'oct.')
                s[9] = s[9].replace('Aug', 'août')

                #7:01 - 9 oct. 2012
                _date, _hour = s[9].split(' - ')[1], s[9].split(' - ')[0]
                d,m,y = _date.split()[0],_date.split()[1],_date.split()[2]

                if str(m).lower().startswith('av') or str(m).lower().startswith('ap'):
                    m = 'April'
                if str(m).lower().startswith('au') or str(m).lower().startswith('ao'):
                    m = 'August'
                if str(m).lower().startswith('juil') or str(m).lower().startswith('jul'):
                    m = 'July'
                if str(m).lower().startswith('juin') or str(m).lower().startswith('jun'):
                    m = 'June'
                if str(m).lower().startswith('s'):
                    m = 'September'
                if str(m).lower().startswith('o'):
                    m = 'October'
                if str(m).lower().startswith('n'):
                    m = 'November'
                if str(m).lower().startswith('d'):
                    m = 'December'
                if str(m).lower().startswith('f'):
                    m = 'February'
                if str(m).lower().startswith('ja'):
                    m = 'January'
                if str(m).lower().startswith('mar'):
                    m = 'March'
                if str(m).lower().startswith('may') or str(m).lower().startswith('mai'):
                    m = 'May'

                _date = '{} {} {}'.format(d,m,y)

                if 'PM' in _hour:
                    _hour = _hour.replace('PM', '')
                    _h,_m = int(_hour.split(":")[0])+12, _hour.split(":")[1]
                    if _h >= 24:
                        _h = 00
                    _hour = '{}:{}'.format(_h,_m)
                _hour = _hour.replace('AM', '').strip()
                fDate = '{} {}'.format(_date,_hour)
                dd = pp.parse(fDate)#datetime.strptime(fDate,'%d %b %Y %H:%M')
                d = {'text':s[8], 'tweet_id':s[6], 'date':dd}
                if s[6] in ids:
                    d['event_id'] = ids[s[6]]
                data.append(d)
        os.remove(folder+'/'+f)
        log.debug("Inserting tweets in the database")
        print("Inserted {} tweets".format(len(data)))
        print("Ignored {} tweets".format(ignored))
        if len(data) > 0:
            db.insert("tweets", data)


if __name__ == '__main__':
    update()

    #saveRelevent(opts.relevant)
    #parseFile(opts.folder)



