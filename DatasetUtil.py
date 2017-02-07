import csv
from datetime import datetime, date
from helper import  MongoHelper as db
from dateutil import parser as pp

import helper
from optparse import OptionParser
#locale.setlocale(locale.LC_ALL, "en_EN")
log = helper.enableLog()
import os
#sys.stdout = open('output.csv', "w",encoding="utf8")

db.connect("event_2012")
ids = {}

def loadIs(file):
    global ids
    ids ={}
    with open(file, encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        for t in reader:
            ids[t[1]] = t[0]
    return ids

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

        log.debug("Inserting tweets in the database")
        print("Inserted {} tweets".format(len(data)))
        print("Ignored {} tweets".format(ignored))
        db.insert("tweets", data)


if __name__ == '__main__':
    parser = OptionParser('''%prog -o ontology -t type -f force ''')
    parser.add_option('-r', '--relevant', dest='relevant', default="relevant_id.tsv")
    parser.add_option('-f', '--folder', dest='folder', default="part")
    opts, args = parser.parse_args()

    loadIs(opts.relevant)
    parseFile(opts.folder)

    fDate = '1 November 2012 0:09'
    dd = pp.parse(fDate)
    print(dd)



