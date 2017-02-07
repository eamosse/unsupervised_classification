from helper import MongoHelper as db
db.connect("tweets_dataset")
def generateDefinition(ids):
    tweets = db.find(collection="annotation_unsupervised", query={'id':{'$in':ids}})
