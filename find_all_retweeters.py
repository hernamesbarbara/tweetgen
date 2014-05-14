#!/usr/bin/env python
# -*- coding: utf-8 -*- 
"""find_all_retweeters.py
"""
import os
import sys
import json
from pymongo import MongoClient
import twitter
from twitterapi import authenticate_from_file
from twitterapi import make_twitter_request
from twitterapi import log_msg

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017')
db = MongoClient(MONGO_URI)['yhtweets']

FMT_STATUS_URL = "https://twitter.com/yhathq/status/{status_id}"

def get_retweeters_details(status_id, api):
    return make_twitter_request(api.statuses.retweets.id, id=status_id)

def find_by_status_id(status_id):
    return db.retweeters.find_one({"status_id": status_id})

def find_by_object_id(object_id):
    return db.retweeters.find_one({"_id": object_id})

def has_retweeter_details(doc):
    return "retweeter_details" in doc.keys()

twitter_api = authenticate_from_file('./yhat_secrets.json')
tweet_ids = open('./tweet_ids.csv').read().split('\n')

for i, tweet_id in enumerate(tweet_ids):
    if i % 10 == 0:
        print log_msg("{} of {}".format(i, len(tweet_ids)))
        print log_msg("{} records in the database".format(db.retweeters.count()))
    if not tweet_id:
        continue
    tweet_id = str(tweet_id)
    rted_by = find_by_status_id(tweet_id)
    if rted_by is None:
        rted_by = get_retweeters_details(tweet_id, twitter_api)
        rted_by = {
            "status_id": tweet_id,
            "retweeters": rted_by
        }
        rted_by = db.retweeters.save(rted_by)
