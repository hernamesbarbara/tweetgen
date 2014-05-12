#!/usr/bin/env python
# -*- coding: utf-8 -*- 
"""find_all_retweeters.py
"""
import os
import sys
import numpy as np
import pandas as pd
import json
import psycopg2 as pg
from pymongo import MongoClient
import twitter
from twitterapi import authenticate_from_file
from twitterapi import make_twitter_request
from twitterapi import log_msg

MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017')

def get_retweeters(status_id, api):
    return make_twitter_request(api.statuses.retweeters.ids, _id = status_id)

twitter_api = authenticate_from_file('./yhat_secrets.json')
tweet_ids = open('./tweet_ids.csv').read().split('\n')
db = MongoClient(MONGO_URI)['yhtweets']


for i, tweet_id in enumerate(tweet_ids):
    if not tweet_id:
        continue
    tweet_id = str(tweet_id)
    if i % 10 == 0:
        print >> sys.stdout, log_msg("{} of {}".format(i, len(tweet_ids)))
    if not db.retweeters.find_one({"status_id": tweet_id}):
        rts = get_retweeters(tweet_id, twitter_api)
        if rts:
            new_doc = {
                "status_id": tweet_id,
                "retweeter_ids": map(str, rts['ids'])
            }
            db.retweeters.save(new_doc)

