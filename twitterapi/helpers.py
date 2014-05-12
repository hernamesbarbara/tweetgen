#!/usr/bin/env python
# -*- coding: utf-8 -*- 
"""twitterapi.py
"""

import sys
import os
import time
from datetime import datetime
import string
from urllib2 import URLError 
from httplib import BadStatusLine
import ujson as json
import twitter
from twitter import TwitterHTTPError

LOG_FORMAT = "{TIMESTAMP}: {MSG}"

def log_msg(msg):
    timestamp = datetime.utcnow()
    return LOG_FORMAT.format(TIMESTAMP=timestamp, MSG=msg)

def make_twitter_request(api_func, max_errors=10, *args, **kwargs):

    def handle_twitter_errors(err, wait_period=2, sleep_when_rate_limited=True):
        if wait_period > 3600:
            print >> sys.stderr, log_msg("Too many retries. Exiting.")
            raise err
        if err.e.code == 401:
            print >> sys.stderr, log_msg('Encountered 401 Error (Not authorized)')
            return None
        elif err.e.code == 404:
            print >> sys.stderr, log_msg('Encountered 404 Error (Not Found)')
            return None
        elif err.e.code == 429:
            print >> sys.stderr, log_msg('Encountered 429 Error (Rate Limit Exceeded)')
            if sleep_when_rate_limited:
                print >> sys.stderr, log_msg('Retrying in 15 minutes...ZzZ...')
                sys.stderr.flush()
                time.sleep(60*15+5)
                print >> sys.stderr, log_msg('...ZzZ...Awake now and trying again.')
                return 2
            else:
                # if sleep_when_rate_limited == False, caller
                # must handle rate limit error manually
                raise err 
        elif err.e.code in (500, 502, 503, 504):
            msg = "Encountered {} Error. Retrying in {} seconds."
            print >> sys.stderr, log_msg(msg.format(err.e.code, wait_period))
            time.sleep(wait_period)
            wait_period *= 1.5
            return wait_period
        else:
            raise err
    wait_period = 2
    error_count = 0
    while True:
        try:
            return api_func(*args, **kwargs)
        except TwitterHTTPError, err:
            error_count = 0
            wait_period = handle_twitter_errors(err, wait_period)
            if not wait_period:
                return 
        except URLError, err:
            error_count += 1
            print >> sys.stderr, log_msg("URLError Encountered. Continuing.")
            if error_count > max_errors:
                print >> sys.stderr, log_msg("Too many consecutive errors. Bailing out.")
                raise
        except BadStatusLine, err:
            error_count += 1
            print >> sys.stderr, log_msg("BadStatusLine encountered. Continuing.")
            if error_count > max_errors:
                print >> sys.stderr, log_msg("Too many consecutive errors. Bailing out.")
                raise

def oauth_login(oauth_token, oauth_token_secret, consumer_key, consumer_secret):
    auth = twitter.oauth.OAuth(
        oauth_token, 
        oauth_token_secret, 
        consumer_key, 
        consumer_secret
    )
    return twitter.Twitter(auth=auth)


def authenticate_from_file(filename):
    creds = json.load(open(filename))
    return oauth_login(**creds)

def rm_weird_chars(txt):
    return "".join(ch for ch in txt if ch in string.printable)

