"""
Simple flask application that emulates how twitter behaves.
It takes as input a directory where it will find a small number of user
profiles from which to load information
"""

from flask import *
from twitter.settings import LOOKUP_DATABASE, LOOKUP_PORT, LOOKUP_URL

app = Flask(__name__)

dataset_timeline = {}
dataset_followers = {}

DEBUG = True

import os
import sys
import json
import gzip
import glob
import time
import struct

def load_tweets(ds, user_id, filename):
    if filename.endswith('.gz'):
        opener = gzip.open
    else:
        opener = open

    with opener(filename, 'r') as input:
        data = input.readlines()
        ds[user_id] = data

def load_followers(ds, user_id, filename):
    followers = []

    if filename.endswith('.gz'):
        opener = gzip.open
    else:
        opener = open

    with opener(filename, 'r') as input:
        while True:
            data = input.read(struct.calcsize("!Q"))

            if not data:
                break

            followers.append(struct.unpack("!Q", data)[0])

        ds[user_id] = followers

def load_dataset_from(directory):
    print "Loading data from", directory

    global dataset_timeline
    global dataset_followers

    for count, filename in enumerate(glob.glob(os.path.join(directory, '*/*'))):
        try:
            name = os.path.basename(filename)
            user_id = int(name[:name.index('.')])
        except:
            continue

        if filename.endswith('.twt') or filename.endswith('.twt.gz'):
            load_tweets(dataset_timeline, user_id, filename)
        elif filename.endswith('.fws') or filename.endswith('.fws.gz'):
            load_followers(dataset_followers, user_id, filename)

    print "Dataset successfully loaded. %d files loaded." % count

def get_timeline(user_id, max_id=-1, since_id=-1):
    response = []
    for line in dataset_timeline[user_id]:
        tweet = json.loads(line)
        if max_id == -1 and since_id == -1:
            response.append(tweet)
        else:
            if max_id != -1 and since_id != -1:
                if int(tweet['id_str']) <= max_id and int(tweet['id_str']) > since_id:
                    print since_id, tweet['id_str'], max_id
                    response.append(tweet)
            elif max_id != -1:
                if int(tweet['id_str']) <= max_id:
                    response.append(tweet)
            elif since_id != -1:
                if int(tweet['id_str']) > since_id:
                    response.append(tweet)
        if len(response) >= 200:
            break

    return response

def get_followers(user_id, cursor=0):
    response = []

    for count, line in enumerate(dataset_followers[user_id]):
        if len(response) >= 500:
            break
        if count >= cursor:
            response.append(line)

    if count == len(dataset_followers[user_id]) - 1:
        count = 0

    return {'ids': response, 'next_cursor_str': count}

remaining = 1

def set_rate(response):
    global remaining

    response.headers['x-ratelimit-remaining'] = remaining
    response.headers['x-ratelimit-reset'] = int(time.time() + 2)
    remaining -= 1

    if remaining == -1:
        response.data = json.dumps([])
        remaining = 100

    return response

@app.route('/1/statuses/user_timeline.json')
def lookup_by_userid():
    max_id = request.args.get('max_id', '')
    since_id = request.args.get('since_id', '')
    user_id = request.args.get('user_id', '')

    # TODO: Aggiungi restrizioni su IP

    try:
        since_id = int(since_id)
    except:
        since_id = -1

    try:
        max_id = int(max_id)
    except:
        max_id = -1

    if not user_id:
        return Response('', status=404, mimetype='application/json')

    try:
        data = get_timeline(int(user_id), max_id, since_id)
        return set_rate(Response(json.dumps(data), status=200, mimetype='application/json'))
    except KeyError:
        return set_rate(Response('', status=404, mimetype='application/json'))

@app.route('/1/followers/ids.json')
def get_followers_ids():
    user_id = request.args.get('user_id', '')
    cursor  = request.args.get('cursor', '')

    try:
        cursor = int(cursor)
    except:
        cursor = 0

    try:
        data = get_followers(int(user_id), cursor)
        return set_rate(Response(json.dumps(data), status=200, mimetype='application/json'))
    except KeyError:
        return set_rate(Response('', status=404, mimetype='application/json'))

@app.route('/1/users/lookup.json', methods=['POST'])
def lookup_users():
    lookup = []
    user_ids = request.form.get('user_id', '')

    for user_id in user_ids.split(','):
        # Randomly generate a lookup information
        lookup.append({
            'lang': 'en',
            'statuses_count': 200,
            'screen_name': 'testing',
            'id_str': user_id
        })

    return set_rate(Response(json.dumps(lookup), status=200, mimetype='application/json'))


if __name__ == '__main__':
    load_dataset_from(sys.argv[1])
    app.run(debug=True)
