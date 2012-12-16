import sys
from json import loads
from random import randint

SAMPLE_COUNT = 10000
NUM_REDUCERS = 12

sample = []

for idx, line in enumerate(sys.stdin):
    tweet = loads(line.strip())
    tweet_id = int(tweet['id_str'])

    if len(sample) < SAMPLE_COUNT:
        sample.append(tweet_id)
    else:
        r = randint(0, idx)

        if r < SAMPLE_COUNT:
            sample[r] = tweet_id

sample.sort()
step_size = len(sample) / float(NUM_REDUCERS)
last = -1

for i in xrange(1, NUM_REDUCERS):
    k = int(step_size * i)

    while (last >= k and sample[last] == sample[k]):
        k += 1

    print sample[k]
    last = k
