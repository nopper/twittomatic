"""
Simple script that extract a snapshot of random users from the dataset
"""

import os
import sys
import random
import tarfile
from optparse import OptionParser

class SnapshotExport(object):
    def __init__(self, dataset, num_users):
        self.dataset = dataset
        users = self.sample_users(num_users)

        with tarfile.open(mode='w|', fileobj=sys.stdout) as archive:
            for current, user_id in enumerate(users):
                archive.add(self.get_filename(user_id, 'twt'))
                archive.add(self.get_filename(user_id, 'fws'))

                sys.stderr.write("\r%d of %d processed [%02d%%]" % \
                    (current + 1, len(users), (current + 1) * 1.0 / len(users) * 100))
                sys.stderr.flush()

        sys.stderr.write("\nCompleted\n")
        sys.stderr.flush()

    def get_filename(self, user_id, suffix):
        return os.path.join(self.dataset, str(user_id)[:2], "%d." % user_id) + suffix

    def sample_users(self, num_users):
        results = []
        position = 0

        for dirpath, dirname, filenames in os.walk(self.dataset):
            for user_id in map(lambda x: int(x[:-4]),
                               filter(lambda x: x.endswith('twt'), filenames)):

                position += 1

                if len(results) < num_users:
                    if os.path.exists(self.get_filename(user_id, 'twt')) and \
                       os.path.exists(self.get_filename(user_id, 'fws')):
                        results.append(user_id)
                else:
                    j = random.randint(1, position)

                    if j < num_users and \
                       os.path.exists(self.get_filename(user_id, 'twt')) and \
                       os.path.exists(self.get_filename(user_id, 'fws')):
                        results[j] = user_id

        return results

if __name__ == "__main__":
    parser = OptionParser(usage="%s -t ~/twitter-ds -d 1000 > /tmp/sample-1000.tar" % sys.argv[0],
                          description="Extract the sample and create tarfile stream on stdout")
    parser.add_option("-d", "--dataset", action="store", type="string", dest="dataset")
    parser.add_option("-n", "--number", action="store", type="int", dest="number", default=0)

    (options, args) = parser.parse_args()

    if options.number > 0 and options.dataset:
        SnapshotExport(options.dataset, options.number)
    else:
        parser.print_help()