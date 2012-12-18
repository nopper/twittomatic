"""
Simple script to extract user informations from the dataset
"""

import os
import gzip
import json

class UserInfoExport(object):
    def __init__(self, dataset, outputfile):
        self.outputfile = outputfile
        self.dataset = os.path.abspath(dataset)
        self.userinfo = {}

    def extract_userinfo(self, user_id, filename, opener=open):
        try:
            with opener(filename, 'r') as input:
                for line in input:
                    tweet = json.loads(line.strip())
                    self.userinfo[user_id] = tweet['user']
                    break
        except Exception, exc:
            print "Error while reading %s [%s]" % (filename, str(exc))

    def run(self):
        for root, dirnames, filenames in os.walk(self.dataset):
            for filename in filenames:
                if filename.endswith('.twt'):
                    self.extract_userinfo(int(filename[:-4]),
                                          os.path.join(root, filename),
                                          opener=open)
                elif filename.endswith('.twt.gz'):
                    self.extract_userinfo(int(filename[:-7]),
                                          os.path.join(root, filename),
                                          opener=gzip.open)

        with gzip.open(self.outputfile, 'w') as output:
            for user_id, dct in sorted(self.userinfo.items()):
                output.write(json.dumps(dct, sort_keys=True) + '\n')

        print "%d user informations collected" % len(self.userinfo)


if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(description="Create a representative json file containing user informations")
    parser.add_option("-d", "--dataset", dest="dataset",
                      help="Dataset where the .twt or .twt.gz files reside")
    parser.add_option("-o", "--output", dest="output",
                      help="Output file (.json.gz)")

    (options, args) = parser.parse_args()

    if options.dataset and options.output:
        app = UserInfoExport(options.dataset, options.output)
        app.run()
    else:
        parser.print_help()