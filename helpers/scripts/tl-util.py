"""
Simple utility to manage with twt files
"""

import json
import gzip
from optparse import OptionParser

def cat(filename, expression):
    with gzip.open(filename, 'r') as input:
        for line in input:
            tweet = json.loads(line.strip())
            result = []

            for subexpr in expression.split(','):
                obj = tweet
                for param in subexpr.split('/'):
                    obj = obj.get(param, '')
                result.append(unicode(obj))

            print ('\t'.join(result)).encode('utf8')

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-e", "--expression", dest="expression", default="created_at,user/screen_name,text",
                      help="Expression")

    (options, args) = parser.parse_args()

    if len(args) == 1:
        cat(args[0], options.expression)
    else:
        parser.print_help()
