"""
Simple script that extracts useful information from a wikipedia mirror.
Please be sure to setup your own mirror and to not launch this script
against the real wikipedia.
"""

import sys
import gzip
import json
import mwclient

class TitlesExtractor(object):
    """
    This class simply extracts a list of wikipedia pages directly from
    wikipedia.  You should end up with a complete list in less than an hour.
    """
    def __init__(self, hostname, path='/w/'):
        self.site = mwclient.Site(hostname, path)

    def save(self, filename):
        print "Requesting a list of pages from wikipedia"

        with gzip.open(filename, 'w') as output:
            for count, page in enumerate(self.site.allpages()):
                output.write(json.dumps({
                    'id': page._info['pageid'],
                    'ns': page._info['ns'],
                    'revid': page._info['lastrevid'],
                    'length': page._info['length'],
                    'touched': page._info['touched'],
                    'title': page.page_title,
                    'name': page.name
                }, sort_keys=True) + '\n')

                sys.stdout.write("%d pages extracted\r" % (count + 1))
                sys.stdout.flush()

        sys.stdout.write("%d pages extracted\n" % (count + 1))
        sys.stdout.flush()

class HTMLExtractor(object):
    """
    This class request the rendered HTML for every page you have downloaded
    with the TitleExtractor. Better split your input in several files and run
    this class in parallel. With 10 workers you should scrape the entire
    italian wikipedia in lass than 24 hours.

    To split the input just run:

        $ zcat wikipedia-titles.json.gz | split -l 100000 - wikipedia-
    """
    def __init__(self, hostname, path='/w/'):
        self.site = mwclient.Site(hostname, path)

    def parse_block(self, blockname, restart=0):
        with open(blockname, 'r') as input:
            with gzip.open(blockname + '.json.gz', 'a') as output:
                for count, line in enumerate(input):
                    if restart > count:
                        continue

                    page = json.loads(line.strip())

                    limit = 3
                    attempt = 0

                    while attempt <= limit:
                        try:
                            attempt += 1
                            print "Requesting %s requests made [%d to restart]" % (page['name'].encode('utf8'), count)
                            result = self.site.api('parse', page=page['name'])
                            html = result['parse']['text']['*']
                            page['html'] = html

                            output.write(json.dumps(page, sort_keys=True) + '\n')
                            break
                        except Exception, exc:
                            print "Got an error %s. Retry %d of %d" % (str(exc), attempt, limit)

                            if attempt == limit:
                                raise exc

class TemplateExtractor(object):
    def __init__(self, hostname, path='/w/'):
        self.site = mwclient.Site(hostname, path)

    def parse_block(self, blockname, restart=0):
        with open(blockname, 'r') as input:
            with gzip.open(blockname + '-templates.json.gz', 'a') as output:
                for count, line in enumerate(input):
                    if restart > count:
                        continue

                    info = json.loads(line.strip())
                    page = mwclient.page.Page(self.site, info['name'], info)

                    limit = 3
                    attempt = 0

                    while attempt <= limit:
                        try:
                            attempt += 1
                            print "Requesting %s requests made [%d to restart]" % (info['name'].encode('utf8'), count)
                            info['templates'] = map(lambda page: {
                                'id': page._info['pageid'],
                                'ns': page._info['ns'],
                                'revid': page._info['lastrevid'],
                                'length': page._info['length'],
                                'touched': page._info['touched'],
                                'title': page.page_title,
                                'name': page.name
                            }, [t for t in page.templates()])

                            output.write(json.dumps(info, sort_keys=True) + '\n')
                            break
                        except Exception, exc:
                            print "Got an error %s. Retry %d of %d" % (str(exc), attempt, limit)

                            if attempt == limit:
                                pass

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-t", "--tool", dest="tool",
                      help="Tool to use [title, html, templates]")
    parser.add_option("-r", "--restart", dest="restart", type="int", default=0,
                      help="Restart from line (default: 0)")

    (options, args) = parser.parse_args()

    if options.tool == 'title':
        app = TitlesExtractor('it.wikipedia.org')
        app.save(args[0])
    elif options.tool == 'html':
        app = HTMLExtractor('it.wikipedia.org')
        app.parse_block(args[0], options.restart)
    elif options.tool == 'templates':
        app = TemplateExtractor('it.wikipedia.org')
        app.parse_block(args[0], options.restart)
    else:
        parser.print_help()
