#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import cgi
import gzip
import re
import requests
from json import loads
from portals import PortalExtractor
from annotation import AnnotationExtractor
from hashtag import HashtagExtractor

HTML_PAGE = """<html>
<head>
<script src="http://ajax.googleapis.com/ajax/libs/jquery/1.8.0/jquery.min.js" type="text/javascript"></script>
<script>
   	$(document).ready(function() {
   		$('body').append('<div id="anchorTitle" class="anchorTitle1"></div>');
   		$('a[title!=""]').each(function() {
   			var a = $(this);
   			a.hover(
   				function() {
   					showAnchorTitle(a); 
   				}, 
   				function() { 
   					hideAnchorTitle();
   				}
   			)
   			.data('title', a.attr('title'))
   			.removeAttr('title');
   		});
        $('a').each(function() {
            var rho = parseFloat($(this).attr('data-rho'));
            var fontsize = 72 * rho;
            $(this).css('fontSize', fontsize);
        });
   		$('input[name=styleSwitcher]').change(function() {
   			$('#anchorTitle').toggleClass('anchorTitle1').toggleClass('anchorTitle2');
   			return false;
   		});
   	});

   	function showAnchorTitle(element) {
   		var offset = element.offset();
        var text =
           "<p>Wikipedia page: " + element.data('title') + "</p>" +
           "<p>Rho factor: " + element.data('rho') +       "</p>";
        $(anchorTitle).css({ 
   			'top'  : (offset.top + element.outerHeight() + 4) + 'px',
   			'left' : offset.left + 'px'
   		})
   		.html(text)
   		.show();
   	}

   	function hideAnchorTitle() {
   		$('#anchorTitle').hide();
   	}
</script>
<style>
	body {
		background-color: white;
		font-family: Helvetica, Arial, sans-serif;
		font-size: 14px;
	}
	/* normally one of these would be #anchorTitle instead of having two classes
	 * with many of the properties the same; for the purposes of this demo we have
	 * two styles which can be toggled by clicking the radio button
	 */
	.anchorTitle1 {
		/* border radius */
		-moz-border-radius: 8px;
		-webkit-border-radius: 8px;
		border-radius: 8px;
		/* box shadow */
		-moz-box-shadow: 2px 2px 3px #e6e6e6;
		-webkit-box-shadow: 2px 2px 3px #e6e6e6;
		box-shadow: 2px 2px 3px #e6e6e6;
		/* other settings */
		background-color: #fff;
		border: solid 3px #d6d6d6;
		color: #333;
		display: none;
		font-family: Helvetica, Arial, sans-serif;
		font-size: 11px;
		line-height: 1.3;
		max-width: 200px;
		padding: 5px 7px;
		position: absolute;
	}
	.anchorTitle2 {
		/* set background-color for browsers that don't support gradients as fallback, then define gradient */
		background-color: #888;
	    background:-webkit-gradient(linear, left top, left bottom, from(#777), to(#999));
		background:-moz-linear-gradient(top, #777, #999) !important;
		background:-o-linear-gradient(top, #777, #999) !important;
		background:linear-gradient(top, #777, #999) !important;
		filter: progid:DXImageTransform.Microsoft.gradient(startColorstr='#777777', endColorstr='#999999') !important;
		/* border radius */
		-moz-border-radius: 8px;
		-webkit-border-radius: 8px;
		border-radius: 8px;
		/* box shadow */
		-moz-box-shadow: 2px 2px 3px #bbb;
		-webkit-box-shadow: 2px 2px 3px #bbb;
		box-shadow: 2px 2px 3px #bbb;
		/* other settings */
		border: solid 2px #666;
		color: #fff;
		display: none;
		font-family: Helvetica, Arial, sans-serif;
		font-size: 11px;
		line-height: 1.3;
		max-width: 200px;
		padding: 5px 7px;
		position: absolute;
	}
	* html #anchorTitle {
		/* IE6 does not support max-width, so set a specific width instead */
		width: 200px;
	}

    .tweet {
        border: solid 1px #666;
        background-color: #ddd;
        font-size: 12px;
        font-family: Trebuchet, Helvetica, Arial, sans-serif;
        padding: 5px 5px;
        -moz-box-shadow: 2px 2px 3px #e6e6e6;
        -webkit-box-shadow: 2px 2px 3px #e6e6e6;
        box-shadow: 2px 2px 3px #e6e6e6;
        -moz-border-radius: 8px;
        -webkit-border-radius: 8px;
        border-radius: 8px;
        background:-webkit-gradient(linear, left top, left bottom, from(#fff), to(#ddd));
        margin-bottom: 12px;
    }

    .tweet .categories {
        font-size: 10px;
        font-style: italic;
        font-family: Helvetica, Arial, sans-serif;
        border-bottom: 1px solid;
    }
</style>
<body>
"""

class HTMLRenderer(object):
    def __init__(self):
        self.extractor = AnnotationExtractor()
        self.hastag = HashtagExtractor()
        self.portal = PortalExtractor()

    def run(self, inputfile, outputfile):
        with gzip.open(inputfile, 'r') as input:
            with open(outputfile, 'w') as output:
                output.write(HTML_PAGE)

                for line in input:
                    tweet = loads(line)
                    #output.write("<p>%s</p>" % tweet['text'].encode('ascii', 'xmlcharrefreplace'))

                    text = self.hastag.sanitize(tweet['text'])
                    annotations = self.extractor.annotate(text, is_tweet=False, raw=True)

                    output.write("<div class='tweet'>")

                    # Try a simple categorization
                    output.write("<div class='categories'>")
                    categories = self.portal.categories(map(lambda x: int(x['id']), annotations))

                    if categories:
                        output.write((', '.join(categories)).encode('ascii', 'xmlcharrefreplace'))
                    else:
                        output.write('No categories found')

                    output.write("</div>\n")

                    output.write("<div class='text'>")
                    output.write(self.render_single(text, annotations))
                    output.write("</div>\n")

                    output.write("</div>\n")

                output.write("</body>")

    def render_single(self, text, annotations):
        html = ""
        prev = 0

        #text = text.encode('utf8')
        #print repr(text)

        for annotation in annotations:
            start, stop = annotation['start'], annotation['end']
            #print start, stop
            #rho, id = annotation['rho'].encode('utf8'), annotation['id']
            #spot, title = annotation['spot'].encode('utf8'), annotation['title'].encode('utf8')
            rho, id = annotation['rho'], annotation['id']
            spot, title = annotation['spot'], annotation['title']

            html += text[prev:start]
            html += "<a href='#' data-spot='%s' data-title='%s' data-rho='%s'>%s</a>" % \
                    (spot, title, rho, text[start:stop])
            prev = stop

        html += text[prev:]

        #return html.decode('utf8', 'ignore').encode('ascii', 'xmlcharrefreplace')
        return html.encode('ascii', 'xmlcharrefreplace')


if __name__ == "__main__":
    renderer = HTMLRenderer().run(sys.argv[1], sys.argv[2])
