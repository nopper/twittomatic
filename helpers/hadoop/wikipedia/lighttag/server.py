"""
Simple sentiment analysis web service
"""

import os
import json
from flask import Flask, Response, request
app = Flask(__name__)

from lighttag import LightTag

tagger = LightTag("anchors.marisa")

@app.route('/sentiment')
def main():
    sentence = request.args['sentence']
    return Response(json.dumps(tagger.annotate(sentence)),
                    status=200, mimetype="application/json")

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
