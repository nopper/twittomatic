"""
Simple sentiment analysis web service
"""

import os
import json
from flask import Flask, Response, request
app = Flask(__name__)

from sentiment import Analyzer

analyzer = Analyzer()

@app.route('/sentiment')
def main():
    sentence = request.args['sentence']
    return Response(json.dumps({
        'rate': analyzer.analyze_sentence(sentence)
    }), status=200, mimetype="application/json")

if __name__ == '__main__':
    app.run(debug=True)
