
import json
import os
import sys

from flask import Flask, request
#DEBUG=True
app = Flask(__name__)

@app.route("/")
def index():
	to_return = ""
    with open('info_text.html', 'r') as f:
    	to_return = f.read()
	return to_return

import web_views


@app.route("/trips")
def view_all_trips():
    print request.args
    tstart = request.args.get('tstart', None)
    tend = request.args.get('tend', None)
    if tstart: tstart = int(tstart)
    if tend: tend = int(tend)
    print tstart, tend

    return json.dumps(web_views.get_trips(start=tstart, end=tend))



if __name__ == "__main__":
    app.run()

