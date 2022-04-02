#!/usr/bin/env python3
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import cgi
import pathlib
import pickle


class Router():
    def __init__(self, DBSegmentsConfigFile="DBSegments", isReplica=False, replicaIP = None, segmentValueFile="SegmentValueTable.pickle"):
        self.readDBSegments(DBSegmentsConfigFile)
        self.isReplica = isReplica
        if not isReplica:
            self.replica = replicaIP
        self.nextSegment = 0
        self.segmentValueFile = pathlib.Path(segmentValueFile)
        if self.segmentValueFile.exists():
            self.segmentValueTable = pickle.load(self.segmentValueFile.open("rb"))
        else:
            self.segmentValueTable = []
    def readDBSegments(self, configFile="DBSegments"):
        self.configFileDBSegments = pathlib.Path(configFile)
        self.DBSegments = []
        with self.configFileDBSegments.open() as f:
            for line in f.readlines():
                leaderAndFollowers = line.strip().split()
                self.DBSegments.append(leaderAndFollowers)
            if len(self.DBSegments) == 0:
                raise SystemExit('ERROR: not enough Database segments. Check database nodes config file.')
    def nextSegment(self):
        self.nextSegment +=1
        return self.nextSegment
    def addKeyToSegmentValueTable(self, key, segment):
        self.segmentValueTable[key] = segment
        pickle.dump( self.segmentValueTable, self.segmentValueFile.open("wb") )
    def deleteKeyToSegmentValueTable(self, key):
        del self.segmentValueTable[key]
        pickle.dump( self.segmentValueTable, self.segmentValueFile.open("wb") )
        
        

class RequestHandler(BaseHTTPRequestHandler):

    def acknowledge(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    #def do_GET(self):
    #    self.acknowledge()
    #    self.wfile.write("GET request for {}".format(self.path).encode('utf-8'))

    def do_POST(self):
        ctype, pdict = cgi.parse_header(self.headers['content-type'])
        
        # refuse to receive non-json content
        if ctype != 'application/json':
            self.send_response(400)
            self.end_headers()
            return

        content_length = int(self.headers['content-length'])
        post_data = json.loads(self.rfile.read(content_length))
        print(f"{type(post_data['value'])}")

        self.acknowledge()
        #self.wfile.write(f"AAA {post_data}")
    
    


def run(server_class=HTTPServer, handler_class=RequestHandler, port=7777):
    server_address = ('', port)
    router = Router()
    httpd = server_class(server_address, handler_class)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()

if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
