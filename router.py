#!/usr/bin/env python3
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import cgi
import pathlib
import pickle
import requests


class Router():
    def __init__(self, DBSegmentsConfigFile="DBSegments", isReplica=False, replicaIP = None, segmentValueFile="SegmentValueTable.pickle"):
        self.DBSegments = []
        self.readDBSegments(DBSegmentsConfigFile)
        self.isReplica = isReplica
        if not isReplica:
            self.replica = replicaIP
        self.nextSegment = 0
        self.segmentValueFile = pathlib.Path(segmentValueFile)
        if self.segmentValueFile.exists():
            self.segmentValueTable = pickle.load(self.segmentValueFile.open("rb"))
        else:
            self.segmentValueTable = {}
    def readDBSegments(self, configFile="DBSegments"):
        self.configFileDBSegments = pathlib.Path(configFile)
        with self.configFileDBSegments.open() as f:
            for line in f.readlines():
                leaderAndFollowers = line.strip().split()
                self.DBSegments.append(leaderAndFollowers)
            if len(self.DBSegments) == 0:
                raise SystemExit('ERROR: not enough Database segments. Check database nodes config file.')
        print("Read DB segments")
    def calculateNextSegment(self):
        self.nextSegment = (self.nextSegment + 1) % len(self.DBSegments)
        return self.nextSegment
        
        return self.nextSegment
    def addKeyToSegmentValueTable(self, key, segment):
        self.segmentValueTable[key] = segment
        pickle.dump( self.segmentValueTable, self.segmentValueFile.open("wb") )
    def deleteKeyToSegmentValueTable(self, key):
        del self.segmentValueTable[key]
        pickle.dump( self.segmentValueTable, self.segmentValueFile.open("wb") )
        
        

router = Router()
print ("ÁA")
class RequestHandler(BaseHTTPRequestHandler):

    def acknowledge(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    #def do_GET(self):
    #    self.acknowledge()
    #    self.wfile.write("GET request for {}".format(self.path).encode('utf-8'))

    def do_POST(self):
        global router
        ctype, pdict = cgi.parse_header(self.headers['content-type'])
        
        # refuse to receive non-json content
        if ctype != 'application/json':
            self.send_response(400)
            self.end_headers()
            return

        contentLength = int(self.headers['content-length'])
        postData = json.loads(self.rfile.read(contentLength))

        segment = router.calculateNextSegment()
        segmentIndex = 0 # 0 is segment leader
        DBSegmentLeader = router.DBSegments[segment][segmentIndex] # 0 position is leader
        while segmentIndex < len (router.DBSegments[segment]): # while DB replicas in this segment
            try:
                print(f"Trying with Node {segmentIndex}")
                r = requests.post(f'http://{DBSegmentLeader}:7777', json=postData, timeout=5)
                print(f"Success with Node {segmentIndex}")
                break
            except requests.exceptions.Timeout:
                print (f"Node {segmentIndex} in segment {segment} not responding.", end=" ")
                segmentIndex +=1 # use a replica
        if segmentIndex == len(router.DBSegments):
            raise SystemExit("No DB node in segment {segment} is responding.")
        router.addKeyToSegmentValueTable(postData['key'], segment)
        import time
        time.sleep(3)
        print(router.segmentValueTable)


        self.acknowledge()
        #self.wfile.write(f"AAA {post_data}")

def run(server_class=HTTPServer, handler_class=RequestHandler, port=7777):
    server_address = ('', port)
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
