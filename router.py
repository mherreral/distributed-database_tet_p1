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

    def acknowledge(self, code=200, contentType='application/json', message=""):
        self.send_response(code)
        self.send_header('Content-type', contentType)
        self.wfile.write(f"{json.dumps(message)}")
        self.end_headers()

    def relayMessageToDBNode(self, segment, postData):
        global router
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
            message["message"] = "No DB node in segment {segment} is responding."
            self.acknowledge(code=400, message=json.dumps(message))
            print("No DB node in segment {segment} is responding.")
        else:
            self.acknowledge(code=r.status_code, message=r.text)
        

    def do_POST(self):
        global router
        ctype, pdict = cgi.parse_header(self.headers['content-type'])
        
        # refuse to receive non-json content
        if ctype != 'application/json':
            message["message"] = "Not sent a json. Please send a json"
            self.acknowledge(code=400, message=json.dumps(message))
            return

        contentLength = int(self.headers['content-length'])
        postData = json.loads(self.rfile.read(contentLength))
        postData["source"] = "router"
        if postData['method'] == "write":
            segment = router.calculateNextSegment()
            router.addKeyToSegmentValueTable(postData['key'], segment)
        else:
            try:
                segment = router.segmentValueTable[postData['key']]
            except KeyError:
                message = json.dumps({"message": "No value for key: {postData['key']}"})
                self.acknowledge(code=404, message=message)
                return

        if postData["method"] == "delete":
            router.deleteKeyToSegmentValueTable(postData["key"])
        self.relayMessageToDBNode(segment, postData)

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
