#!/usr/bin/env python3
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import cgi
import pathlib
import pickle
import requests
import logging


class Router():
    def __init__(self, DBSegmentsConfigFile="DBSegments", isReplica=False, replicaIP=None, segmentValueFile="segmentKeyTable.pickle"):
        self.DBSegments = []
        self.readDBSegments(DBSegmentsConfigFile)
        self.isReplica = isReplica
        self.replica = '10.0.0.241' 
        #if not isReplica:
        #    self.replica = replicaIP
        self.nextSegment = 0
        self.segmentValueFile = pathlib.Path(segmentValueFile)
        if self.segmentValueFile.exists():
            self.segmentKeyTable = pickle.load(
                self.segmentValueFile.open("rb"))
        else:
            self.segmentKeyTable = {}

    def readDBSegments(self, configFile="DBSegments"):
        self.configFileDBSegments = pathlib.Path(configFile)
        with self.configFileDBSegments.open() as f:
            for line in f.readlines():
                leaderAndFollowers = line.strip().split()
                self.DBSegments.append(leaderAndFollowers)
            if len(self.DBSegments) == 0:
                raise SystemExit(
                    'ERROR: not enough Database segments. Check database nodes config file.')
        print("Read DB segments")

    def calculateNextSegment(self):
        self.nextSegment = (self.nextSegment + 1) % len(self.DBSegments)
        return self.nextSegment

    def addKeyTosegmentKeyTable(self, key, segment):
        self.segmentKeyTable[key] = segment
        pickle.dump(self.segmentKeyTable, self.segmentValueFile.open("wb"))

    def deleteKeyTosegmentKeyTable(self, key):
        del self.segmentKeyTable[key]
        pickle.dump(self.segmentKeyTable, self.segmentValueFile.open("wb"))


class RequestHandler(BaseHTTPRequestHandler):

    # message must be encoded already
    def acknowledge(self, code=200, contentType='application/json', message=""):
        self.send_response(code)
        self.send_header('Content-type', contentType)
        self.end_headers()
        self.wfile.write(message)

    def relayMessageToDBNode(self, segment, postData):
        global router
        segmentIndex = 0  # 0 is segment leader
        # 0 position is leader
        DBSegmentLeader = router.DBSegments[segment][segmentIndex]
        # while DB replicas in this segment
        while segmentIndex < len(router.DBSegments[segment]):
            try:
                print(f"Trying with Node {segmentIndex} {DBSegmentLeader}")
                r = requests.post(f'http://{DBSegmentLeader}:80', json=postData, timeout=5)
                print(f"Success with Node {segmentIndex}")
                break
            except requests.exceptions.Timeout:
                print(
                    f"Node {segmentIndex} in segment {segment} not responding.", end=" ")
                segmentIndex += 1  # use a replica
        if segmentIndex == len(router.DBSegments):
            message["message"] = "No DB node in segment {segment} is responding."
            self.acknowledge(code=400, message=json.dumps(
                message).encode('utf-8'))
            print("No DB node in segment {segment} is responding.")
        else:
            self.acknowledge(code=r.status_code,
                             message=r.text.encode('utf-8'))

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_POST(self):
        global router
        logging.basicConfig(filename="Client.log",
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt='%m/%d/%Y %I:%M:%S %p'
            )

        logging.info(f"Started post")

        ctype, pdict = cgi.parse_header(self.headers['content-type'])

        # refuse to receive non-json content
        if ctype != 'application/json':
            message["message"] = "Not sent a json. Please send a json"
            self.acknowledge(code=400, message=json.dumps(
                message).encode('utf-8'))
            logging.info(f"Err - Refused to recieve non-json content")
            return
        
        contentLength = int(self.headers['content-length'])
        postData = json.loads(self.rfile.read(contentLength))
        if postData['source'] == 'client':
            fromClient = True
        else:
            fromClient = False
        if not router.isReplica:
            postData['source'] = 'router'
            try:
                r = requests.post(
                    f'http://{router.replica}:80', json=postData, timeout=5)
            except requests.exceptions.Timeout:
                print(f"Replica for router not responding.", end=" ")
                logging.info(f"Replica for router not responding")
        postData["source"] = "router"
        if postData['method'] == "put":
            segment = router.calculateNextSegment()
            router.addKeyTosegmentKeyTable(postData['key'], segment)
        else:
            try:
                segment = router.segmentKeyTable[postData['key']]
            except KeyError:
                message = "".encode('utf-8')
                self.acknowledge(code=404, message=message)
                logging.info(f"Err - No value for key")
                return

        if postData["method"] == "delete":
            router.deleteKeyTosegmentKeyTable(postData["key"])
            logging.info(f"Deleting")
        if fromClient:
            self.relayMessageToDBNode(segment, postData)
            logging.info(f"Router is not replica")


def run(server_class=HTTPServer, handler_class=RequestHandler, port=80):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()


if __name__ == '__main__':
    import argparse
    # Parameters management
    parser = argparse.ArgumentParser(description="Router")
    parser.add_argument("--port", type=int, metavar="PORT",
                        default=80, help="Port on which to run. Default is 80.")
    parser.add_argument("--replica", type=str, default="false",
                        help="If is replica - supports 'false' and 'true'. Default is false.")

    args = parser.parse_args()
    if args.replica.lower() == "false":
        isReplica = False
    else:
        isReplica = True
    port = int(args.port)
    router = Router(isReplica=isReplica)

    logging.basicConfig(filename="router.log",
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt='%m/%d/%Y %I:%M:%S %p'
        )

    logging.info(f"STARTED ROUTER USING PORT {port}")

    run(port=port)
