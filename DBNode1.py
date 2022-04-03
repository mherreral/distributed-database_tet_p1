#!/usr/bin/env python3
from genericpath import exists
from http.server import BaseHTTPRequestHandler, HTTPServer
from sys import argv
import logging
import json
import os
import requests
import socket


isReplica = True
dbnodes = ['10.0.1.197', '10.0.1.32']
rootDir = '/home/ec2-user/db'
#rootDir = '/home/mhl/Documents/2022-1/tele/distributed-database_tet_p1'


class Database(BaseHTTPRequestHandler):
    global dbnodes, rootDir

    # Send response to client
    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def sendRequestToReplica(self, query):
        hostname = socket.gethostname()
        local_ip = str(socket.gethostbyname(hostname))
        for node in dbnodes:
            if(node != local_ip):
                nextDB = 'http://' + node + ':8080/'
                r = requests.post(nextDB, json=query)
            else:
                pass

    # Return data from DB
    def get(self, post_data):
        filename = post_data['key']
        requestedFile = os.path.join(rootDir, filename)
        # Open and send requested file if found, if not, send 404
        try:
            file = open(requestedFile, "r")
            content = file.read()
            file.close()
            self._set_response(200)
            # self.wfile.write(content.encode('utf-8'))
            self.wfile.write(content)
        except:
            self._set_response(404)

    # Update resource in database
    def update(self, post_data):
        filename = post_data['key']
        file_content = post_data['value']
        requestedFile = os.path.join(rootDir, filename)
        if exists(requestedFile):
            # Update file
            file = open(requestedFile, "w")
            file.write(file_content)
            file.close()
            self._set_response(200)
        else:
            self._set_response(404)

        if not isReplica:
            self.sendRequestToReplica(post_data)

    # Delete resource in database
    def delete(self, post_data):
        filename = post_data['key']
        requestedFile = os.path.join(rootDir, filename)
        # Open and delete requested file if found, if not, send 404
        if exists(requestedFile):
            os.remove(requestedFile)
            self._set_response(200)
        else:
            self._set_response(404)

        if not isReplica:
            self.sendRequestToReplica(post_data)

    # Create resource in database
    def put(self, post_data):
        filename = post_data['key']
        file_content = post_data['value']
        # Save file
        requestedFile = os.path.join(rootDir, filename)
        file = open(requestedFile, "w")
        file.write(file_content)
        file.close()
        self._set_response(200)

        if not isReplica:
            self.sendRequestToReplica(post_data)

    def do_POST(self):
        # Gets the size of data
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logging.info("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
                     str(self.path), str(self.headers), post_data)

        source = post_data['source']
        method = post_data['method']

        global isReplica
        # If message is sent from router, then the node is leader
        if source == 'router':
            isReplica = False
            post_data['source'] = 'leader'
            if method == 'get':
                self.get(post_data)
            elif method == 'put':
                self.put(post_data)
            elif method == 'update':
                self.put(post_data)
            else:  # delete
                self.delete(post_data)
        else:
            isReplica = True
            if method == 'get':
                self.get(post_data)
            elif method == 'put':
                self.put(post_data)
            elif method == 'update':
                self.put(post_data)
            else:  # delete
                self.delete(post_data)


def run(server_class=HTTPServer, handler_class=Database, port=8080):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info('Starting httpd...\n')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Stopping httpd...\n')


if __name__ == "__main__":

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
