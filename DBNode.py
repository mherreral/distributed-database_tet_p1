#!/usr/bin/env python3
from genericpath import exists
from http.server import BaseHTTPRequestHandler, HTTPServer
from sys import argv
import logging
import json
import os

from flask import request

isReplica = False
followersIp = []
#rootDir = '/db'
rootDir = '/home/mhl/Documents/2022-1/tele/distributed-database_tet_p1'


class Database(BaseHTTPRequestHandler):
    global isReplica, followersIp, rootDir

    # Send response to client
    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def sendRequestToReplica(self, key, text):
        pass

    # Return data from DB
    def get(self, filename):
        #requestedFile = os.path.join(rootDir, file)
        # Open and send requested file if found, if not, send 404
        try:
            file = open(filename, "r")
            content = file.read()
            file.close()
            self._set_response(200)
            self.wfile.write(content.encode('utf-8'))
        except:
            self._set_response(404)

    # Update resource in database
    def update(self, filename, file_content):
        #requestedFile = os.path.join(rootDir, post_data['key'])
        if exists(filename):
            # Update file
            file = open(filename, "w")
            file.write(file_content)
            file.close()
            self._set_response(200)
        else:
            self._set_response(404)

        if not getIsReplica():
            pass

    # Delete resource in database
    def delete(self, filename):
        #requestedFile = os.path.join(rootDir, get_data['key'])
        # Open and delete requested file if found, if not, send 404
        if exists(filename):
            os.remove(filename)
            self._set_response(200)
        else:
            self._set_response(404)

    # Create resource in database
    def post(self, file, file_content):
        # Save file
        #requestedFile = os.path.join(rootDir, post_data['key'])
        file = open(file, "w")
        file.write(file_content)
        file.close()
        self._set_response(200)

    def do_GET(self):
        logging.info("GET request,\nPath: %s\nHeaders:\n%s\n",
                     str(self.path), str(self.headers))

        # Obtain json request from client
        request_lenght = len(self.path)
        query = self.path[2:request_lenght]

        # Obtain path
        get_data = query.split("=")
        requested_file = get_data[1]
        self.get(requested_file)

    def do_POST(self):
        # Gets the size of data
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logging.info("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
                     str(self.path), str(self.headers), post_data)

        requested_file = post_data['key']
        file_content = post_data['value']
        self.post(requested_file, file_content)

    def do_PUT(self):
        # Gets the size of data
        content_length = int(self.headers['Content-Length'])
        put_data = json.loads(self.rfile.read(content_length))
        logging.info("PUT request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
                     str(self.path), str(self.headers), put_data)

        requested_file = put_data['key']
        file_content = put_data['value']
        self.update(requested_file, file_content)

    def do_DELETE(self):
        logging.info("DELETE request,\nPath: %s\nHeaders:\n%s\n",
                     str(self.path), str(self.headers))

        # Obtain json request from client
        request_lenght = len(self.path)
        query = self.path[2:request_lenght]

        # Obtain path
        delete_data = query.split("=")
        requested_file = delete_data[1]
        self.delete(requested_file)


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
