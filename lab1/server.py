#!/usr/bin/env python3

# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Server that serves clients trying to work with the database."""

import threading
import socket
import json
import random
import argparse

import sys
sys.path.append("../modules")
from Server.database import Database
from Server.Lock.readWriteLock import ReadWriteLock

# -----------------------------------------------------------------------------
# Initialize and read the command line arguments
# -----------------------------------------------------------------------------

rand = random.Random()
rand.seed()
description = """\
Server for a fortune database. It allows clients to access the database in
parallel.\
"""
parser = argparse.ArgumentParser(description=description)
parser.add_argument(
    "-p", "--port", metavar="PORT", dest="port", type=int,
    default=rand.randint(1, 10000) + 40000, choices=range(40001, 50000),
    help="Set the port to listen to. Values in [40001, 50000]. "
         "The default value is chosen at random."
)
parser.add_argument(
    "-f", "--file", metavar="FILE", dest="file", default="dbs/fortune.db",
    help="Set the database file. Default: dbs/fortune.db."
)
opts = parser.parse_args()

db_file = opts.file
server_address = ("", opts.port)

# -----------------------------------------------------------------------------
# Auxiliary classes
# -----------------------------------------------------------------------------


class Server(object):

    """Class that provides synchronous access to the database."""

    def __init__(self, db_file):
        self.db = Database(db_file)
        self.rwlock = ReadWriteLock()

    # Public methods

    def read(self):
        #
        # Your code here.
        #
        # Remember the rule:
        # 1. Open the lock
        # 2. Perform action
        # 3. Close the lock
        # We do this so that if after opening the lock,
        # the database can crash while performing the action
        # So even if that happens, the lock should always be placed at the end
        # Hence the 'finally' 
        try:
            self.rwlock.read_acquire()
            fortune = self.db.read()
        finally:
            self.rwlock.read_release()
        return fortune

    def write(self, fortune):
        #
        # Your code here.
        #
        # Remember the rule:
        # 1. Open the lock
        # 2. Perform action
        # 3. Close the lock
        # We do this so that if after opening the lock,
        # the database can crash while performing the action
        # So even if that happens, the lock should always be placed at the end
        # Hence the 'finally' 
        try:
            self.rwlock.write_acquire()
            self.db.write(fortune)
        finally:
            self.rwlock.write_release()


class Request(threading.Thread):

    """ Class for handling incoming requests.
        Each request is handled in a separate thread.
    """

    def __init__(self, db_server, conn, addr):
        threading.Thread.__init__(self)
        self.db_server = db_server
        self.conn = conn
        self.addr = addr
        self.daemon = True

    # Private methods

    def process_request(self, request):
        """ Process a JSON formated request, send it to the database, and
            return the result.

            The request format is:
                {
                    "method": method_name,
                    "args": method_arguments
                }

            The returned result is a JSON of the following format:
                -- in case of no error:
                    {
                        "result": method_result
                    }
                -- in case of error:
                    {
                        "error": {
                            "name": error_class_name,
                            "args": error_arguments
                        }
                    }
        """
        #
        # Your code here.
        #
        # Always use a try-except block while handling remote calls (in future labs too)
        # Since we will have to call the database.py functions
        try:
            requestFromClient = json.loads(request)

            # Call the read function and return the result
            if requestFromClient.get("method") == "read":
                fortune = self.db_server.read()
                response = json.dumps({"result": fortune})
                # Run this to test the exception handling
                # print(1/0)
                return response
                

            # Call the write function and return the result
            elif requestFromClient.get("method") == "write":
                fortune = requestFromClient.get("args")
                self.db_server.write(fortune)
                response = json.dumps({"result": None})
                return response

        except Exception as e:
            print("Error")
            # If an exception occurs,
            # Properly check it by getting its name and the arguments
            # Pack them in JSON and send to the client
            # Client will raise the exception locally
            response = json.dumps({"error": {"name" : type(e).__name__, "args" : e.args}})
            return response


    def run(self):
        try:
            # Threat the socket as a file stream.
            worker = self.conn.makefile(mode="rw")
            # Read the request in a serialized form (JSON).
            request = worker.readline()
            # Process the request.
            result = self.process_request(request)
            # Send the result.
            worker.write(result + '\n')
            worker.flush()
        except Exception as e:
            # Catch all errors in order to prevent the object from crashing
            # due to bad connections coming from outside.
            print("The connection to the caller has died:")
            print("\t{}: {}".format(type(e), e))
        finally:
            self.conn.close()

# -----------------------------------------------------------------------------
# The main program
# -----------------------------------------------------------------------------

print("Listening to: {}:{}".format(socket.getfqdn(), opts.port))
with open("srv_address.tmp", "w") as f:
    f.write("{}:{}\n".format(socket.getfqdn(), opts.port))

sync_db = Server(db_file)

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(server_address)
server.listen(1)

print("Press Ctrl-C to stop the server...")

try:
    while True:
        try:
            conn, addr = server.accept()
            req = Request(sync_db, conn, addr)
            print("Serving a request from {0}".format(addr))
            req.start()
        except socket.error:
            continue
except KeyboardInterrupt:
    pass
