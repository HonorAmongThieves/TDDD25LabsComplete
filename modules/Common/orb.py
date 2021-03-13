# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 16 March 2017
#
# Copyright 2012-2017 Linkoping University
# -----------------------------------------------------------------------------

import threading
import socket
import json

"""Object Request Broker

This module implements the infrastructure needed to transparently create
objects that communicate via networks. This infrastructure consists of:

--  Strub ::
        Represents the image of a remote object on the local machine.
        Used to connect to remote objects. Also called Proxy.
--  Skeleton ::
        Used to listen to incoming connections and forward them to the
        main object.
--  Peer ::
        Class that implements basic bidirectional (Stub/Skeleton)
        communication. Any object wishing to transparently interact with
        remote objects should extend this class.

"""


class CommunicationError(Exception):
    pass


class Stub(object):

    """ Stub for generic objects distributed over the network.

    This is  wrapper object for a socket.

    """

    def __init__(self, address):
        self.address = tuple(address)

    def _rmi(self, method, *args):
        #
        # Your code here.
        #
        # Establish connection with the remote object
        mySocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        mySocket.connect(self.address)
        connection = mySocket.makefile(mode="rw")
        
        # Prepare and send the request to remote object
        requestToNS = json.dumps({"method":method,"args":args}) + '\n'
    
        connection.write(requestToNS)
        connection.flush()

        # Receive the response from remote object 
        responseFromNS = json.loads(connection.readline())
        connection.close()

        #Show the response from remote object
        if responseFromNS.get("error"):
            err = type(responseFromNS['error']['name'], (BaseException,), dict())
            raise err(*responseFromNS['error']['args'])

        return responseFromNS.get("result")

    def __getattr__(self, attr):
        """Forward call to name over the network at the given address."""
        def rmi_call(*args):
            return self._rmi(attr, *args)
        return rmi_call


class Request(threading.Thread):

    """Run the incoming requests on the owner object of the skeleton."""

    def __init__(self, owner, conn, addr):
        threading.Thread.__init__(self)
        self.addr = addr
        self.conn = conn
        self.owner = owner
        self.daemon = True

    # Need a function to process request just like in Lab 1
    def process_request(self, request):
        try:
            requestFromPeer = json.loads(request)
            # Stub requests will be redirected here
            # Get the specified value from the specified object
            result = getattr(self.owner, requestFromPeer["method"])(*requestFromPeer["args"])
            response = json.dumps({"result": result})
            return response

        except Exception as e:
            response = json.dumps({"error": {"name" : type(e).__name__, "args" : e.args}})
            return response

    def run(self):
        #
        # Your code here.
        #
        try:
            # Treat the socket as a file stream.
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


class Skeleton(threading.Thread):

    """ Skeleton class for a generic owner.

    This is used to listen to an address of the network, manage incoming
    connections and forward calls to the generic owner class.

    """

    def __init__(self, owner, address):
        threading.Thread.__init__(self)
        self.address = address
        self.owner = owner
        self.daemon = True
        #
        # Your code here.
        #
        # Bind to the address given and listen for incoming connection
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(self.address)
        self.server.listen(1)

    def run(self):
        #
        # Your code here.
        #
        # If the start function of a threading.Thread is called,
        # then the run function is executed in a new thread
        while True:
            try:
                # accept() will return two things:
                # A socket representing the connection and the address
                # Save them properly in different variables
                conn, addr = self.server.accept()
                # Initialize the Request class and let it handle everything
                # Remember Skeleton only acts as a bridge
                req = Request(self.owner, conn, addr)
                print("Serving a request from {0}".format(addr))
                # .start() will make a new thread of Request and
                # execute its run()
                req.start()
            except socket.error:
                continue
        


class Peer:

    """Class, extended by objects that communicate over the network."""

    def __init__(self, l_address, ns_address, ptype):
        self.type = ptype
        self.hash = ""
        self.id = -1
        self.address = l_address
        self.skeleton = Skeleton(self, ('', l_address[1]))
        self.name_service_address = ns_address
        self.name_service = Stub(self.name_service_address)

    # Public methods

    def start(self):
        """Start the communication interface."""

        self.skeleton.start()
        self.id, self.hash = self.name_service.register(self.type,
                                                        self.address)

    def destroy(self):
        """Unregister the object before removal."""

        self.name_service.unregister(self.id, self.type, self.hash)

    def check(self):
        """Checking to see if the object is still alive."""

        return (self.id, self.type)
