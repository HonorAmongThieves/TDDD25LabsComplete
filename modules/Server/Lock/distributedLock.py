# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 31 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Module for the distributed mutual exclusion implementation.

This implementation is based on the second Ricart-Agrawala algorithm.
The implementation should satisfy the following requests:
    --  when starting, the peer with the smallest id in the peer list
        should get the token.
    --  access to the state of each peer (dictionaries: request, token,
        and peer_list) should be protected.
    --  the implementation should graciously handle situations when a
        peer dies unexpectedly. All exceptions coming from calling
        peers that have died, should be handled such as the rest of the
        peers in the system are still working. Whenever a peer has been
        detected as dead, the token, request, and peer_list
        dictionaries should be updated accordingly.
    --  when the peer that has the token (either TOKEN_PRESENT or
        TOKEN_HELD) quits, it should pass the token to some other peer.
    --  For simplicity, we shall not handle the case when the peer
        holding the token dies unexpectedly.

"""

NO_TOKEN = 0
TOKEN_PRESENT = 1
TOKEN_HELD = 2

import time


class DistributedLock(object):

    """Implementation of distributed mutual exclusion for a list of peers.

    Public methods:
        --  __init__(owner, peer_list)
        --  initialize()
        --  destroy()
        --  register_peer(pid)
        --  unregister_peer(pid)
        --  acquire()
        --  release()
        --  request_token(time, pid)
        --  obtain_token(token)
        --  display_status()

    """

    def __init__(self, owner, peer_list):
        self.peer_list = peer_list
        self.owner = owner
        self.time = 0
        self.token = None
        self.request = {}
        self.state = NO_TOKEN

    def _prepare(self, token):
        """Prepare the token to be sent as a JSON message.
        This step is necessary because in the JSON standard, the key to
        a dictionary must be a string whild in the token the key is
        integer.
        """
        return list(token.items())

    def _unprepare(self, token):
        """The reverse operation to the one above."""
        return dict(token)

    # Public methods

    def initialize(self):
        """ Initialize the state, request, and token dicts of the lock.

        Since the state of the distributed lock is linked with the
        number of peers among which the lock is distributed, we can
        utilize the lock of peer_list to protect the state of the
        distributed lock (strongly suggested).

        NOTE: peer_list must already be populated when this
        function is called.

        """
        #
        # Your code here.
        #
        # These locks are everything in this lab
        # Use them very carefully
        self.peer_list.lock.acquire()
        try:
            peersList = sorted(self.peer_list.peers.keys())
            # If you are the first one to arrive (or the smallest ID (rarely gonna happen))
            # Initialize the token dictionary (it was None before)
            # Say that you have the token present
            # (not held since you never requested it at this time)
            # Add your self in the token dictionary
            if len(peersList) == 0 or peersList[0] > self.owner.id:
                self.token = {}
                self.state = TOKEN_PRESENT
                self.token[self.owner.id] = 0 #self.time
            else:
                # Otherwise go through through everyone and
                # set the request dictionary (meaning no one requested it initially)
                for pid in self.peer_list.get_peers():
                    self.request[pid] = 0
            # Set your own request in the dictionary as the current time too
            self.request[self.owner.id] = 0
        finally:
            self.peer_list.lock.release()

    def destroy(self):
        """ The object is being destroyed. (RIP)

        If we have the token (TOKEN_PRESENT or TOKEN_HELD), we must
        give it to someone else.

        """
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        try:
            if (self.state == TOKEN_PRESENT or self.state == TOKEN_HELD) and self.peer_list.get_peers():
                # If we have it and there are other peers present in the list, 
                # then release it
                self.release()

                #After releasing, check if we still have it (i.e nobody took it)
                if self.state != NO_TOKEN:
                    # If nobody wants it
                    # Sort the peer list
                    # Give it to the first peer
                    peersList = sorted(self.peer_list.peers.keys())
                    for pid in peersList:
                        # Careful not to give to yourself again lol
                        if pid != self.owner.id:
                            # Since it will be remote call, you try-except
                            try:
                                # Ask the other peer to take it
                                self.peer_list.peer(pid).obtain_token(self._prepare(self.token))
                                # If the call was successful (i.e the peer did not quit suddenly)
                                # break the loop since the token's been given
                                break
                            except:
                                # If the peer was not available for some reason
                                # Handle the exception and iterate the loop for next peer
                                print("Peer not unavailable")

                    #Finally say that we no longer have the token
                    self.state = NO_TOKEN
                        
        finally:
            self.peer_list.lock.release()

    def register_peer(self, pid):
        """Called when a new peer joins the system."""
        #
        # Your code here.
        #
        # Initially we don't have any request so set it to self.time (0)
        # And if we got the token (as from Initialize() criteria)
        # Set the obtaining to 0 (self.time)
        self.peer_list.lock.acquire()
        if self.token:
            self.token[pid] = 0
        self.request[pid] = 0
        self.peer_list.lock.release()


    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""
        #
        # Your code here.
        #
        # Remove your entire presence (like Tobi ;))
        self.peer_list.lock.acquire()
        if self.token:
            self.token.pop(pid)
        self.request.pop(pid)
        self.peer_list.lock.release()

    def acquire(self):
        """Called when this object tries to acquire the lock."""
        print("Trying to acquire the lock...")
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        # Increment the time and also update your entry in the request dictionary
        self.time = self.time + 1
        self.request[self.owner.id] = self.time
        # These prints are not necessary
        print("Aquire Request: ", self.request)
        print("Aquire Token: ", self.token)
        print("Aquire Time: ", self.time)
        try:
            if self.state == TOKEN_PRESENT:
                # If we have the token, simply use it
                self.state = TOKEN_HELD
            elif self.state == NO_TOKEN:
                # Request token from others
                peersList = sorted(self.peer_list.peers.keys())
                # It is important to release the lock
                # since we are going to request others
                # If we never release it,
                # they will not be able to acquire the lock
                # to process the request (hence deadlock)
                self.peer_list.lock.release()
                print(peersList)
                # We need to send the request to everyone
                # since we don't know who has it
                # Also note that even if the first one has it
                # We still need to send request to others
                # For example, if the peer having token releases
                # while a lot of peers have requested and we don't receive it first
                # then the next peer who gets it should already have a
                # pending request from us
                for pid in peersList:
                    if pid != self.owner.id:
                        print("Hey ID {}, give me token".format(pid))
                        try:
                            self.peer_list.peer(pid).request_token(self.time, self.owner.id)
                        except:
                            print("Peer not available")

                # Acquire the lock again that you released above
                self.peer_list.lock.acquire()
                while self.state != TOKEN_HELD:
                    # Keeping waiting for the token
                    # sleep and come back later to avoid the deadlock
                    # We do this to give a chance to others
                    # to process their request_token function
                    # Remember to import time
                    self.peer_list.lock.release()
                    time.sleep(0.1)
                    self.peer_list.lock.acquire()
                    pass
                      
        finally:
            self.peer_list.lock.release()



    def release(self):
        """Called when this object releases the lock."""
        print("Releasing the lock...")
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        try:
            self.state = TOKEN_PRESENT
            # Sort the peer list
            # Loop through it
            # Split the list into two parts
            # Right part is from IDs > current peer to the end of the list
            # Left part is from start of the list to IDs < current peer
            # Go through the Right one first and find the deserving peer
            # If not found, go through the Left.
            
            foundReceiver = False
            rightList = []
            leftList = []
            sortedList = sorted(self.peer_list.peers.keys())
            for pid in sortedList:
                if pid < self.owner.id:
                    leftList.append(pid)
                elif pid > self.owner.id:
                    rightList.append(pid)
            
            print(sortedList)
            print(leftList)
            print(rightList)
            
            for pid in rightList:
                if self.request[pid] > self.token[pid]: # Condition to check for a pending request    
                    print("Found receiver {} in rightList".format(pid))
                    self.token[self.owner.id] = self.time
                    #Send it to the that peer           
                    try:
                        self.peer_list.peer(pid).obtain_token(self._prepare(self.token))
                        self.state = NO_TOKEN
                        foundReceiver = True
                        break
                    except:
                        print("Peer id {} unavailable".format(pid))
                    


            if foundReceiver == False:
                for pid in leftList:
                    if self.request[pid] > self.token[pid]:        
                        print("Found receiver {} in leftList".format(pid))
                        self.token[self.owner.id] = self.time
                        #Send it to the that peer
                        try:
                            # Remember to send it in prepare
                            self.peer_list.peer(pid).obtain_token(self._prepare(self.token))
                            self.state = NO_TOKEN
                            foundReceiver = True
                            break
                        except:
                            print("Peer id {} unavailable".format(pid))
                        

        finally:
            self.peer_list.lock.release()

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""
        #
        # Your code here.
        #
        print("Received a request from peer {}".format(pid))
        self.peer_list.lock.acquire()
        # Updating the dictionary for that peer's request with the latest time
        self.request[pid] = max(time, self.request[pid])
        # More unnecessary prints
        print("Request Token Request: ", self.request)
        print("Request Token Token: ", self.token)
        print("Request Token Time: ", self.time)
        try:
            if self.state == TOKEN_PRESENT:
                # Release the token
                print("Token present")
                self.release()
            elif self.state == TOKEN_HELD:
                # Ain't gonna give it to nobody
                print("Token held")


        finally:
            self.peer_list.lock.release()


    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print("Receiving the token...")
        #
        # Your code here.
        #
        self.peer_list.lock.acquire()
        # Remember to receive it in unprepare
        self.token = self._unprepare(token)
        # MOAR prints
        print("Obtain Request: ", self.request)
        print("Obtain Token: ", self.token)
        print("Obtain Time: ", self.time)
        try:
            if self.time > self.token[self.owner.id]:
                # If we requested it, we should use it
                self.state = TOKEN_HELD
            else:
                # Never requested, still received? (lucky me) Keep it
                self.state = TOKEN_PRESENT
        finally:
            self.peer_list.lock.release()

    def display_status(self):
        """Print the status of this peer."""
        self.peer_list.lock.acquire()
        try:
            nt = self.state == NO_TOKEN
            tp = self.state == TOKEN_PRESENT
            th = self.state == TOKEN_HELD
            print("State   :: no token      : {0}".format(nt))
            print("           token present : {0}".format(tp))
            print("           token held    : {0}".format(th))
            print("Request :: {0}".format(self.request))
            print("Token   :: {0}".format(self.token))
            print("Time    :: {0}".format(self.time))
        finally:
            self.peer_list.lock.release()
