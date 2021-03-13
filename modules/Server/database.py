# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 24 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

"""Implementation of a simple database class."""

import random


class Database(object):

    """Class containing a database implementation."""

    def __init__(self, db_file):
        self.db_file = db_file
        self.rand = random.Random()
        self.rand.seed()
        #
        # Your code here.
        #
        # Open the database file in a readable form
        # Split it with newline character before and after the % separator
        # Store in a list
        self.readFromFile = open(db_file, "r")
        self.myList = self.readFromFile.read().split("\n%\n")
        # Delete the last entry as it won't be a fortune
        self.myList.pop()
        # Close it
        self.readFromFile.close()

    def read(self):
        """Read a random location in the database."""
        #
        # Your code here.
        #
        # print(self.myList)
        # Return a random fortune from the list
        return random.choice(self.myList)
        

    def write(self, fortune):
        """Write a new fortune to the database."""
        #
        # Your code here.
        #
        # Open the database file in a writeable form
        # Simply append the list with the fortune
        self.writeInFile = open(self.db_file,"a")
        self.myList.append(fortune)
        # Write to the file in the same way as the split
        # i.e newline before and after the % separator
        self.writeInFile.write(fortune + "\n" + "%" + "\n")
        # Closing the writable file is very important
        # Otherwise the changes won't take place until you exit the program
        self.writeInFile.close()

    
