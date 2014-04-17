#!/usr/bin/env python

"""
Python source code - replace this with a description of the code and write the code below this text.
"""

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
import threading
import time
import random
import copy
from SimpleXMLRPCServer import SimpleXMLRPCServer 
import xmlrpclib
import bard_config as cf
from sets import Set
import sys
import socket

"""
Bard class - Thin client that updates the server when medal tally or score needs to be changed.
"""

global tally_exit
tally_exit = False
global score_exit
score_exit = False

end_event_set = Set([])
total_event_num = 3

class Bard():
    """Class that manage updatescore and updateTally. Inherits threading.Thread methods"""
#    def __init__(self):

    def start(self):
        """ Launches the update function"""
	try :
		self.updateScore()
	except socket.error, (value,message):
		print "Could not open socket to the server: " + message
		return
	except :
		info = sys.exc_info()
		print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]

    def updateScore(self):
        """
        Updates score to tell if event should end or if score should be updated
        Uses a remote procedure call to update the score
        """
        score = {"Curling":(0,0,False),"Skating":(0,0,False), "Skiing":(0,0,False)}
        should_end = False
	
	end_event_num = 0

        while not score_exit:
            team, sport = get_team(), get_sport()
	    if sport in end_event_set :
		    continue
            time.sleep(cf.update_show_interval)
            print score
	    event_end_prob = cf.event_end_prob
            if random.random() >= 1 - event_end_prob:
		event_end_prob += cf.event_end_prob_incr_per_interval
                should_end = True

		end_event_num += 1
		end_event_set.add(sport)
		score[sport] = (score[sport][0], score[sport][1], should_end)
		proxy.setScore(sport, score[sport])

		G_score = score[sport][0]
		R_score = score[sport][1]
		if G_score > R_score :
                	proxy.incrementMedalTally('Gauls', 'Gold')
                	proxy.incrementMedalTally('Romans', 'Silver')
		elif G_score == R_score :
                	proxy.incrementMedalTally('Gauls', 'Silver')
                	proxy.incrementMedalTally('Romans', 'Gold')
		else :
                	proxy.incrementMedalTally('Gauls', 'Gold')
                	proxy.incrementMedalTally('Romans', 'Gold')

		if end_event_num == total_event_num :
			should_end = False
			break
		else :
			should_end = False
			continue
            if random.random() >= 1 - cf.score_update_prob:
                if team == "Gauls":
                    score[sport] = (score[sport][0]+1, score[sport][1], should_end)
                    proxy.setScore(sport, score[sport])
                else:
                    score[sport] = (score[sport][0], score[sport][1]+1, should_end)
                    proxy.setScore(sport, score[sport])
	print score
	return

def get_medal_type():
    """Returns a random medal type"""
    num = random.randrange(1,4)
    if num == 1:
        return "Gold"
    if num == 2:
        return "Silver"
    else:
        return "Bronze"


def get_sport():
    """Returns a random sport"""
    num = random.randrange(1,4)
    if num == 1:
        return "Curling"
    if num == 2:
        return "Skating"
    else:
        return "Skiing"

def get_team():
    """Returns a random team"""
    num = random.randrange(1,3)
    if num == 1:
        return "Gauls"
    else:
        return "Romans"

lock = threading.Lock()
proxy = xmlrpclib.ServerProxy("http://" + cf.server_ip + ":" + cf.server_port)
score_thread = Bard()
score_thread.start()
