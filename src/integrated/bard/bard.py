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
import os
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

    def connect_to_master(self):
        """ Connect with the master frontserver"""
        global proxy
        while True:
            try:
                master_str = proxy.areYouMaster(True)

                if master_str == 'OK':
                    break
                elif master_str == "":
                    sleep_time = 2
                    print 'master has not been elected yet, sleep ', sleep_time, ' seconds'
                    time.sleep(sleep_time)
                else:
                    print 'master address (which is the address of final assigned frontend server) is ', 'http://' + master_str
                    proxy = xmlrpclib.ServerProxy("http://" + master_str)
                    break
            except Exception as e:
                print 'cannot connect to front server'
                print 'change to another front server'
                time.sleep(2)
                global current_index
                current_index = self.select_proxy()
                while current_index == -1:
                    print 'no available front servers'
                    print 'wait...'
                    time.sleep(2)
                    current_index = self.select_proxy()

                print 'connect to ', URL_list[current_index]
                proxy = s_list[current_index]

    def select_proxy(self):
        count = -1
        for s in s_list:
            count += 1
            try:
                s.check_alive()
                return count
            except:
                pass
        return -1

    def start(self):
        """ Connect with the master frontserver"""
        self.connect_to_master()
                
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
        print 'Event generating time is used for estimating the event updating time in the frontend server.'
        print 'Event Generating Time is ', time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime(os.times()[4]))
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
            connect_flag = 'YES'
            print 'Event Generating Time is ', time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime(os.times()[4]))
            if random.random() >= 1 - event_end_prob: # event end
                event_end_prob += cf.event_end_prob_incr_per_interval
                should_end = True

                end_event_num += 1
                end_event_set.add(sport)
                score[sport] = (score[sport][0], score[sport][1], should_end)
                connect_flag = self.set_score(sport, score[sport])

                G_score = score[sport][0]
                R_score = score[sport][1]
                if G_score > R_score :
                    connect_flag = self.incre_medal_tally('Gauls', 'Gold')
                    connect_flag = self.incre_medal_tally('Romans', 'Silver')
                elif G_score < R_score :
                    connect_flag = self.incre_medal_tally('Gauls', 'Silver')
                    connect_flag = self.incre_medal_tally('Romans', 'Gold')
                else :
                    connect_flag = self.incre_medal_tally('Gauls', 'Gold')
                    connect_flag = self.incre_medal_tally('Romans', 'Gold')

                if end_event_num == total_event_num :
                    should_end = False
                    break
                else :
                    should_end = False
                    if connect_flag == 'NO':
                        time.sleep(2)
                        self.connect_to_master()
                    continue
            if random.random() >= 1 - cf.score_update_prob: # update score
                if team == "Gauls":
                    score[sport] = (score[sport][0]+1, score[sport][1], should_end)
                    connect_flag = self.set_score(sport, score[sport])
                else:
                    score[sport] = (score[sport][0], score[sport][1]+1, should_end)
                    connect_flag = self.set_score(sport, score[sport])
            if connect_flag == 'NO':
                time.sleep(2)
                self.connect_to_master()
        print score
        return

    def incre_medal_tally(self, team, medal):
        while True:
            try:
                connect_flag = proxy.incrementMedalTally(team, medal)
                return connect_flag
            except:
                time.sleep(1)
                self.connect_to_master()
    def set_score(self, event, score):
        while True:
            try:
                connect_flag = proxy.setScore(event, score)
                return connect_flag
            except:
                time.sleep(1)
                self.connect_to_master()

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
remote_addresses = (cf.remote_server_ips, cf.remote_server_ports)
assigned_server_index = cf.assigned_server_index
current_index = assigned_server_index

URL_list = []
s_list = []

remote_servers_num = len(remote_addresses[0])

for i in range(remote_servers_num):
    URL = "http://" + remote_addresses[0][i] + ":" + str(remote_addresses[1][i]);
    URL_list.append(remote_addresses[0][i]+':'+str(remote_addresses[1][i]))
    s_list.append(xmlrpclib.ServerProxy(URL))

print "the address of initially assigned frontend server is http://", URL_list[current_index]
proxy = s_list[current_index]

score_thread = Bard()
score_thread.start()
