#!/usr/bin/env python

"""
Python source code - replace this with a description of the code and write the code below this text.
"""

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import time
import threading
import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer,SimpleXMLRPCRequestHandler
import os
import sys
import socket
import xmlrpclib
import server_config as cf
import numpy as np

sys.path.append(os.getcwd())
#import timeServer.timeServer as ts
#import timeServer.time_config as tcf
#
#global time_ip
#global time_port
#global time_proxy

# Threaded mix-in
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass 

# Example class to be published
global tally_board
global score_board
global dummy_score_for_an_event
global push_registered_map

global team_name_dict # ['Gauls', 'Romans']
global medal_type_dict # ['Gold', 'Silver', 'Bronze']
global event_type_dict # ['Curling', 'Skating', 'Skiing']
global front_server_dict
global client_dict

global t_file
global s_file

global t_file_name
global s_file_name

class ReaderWriterLocks:
    """Structure to store related locks for realizing the second type reader and writer lock"""
    def __init__(self):
        self.lock_1 = threading.Lock()
        self.lock_2 = threading.Lock()
        self.lock_3 = threading.Lock()
        self.lock_r = threading.Lock()
        self.lock_w = threading.Lock()
        self.read_count = 0
        self.write_count = 0

class RequestObject:
    sum_val = 0;
    def __init__(self, tb_lock, sb_lock):
        self.tb_lock = tb_lock
        self.sb_lock = sb_lock
        self.p_map_lock = threading.Lock()
    
    def get_team_name_index(self, teamName):
        team_name_index = -1
        if team_name_dict.has_key(teamName):     
            team_name_index = team_name_dict[teamName]
        return team_name_index

    def get_medal_type_index(self, medalType):
        medal_type_index = -1
        if medal_type_dict.has_key(medalType):     
            medal_type_index = medal_type_dict[medalType]
        return medal_type_index

    def get_event_type_index(self, eventType):
        event_type_index = -1
        if event_type_dict.has_key(eventType):     
            event_type_index = event_type_dict[eventType]
        return event_type_index

    def pre_read(self, lock):
        lock.lock_3.acquire()
        lock.lock_r.acquire()
        lock.lock_1.acquire()
        lock.read_count += 1
        if lock.read_count == 1: 
            lock.lock_w.acquire();
        lock.lock_1.release()
        lock.lock_r.release()
        lock.lock_3.release()

    def post_read(self, lock):
        lock.lock_1.acquire()
        lock.read_count -= 1
        if lock.read_count == 0: 
            lock.lock_w.release();
        lock.lock_1.release()

    def pre_write(self, lock):
        lock.lock_2.acquire()
        lock.write_count += 1
        if lock.write_count == 1: 
            lock.lock_r.acquire()
        lock.lock_2.release()

        lock.lock_w.acquire()

    def post_write(self, lock):
        lock.lock_w.release()

        lock.lock_2.acquire()
        lock.write_count -= 1
        if lock.write_count == 0: 
            lock.lock_r.release()
        lock.lock_2.release()

    def incrementMedalTally(self, teamName, medalType):
        """increase medal for a team"""
        # lock
        self.pre_write(self.tb_lock)

        # write here
        team_name_index = self.get_team_name_index(teamName)
        medal_type_index = self.get_medal_type_index(medalType)

        if team_name_index != -1 and medal_type_index != -1:
            #tally_board[medal_type_index][team_name_index] += 1 # increase medal tally number of the medalType
            #tally_num = tally_board[medal_type_index][team_name_index]

            # write obtained medal tally into the output file
            with open(t_file_name, 'r+') as t_file :
                t_file_data = t_file.readlines()
                x = t_file_data[team_name_index] 
                y = x.index('[')
                x_val = eval(x[y:])
                x_val[medal_type_index] += 1
                t_file_data[team_name_index] = str(teamName) + ': ' + str(x_val) + '\n'
                t_file.seek(0)
                t_file.writelines(t_file_data)

        # unlock
        self.post_write(self.tb_lock)
        return True

    def getMedalTally(self, teamName):
        """get medal for a team"""
        # lock
        self.pre_read(self.tb_lock)

        # read here
        team_name_index = self.get_team_name_index(teamName)

        if team_name_index != -1:
            #gold_num = tally_board[0][team_name_index]
            #silver_num = tally_board[1][team_name_index]
            #bronze_num = tally_board[2][team_name_index]
            with open(t_file_name, 'r+') as t_file :
                t_file_data = t_file.readlines()
                x = t_file_data[team_name_index] 
                y = x.index('[')
                result = eval(x[y:])
                gold_num = result[0]
                silver_num = result[1]
                bronze_num = result[2]
        else: # the teamName is invalid
            gold_num = -1
            silver_num = -1
            bronze_num = -1

        # unlock
        self.post_read(self.tb_lock)

        return [gold_num, silver_num, bronze_num]

    def setScore(self, eventType, score): # score is a list (score_of_Gauls, score_of_Romans, flag_whether_the_event_is_over)
        """set score"""
        print score
        # lock
        self.pre_write(self.sb_lock)

        # write here
        event_type_index = self.get_event_type_index(eventType)

        if event_type_index != -1:
            #score_board[event_type_index] = score
            # push to the clients
            client_set = push_registered_map[event_type_index]

            # this part is added for testing the performance gain of cache over pure disk I/O #write obtained scores into the output file
            with open(s_file_name, 'r+') as s_file :
                s_file_data = s_file.readlines()
                s_file_data[event_type_index] = str(eventType) + ': ' + str(score)  + '\n'
                s_file.seek(0)
                s_file.writelines(s_file_data)

            for clientID in client_set :
                self.__pushUpdate(clientID, eventType, score)

        # unlock
        self.post_write(self.sb_lock)
        return True

    def getScore(self, eventType):
        """get score"""
        # lock
        self.pre_read(self.sb_lock)

        # read here
        event_type_index = self.get_event_type_index(eventType)

        if event_type_index != -1:
            #score = score_board[event_type_index]
            # this part is added for testing the performance gain of cache over pure disk I/O #write obtained scores into the output file
            with open(s_file_name, 'r+') as s_file :
                s_file_data = s_file.readlines()
                x = s_file_data[event_type_index]
                y = x.index('[')
                score = eval(x[y:])
        else:
            score = dummy_score_for_an_event; 

        # unlock
        self.post_read(self.sb_lock)

        return score

    def registerClient(self, clientID, eventTypes): # eventTypes is a list of eventType
        """register client with event(s)"""
        if clientID not in front_server_dict:
            self.p_map_lock.acquire()
#            print "+++++++++++++++++regi first+++++++++++++++++"
            try :
                URL = "http://" + clientID
                front_server_dict[clientID] = [xmlrpclib.ServerProxy(URL), set()]
            except :
                info = sys.exc_info()
                print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
                try :
                    del front_server_dict[clientID]
                except :
                    info = sys.exc_info()
                    print "Unexpected exception:", info[0],",",info[1]
                    self.p_map_lock.release()
                    return False
                else :
                    self.p_map_lock.release()
                    return False
            else :
#                print eventTypes
                for eventType in eventTypes:
#                    print eventType
                    event_type_index = self.get_event_type_index(eventType)
                    if event_type_index != -1:
                        push_registered_map[event_type_index].add(clientID)
                        front_server_dict[clientID][1].add(eventType)
                self.p_map_lock.release()
                return True
        else :
            self.p_map_lock.acquire()
#            print "+++++++++++++++++regi else+++++++++++++++++"
            for eventType in eventTypes:
#                print eventType
                event_type_index = self.get_event_type_index(eventType)
                if event_type_index != -1:
                    push_registered_map[event_type_index].add(clientID)
                    front_server_dict[clientID][1].add(eventType)
            self.p_map_lock.release()
            return True

    def deRegisterClient(self, clientID, eventTypes): # eventTypes is a list of eventType
        """de-register client from event(s)"""
        self.p_map_lock.acquire()
        if clientID in front_server_dict:
            for eventType in eventTypes:
                event_type_index = self.get_event_type_index(eventType)
                if event_type_index != -1:
                    push_registered_map[event_type_index].remove(clientID)
                    front_server_dict[clientID][1].remove(eventType)
            if len(front_server_dict[clientID][1]) == 0 :
                del front_server_dict[clientID]
        self.p_map_lock.release()
        return True

    def get_cache_mode(self):
        global cache_mode
        return cache_mode

    def claim_client(self, client_pair): # a two elements tuple ('client_ip:client_port', claim_whether_alive). We should use client_ip:client_id to mark a unique client; claim_whether_alive is a bool
        global client_dict
        global client_dict_lock

        client_dict_lock.acquire()
        client_dict[client_pair[0]] = client_dict[client_pair[1]]
        client_dict_lock.release()

        return True
    
    def __pushUpdate(self, clientID, eventType, score):
        """private method, used to push scores to clients"""
        try :
            front_server_dict[clientID][0].pushUpdate(eventType, score) # revoke pushUpdate(*) in the client side using RPC.
            return
        except socket.error, (value,message):
            print "Could not open socket to the remote instance: " + message
            return
        except :
            info = sys.exc_info()
            print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
            return

if __name__ == "__main__":
    t_file = None
    s_file = None

    t_file_name = './log/bs_tally_board.out'
    s_file_name = './log/bs_score_board.out'

    tally_board = [[0 for x in xrange(2)] for x in xrange(3)]
    score_board = [[0 for x in xrange(4)] for x in xrange(3)] # last element for each x is the timestamp
    front_server_dict = {}
    dummy_score_for_an_event = [-1 for x in xrange(3)]

    push_registered_map = [set() for index in xrange(3)]

    team_name_dict = {"Gauls":0, "Romans":1}
    medal_type_dict = {"Gold":0, "Silver":1, "Bronze":2}
    event_type_dict = {"Curling":0, "Skating":1, "Skiing":2}

    client_dict_lock = threading.Lock()
    client_dict = {}

    cache_mode = cf.cache_mode
    # Instantiate and bind to localhost:8080
    server = AsyncXMLRPCServer(('', int(cf.server_port)), SimpleXMLRPCRequestHandler)

    try :
        # initialize output files
        s_file = open(s_file_name, 'w')
        s_file.writelines([var[0] + ': ' + str(list(score_board[var[1]]))+'\n' for var in sorted(event_type_dict.iteritems(), key=lambda d:d[1], reverse=False)]) 
        s_file.close()

        t_file = open(t_file_name, 'w')

        tally_board_transpose = np.transpose(tally_board)

        t_file.writelines([var[0] + ': ' + str(list(tally_board_transpose[var[1]]))+'\n' for var in sorted(team_name_dict.iteritems(), key=lambda d:d[1], reverse = False)]) 
        t_file.close()
        # end of initialize output files

    except :
        info = sys.exc_info()
        print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
        sys.exit(1)
    else :
        pass

    # Register example object instance
    # tb_lock = threading.Lock();
    # sb_lock = threading.Lock();

    server.register_instance(RequestObject(ReaderWriterLocks(), ReaderWriterLocks()))

    # set up time server
    # ts.SetupServer()

    #time_ip = tcf.cluster_info[str(tcf.process_id)][0]
    #time_port = tcf.cluster_info[str(tcf.process_id)][1]
    #time_proxy = xmlrpclib.ServerProxy("http://" + time_ip + ":" + str(time_port))

    # run!
    server.serve_forever()
