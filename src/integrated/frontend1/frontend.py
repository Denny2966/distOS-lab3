#!/usr/bin/env python

"""
Python source code - replace this with a description of the code and write the code below this text.
"""

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import time
import threading
import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer,SimpleXMLRPCRequestHandler
#import SimpleXMLRPCServer
import os
import sys
import socket
import xmlrpclib
import frontend_config as cf
import re
import numpy as np

sys.path.append(os.getcwd())
import timeServer.timeServer as ts
import timeServer.time_config as tcf

win_per_num_request = cf.win_per_num_request
import heapq as hp
MAX_HEAP_SIZE = 1000
heap = []
c_time = 0
heap_size = 0
remove_count = 0

ack_num_dict = {}
aux_ack_dict = {}

process_num = 0

heap_lock = threading.Lock()
dict_lock = threading.Lock()

pid = cf.process_id
cluster_info = cf.cluster_info

myipAddress = cluster_info[str(pid)][0]
myport = cluster_info[str(pid)][1]

all_processes = []

global client_object
# Threaded mix-in
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass 

tally_board = [[0 for x in xrange(2)] for x in xrange(3)]
score_board = [[0 for x in xrange(4)] for x in xrange(3)]

team_name_dict = {"Gauls":0, "Romans":1}
medal_type_dict = {"Gold":0, "Silver":1, "Bronze":2}
event_type_dict = {"Curling":0, "Skating":1, "Skiing":2}

t_file = None
s_file = None
l_file = None
w_file = None

t_file_name = './log/tally_board.out'
s_file_name = './log/score_board.out'
l_file_name = './log/event_with_l_clock.out'
w_file_name = './log/winners_list.out'

#global sb_lock
#global output_lock
#global s_file_lock

class FirstList(tuple):
    def __lt__(self, other):
        if self[0:2] < other[0:2]:
            return True
        else:
            return False

def get_team_name_index(teamName):
	team_name_index = -1
	if team_name_dict.has_key(teamName): 	
		team_name_index = team_name_dict[teamName]
	return team_name_index

def get_medal_type_index(medalType):
	medal_type_index = -1
	if medal_type_dict.has_key(medalType): 	
		medal_type_index = medal_type_dict[medalType]
	return medal_type_index

def get_event_type_index(eventType):
	event_type_index = -1
	if event_type_dict.has_key(eventType): 	
		event_type_index = event_type_dict[eventType]
	return event_type_index

class ClientObject:
    def __init__(self, remote_host_name, remote_port):
        self.remote_address = (remote_host_name, remote_port)
        
        URL = "http://" + self.remote_address[0] + ":" + str(self.remote_address[1])
        self.s = xmlrpclib.ServerProxy(URL)

        self.time_ip = tcf.cluster_info[str(tcf.process_id)][0]
        self.time_port = tcf.cluster_info[str(tcf.process_id)][1]
        self.time_proxy = xmlrpclib.ServerProxy("http://" + self.time_ip + ":" + str(self.time_port))

    def get_medal_tally(self, client_id, team_name = 'Gauls'):
        global pid
        global c_time
        global s_list

        result = self.s.getMedalTally(team_name)
        return result

    def get_score(self, client_id, event_type = 'Curling' ):
        global pid
        global c_time
        global s_list

        print 'c_time heap_lock released'
        heap_lock.acquire()
        c_time_snapshot = c_time
        c_time += 1
        heap_lock.release()
        print 'c_time heap_lock released'
        req_type = 'score'
        req_para = event_type

        for s in s_list:
            try:
                s.record_request((req_type, req_para,), (c_time_snapshot+1, pid, client_id))
            except Exception as e:
                print e
                time.sleep(0.1)
                try:
                    s.record_request((req_type, req_para,), (c_time_snapshot+1, pid, client_id))
                except:
                    pass

        result = self.s.getScore(event_type)
        return result

    def incrementMedalTally(self, teamName, medalType):
        result = self.s.incrementMedalTally(teamName, medalType)
        return result

    def setScore(self, eventType, score): # score is a list (score_of_Gauls, score_of_Romans, flag_whether_the_event_is_over)
        print self.time_ip, self.time_port
        print ts.getOffset()
        epoch_time = self.time_proxy.getOffset()
        readable_time = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime(epoch_time))
        score += [readable_time]

        return self.s.setScore(eventType, score)

def record_request(request, l_time):
    global s_list
    global heap_lock
    global MAX_HEAP_SIZE
    global heap_size
    global heap
    global c_time

    ele = tuple(l_time)
    ele = FirstList(ele)
    flag = True
    heap_lock.acquire()
#    print 'record_request heap_lock acquired'
    if MAX_HEAP_SIZE <= heap_size:
        flag = False
    else:
        hp.heappush(heap, ele)
        if ele[1] != pid:
            c_time += 1 # suppose the request sent to self is received immediately!
        heap_size += 1

    heap_lock.release()
#    print 'record_request heap_lock released'
    return flag

def send_ack(l_time, pro_id):
    global ack_num_dict
    global aux_ack_dict
    global dict_lock

    l_time = tuple(l_time[0:2])
# l_time is asserted a tuple

    dict_lock.acquire()
    if l_time in ack_num_dict:
        ack_num_dict[l_time] += 1
        aux_ack_dict[l_time] += [pro_id]
    else:
        ack_num_dict[l_time] = 1
        aux_ack_dict[l_time] = [pro_id]
    dict_lock.release()
    return True

def check_alive():
    return True

class HeapThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    def run(self):
        global heap_lock
        global client_object
        global heap_size
        global heap
        global ack_num_dict
        global aux_ack_dict
        global process_num
        global remove_count
        global win_per_num_request
        global pid

        have_sent_ack = set()
        ExpireCount = 3 # 3 seconds
        flag_count = 0

        ele_pre = None

        while True:
            heap_lock.acquire()
            if heap_size == 0:
                print 'nothing'
                pass
            else:
                time.sleep(0.1)
#                print 'sent ack list: ', list(have_sent_ack)
                for l_time in heap:
                    l_time = tuple(l_time)
                    if l_time[0:2] not in have_sent_ack:
                        have_sent_ack.add(l_time[0:2])
                        for s in s_list:
                            try:
                                s.send_ack(l_time, pid)
                            except Exception as e: # retransmission, assume that when this happens, the failed RPC does not affect the status of corresponding server
                                print 'send ack error: ', e
                                try:
                                    s.send_ack(l_time, pid)
                                except:
                                    pass
                dict_lock.acquire()
                try:
                    heap_tmp = tuple(heap[0])
                    print '++++1', heap_tmp

                    if heap_tmp[0:2] in ack_num_dict:
                        print '++++2', ack_num_dict[heap_tmp[0:2]]
                        print '++++3', aux_ack_dict[heap_tmp[0:2]]
                    else:
                        print '++++2,3', ' no dict element'
                    print 'ele_pre', ele_pre
                    print 'ele_cur', heap_tmp[0:2]
                    print 'flag_count', flag_count
                    if ele_pre == heap_tmp[0:2]:
                        flag_count += 1
                    else:
                        flag_count = 0

                    if flag_count >= ExpireCount: # timeout, remove it from the heap
                        if heap_tmp[0:2] in ack_num_dict:
                            del ack_num_dict[heap_tmp[0:2]]
                        ele = hp.heappop(heap)
                        print '****', ele
                        heap_size -= 1
                        flag_count = 0

                    if heap_size > 0:
                        heap_tmp = tuple(heap[0])
                        ele_pre = heap_tmp[0:2]
                    else:
                        ele_pre = None

                    while heap_size > 0:
                        heap_tmp = tuple(heap[0])
                        if heap_tmp[0:2] in ack_num_dict and ack_num_dict[heap_tmp[0:2]] >= process_num:
                            del ack_num_dict[heap_tmp[0:2]]
                            ele = hp.heappop(heap)
                            print '----', ele
                            remove_count += 1
                            heap_size -= 1

                            with open(l_file_name, 'a') as l_file :
                                l_file.write(str((ele[0],ele[2])) + '\n')
                            if remove_count % win_per_num_request == 0:
                                with open(w_file_name, 'a') as w_file :
                                    w_file.write(str(ele[2]) + '\n')
                        else:
                            break
                except Exception as e:
                    print 'wzd'
                    print e
                dict_lock.release()
            heap_lock.release()
            print 'heap thread'
            time.sleep(1+np.random.rand()*2)

class ServerThread(threading.Thread):
    """a RPC server listening to push request from the server of the whole system"""
    def __init__(self, port):
        global remote_host_name
        global remote_port
        threading.Thread.__init__(self)
        self.port = port

        self.localServer = AsyncXMLRPCServer(('', port), SimpleXMLRPCRequestHandler) #SimpleXMLRPCServer(('', port))
        self.localServer.register_instance(ClientObject(remote_host_name, remote_port))

        self.localServer.register_function(record_request, 'record_request')
        self.localServer.register_function(check_alive, 'check_alive')
        self.localServer.register_function(send_ack, 'send_ack')
    def run(self):
        self.localServer.serve_forever()

if __name__ == "__main__":
    try:
        l_file = open(l_file_name, 'w')
        l_file.close()
        w_file = open(w_file_name, 'w')
        w_file.close()
    except Exception as e:
        print e
        sys.exit(1)
    # set up time server
    ts.SetupServer()

    remote_host_name = cf.server_ip
    remote_port = cf.server_port

    server = ServerThread(myport)
    server.daemon = True; # allow the thread exit right after the main thread exits by keyboard interruption.
    server.start() # The server is now running

    for i in cluster_info:
        all_processes.append((cluster_info[i][0], cluster_info[i][1], int(i)))
    process_num = len(all_processes)

    s_list = []
    for process in all_processes:
        URL = "http://" + process[0] + ":"+ str( process[1] )
        print URL
        s_list.append(xmlrpclib.ServerProxy(URL))

    while True:
        try:
            for i in range(len(all_processes)):
                print i
                s_list[i].check_alive()
            break
        except Exception as e:
            print e
            print 'waiting...'   
            time.sleep(2)
            continue

    heap_thread = HeapThread()
    heap_thread.daemon = True
    heap_thread.start()

    while True:
        time.sleep(5)
