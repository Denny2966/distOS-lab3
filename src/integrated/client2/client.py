#!/usr/bin/env python

"""
Python source code - replace this with a description of the code and write the code below this text.
"""

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import client_config as cf
import threading
import SocketServer
import sys
from SimpleXMLRPCServer import SimpleXMLRPCServer,SimpleXMLRPCRequestHandler
#import SimpleXMLRPCServer
import xmlrpclib
import socket
import re
import numpy as np
import random
import socket
import time
# Threaded mix-in
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass 

global assigned_server_index
global current_index
global client_addr
global pid
global proxy
global proxy_lock
global s_list
global URL_list

assigned_server_index = cf.assigned_server_index
client_addr = cf.client_addr
pid = cf.process_id
proxy_lock = threading.Lock()

tally_board = [[0 for x in xrange(2)] for x in xrange(3)]
score_board = [[0 for x in xrange(4)] for x in xrange(3)]

team_name_list = ['Gauls', 'Romans']
medal_type_list = ['Gold', 'Silver', 'Bronze']
event_type_list = ['Curling', 'Skating', 'Skiing']

team_name_dict = {"Gauls":0, "Romans":1}
medal_type_dict = {"Gold":0, "Silver":1, "Bronze":2}
event_type_dict = {"Curling":0, "Skating":1, "Skiing":2}

t_file = None
s_file = None

t_file_name = './log/tally_board.out'
s_file_name = './log/score_board.out'

#global sb_lock
#global output_lock
#global s_file_lock

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

def get_rand_value(value_list):
    return value_list[random.randrange(0, len(value_list))]

class ClientObject:
    def __init__(self, remote_ips, remote_ports):
        global remote_addresses
        remote_addresses = (remote_ips, remote_ports)

    def get_medal_tally(self, s, team_name = 'Gauls'):
        global pid
        fe_addr, result = s.get_medal_tally(pid, team_name)

        team_name_index = get_team_name_index(team_name)
        if team_name_index != -1 :
            tally_board[team_name_index] = result

            # write obtained medal tally into the output file
            with open(t_file_name, 'r+') as t_file :
                t_file_data = t_file.readlines()
                t_file_data[team_name_index] = str(team_name) + ': ' + str(result) + '\n'
                t_file.seek(0)
                t_file.writelines(t_file_data)
        return (fe_addr, result)

    def get_score(self, s, event_type = 'Curling'):
        global pid
        print pid
        fe_addr, result = s.get_score(pid, event_type)

        event_type_index = get_event_type_index(event_type)
        if event_type_index != -1:
            score_board[event_type_index] = result

            # write obtained scores into the output file
            with open(s_file_name, 'r+') as s_file :
                s_file_data = s_file.readlines()
                s_file_data[event_type_index] = str(event_type) + ': ' + str(result)  + '\n'
                s_file.seek(0)
                s_file.writelines(s_file_data)
        return (fe_addr, result)

    def start(self, poisson_lambda, simu_len, get_score_pb):
        global s_list
        global URL_list
        global current_index
        global proxy
        
        t = np.random.rand(poisson_lambda*simu_len) * simu_len
        t.sort()
        t[1:len(t)-1] = t[1:len(t)-1] - t[0:len(t)-2]

        s_list = []
        URL_list = []

        remote_servers_num = len(remote_addresses[0])

        for i in range(remote_servers_num):
            URL = "http://" + remote_addresses[0][i] + ":" + str(remote_addresses[1][i]);
            URL_list.append(remote_addresses[0][i]+':'+str(remote_addresses[1][i]))
            s_list.append(xmlrpclib.ServerProxy(URL))

        proxy_lock.acquire()
        current_index = assigned_server_index
        proxy = s_list[current_index]
        proxy_lock.release()

        count = 0
        break_flag = 0
        for t_val in t:
            count += 1
            #print count
            time.sleep(t_val)

            try:
                if np.random.rand(1) < get_score_pb:
                    print 'event'
                    proxy_lock.acquire()
                    result = self.get_score(proxy, get_rand_value(event_type_list))
                    print '+++++', result
                else:
                    print 'medal'
                    proxy_lock.acquire()
                    result = self.get_medal_tally(proxy, get_rand_value(team_name_list))
                    print '+++++', result
#            except socket.error, (value,message):
#                print "Could not open socket to the server: " + message
            except :
                info = sys.exc_info()
                print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
                
                current_index = self.select_proxy()
                break_flag = current_index

                if current_index != -1:
                    proxy = s_list[current_index]
                    if current_index != assigned_server_index:
                        print 'client: register client on a different front server'
                        proxy.registerClient(URL_list[assigned_server_index], client_addr[0]+':'+str(client_addr[1]))

            proxy_lock.release()

            if break_flag == -1:
                print 'no frontend servers available, exit (you can also choose to reconnect to the frontend servers)'
                break
        for s in s_list:
            try:
                s.deregisterClient(URL_list[assigned_server_index],client_addr[0]+':'+str(client_addr[1]))
            except:
                pass

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

class RPCObject():
    def change_back_proxy(self):
        global current_index
        global proxy
        print 'change_back_proxy is called'

        proxy_lock.acquire()
        current_index =  assigned_server_index
        proxy = s_list[current_index]
        print 'ooooooooooooooooooo'
        print 'ooooooooooooooooooo'
        print 'ooooooooooooooooooo'
        print 'ooooooooooooooooooo'
        print 'ooooooooooooooooooo'
        print 'ooooooooooooooooooo'
        print 'ooooooooooooooooooo'
        print 'I am back to assigned server: ', URL_list[current_index]
        proxy_lock.release()

        return True

class ServerThread(threading.Thread):
    """a RPC server listening to push request from the server of the whole system"""
    def __init__(self):
        threading.Thread.__init__(self)

        self.localServer = AsyncXMLRPCServer(('', cf.client_addr[1]), SimpleXMLRPCRequestHandler) #SimpleXMLRPCServer(('', port))
        self.localServer.register_instance(RPCObject())
    def run(self):
        self.localServer.serve_forever()

if __name__ == "__main__":
    remote_ips = cf.remote_server_ips
    remote_ports = cf.remote_server_ports
    ips_len = len(remote_ips)

#    rand_index = random.randrange(0,2)
#    remote_host_name = ips[rand_index]
#    remote_port = ports[rand_index]

    poisson_lambda = cf.poisson_lambda
    simu_len = cf.simu_len
    get_score_pb = cf.get_score_pb

    client = ClientObject(remote_ips, remote_ports)
    server = ServerThread()
    server.daemon = True; # allow the thread exit right after the main thread exits by keyboard interruption.
    server.start() # The server is now running

    client.start(poisson_lambda, simu_len, get_score_pb)
