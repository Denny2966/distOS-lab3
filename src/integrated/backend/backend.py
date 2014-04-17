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

sys.path.append(os.getcwd())
import timeServer.timeServer as ts
import timeServer.time_config as tcf

global time_ip
global time_port
global time_proxy

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
global client_dict

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
			tally_board[medal_type_index][team_name_index] += 1 # increase medal tally number of the medalType
			tally_num = tally_board[medal_type_index][team_name_index]


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
			gold_num = tally_board[0][team_name_index]
			silver_num = tally_board[1][team_name_index]
			bronze_num = tally_board[2][team_name_index]
		else: # the teamName is invalid
			gold_num = -1
			silver_num = -1
			bronze_num = -1

		# unlock
		self.post_read(self.tb_lock)

		return [gold_num, silver_num, bronze_num]


	def setScore(self, eventType, score): # score is a list (score_of_Gauls, score_of_Romans, flag_whether_the_event_is_over)
		"""set score"""
		# lock
		self.pre_write(self.sb_lock)

		# write here
		event_type_index = self.get_event_type_index(eventType)

		if event_type_index != -1:
			score_board[event_type_index] = score
			# push to the clients
			client_set = push_registered_map[event_type_index]

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
			score = score_board[event_type_index]
		else:
			score = dummy_score_for_an_event; 

		# unlock
		self.post_read(self.sb_lock)

		return score

	def registerClient(self, clientID, eventTypes): # eventTypes is a list of eventType
		"""register client with event(s)"""
		if clientID not in client_dict:
			self.p_map_lock.acquire()
#			print "+++++++++++++++++regi first+++++++++++++++++"
			try :
				URL = "http://" + clientID
				client_dict[clientID] = [xmlrpclib.ServerProxy(URL), set()]
			except :
				info = sys.exc_info()
				print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
				try :
					del client_dict[clientID]
				except :
					info = sys.exc_info()
					print "Unexpected exception:", info[0],",",info[1]
					self.p_map_lock.release()
					return False
				else :
					self.p_map_lock.release()
					return False
			else :
#				print eventTypes
				for eventType in eventTypes:
#					print eventType
					event_type_index = self.get_event_type_index(eventType)
					if event_type_index != -1:
						push_registered_map[event_type_index].add(clientID)
						client_dict[clientID][1].add(eventType)
				self.p_map_lock.release()
				return True
		else :
			self.p_map_lock.acquire()
#			print "+++++++++++++++++regi else+++++++++++++++++"
			for eventType in eventTypes:
#				print eventType
				event_type_index = self.get_event_type_index(eventType)
				if event_type_index != -1:
					push_registered_map[event_type_index].add(clientID)
					client_dict[clientID][1].add(eventType)
			self.p_map_lock.release()
			return True

	def deRegisterClient(self, clientID, eventTypes): # eventTypes is a list of eventType
		"""de-register client from event(s)"""
		self.p_map_lock.acquire()
		if clientID in client_dict:
			for eventType in eventTypes:
				event_type_index = self.get_event_type_index(eventType)
				if event_type_index != -1:
					push_registered_map[event_type_index].remove(clientID)
					client_dict[clientID][1].remove(eventType)
			if len(client_dict[clientID][1]) == 0 :
				del client_dict[clientID]
		self.p_map_lock.release()
		return True

	def __pushUpdate(self, clientID, eventType, score):
		"""private method, used to push scores to clients"""
		try :
			client_dict[clientID][0].pushUpdate(eventType, score) # revoke pushUpdate(*) in the client side using RPC.
			return
		except socket.error, (value,message):
			print "Could not open socket to the remote instance: " + message
			return
		except :
			info = sys.exc_info()
			print "Unexpected exception, cannot connect to the server:", info[0],",",info[1]
			return

if __name__ == "__main__":
	tally_board = [[0 for x in xrange(2)] for x in xrange(3)]
	score_board = [[0 for x in xrange(4)] for x in xrange(3)] # last element for each x is the timestamp
	client_dict = {}
	dummy_score_for_an_event = [-1 for x in xrange(3)]

	push_registered_map = [set() for index in xrange(3)]

	team_name_dict = {"Gauls":0, "Romans":1}
	medal_type_dict = {"Gold":0, "Silver":1, "Bronze":2}
	event_type_dict = {"Curling":0, "Skating":1, "Skiing":2}
	# Instantiate and bind to localhost:8080
	server = AsyncXMLRPCServer(('', int(cf.server_port)), SimpleXMLRPCRequestHandler)

	# Register example object instance
	# tb_lock = threading.Lock();
	# sb_lock = threading.Lock();

	server.register_instance(RequestObject(ReaderWriterLocks(), ReaderWriterLocks()))

	# set up time server
	ts.SetupServer()

	time_ip = tcf.cluster_info[str(tcf.process_id)][0]
	time_port = tcf.cluster_info[str(tcf.process_id)][1]
	time_proxy = xmlrpclib.ServerProxy("http://" + time_ip + ":" + str(time_port))

	# run!
	server.serve_forever()
