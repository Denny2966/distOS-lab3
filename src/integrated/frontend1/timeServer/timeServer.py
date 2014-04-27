import time_config as tcf
import sys
import os
import threading
import xmlrpclib
import time
import subprocess
import socket
import timeit
import SocketServer
import numpy as np

sys.path.append(os.getcwd()+'/..')
#import frontend as fe

from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer): pass 

masterIP = ""
port = -1 # master port, initialized as -1, since master is not identified yet

pid = tcf.process_id
cluster_info = tcf.cluster_info

myipAddress = cluster_info[str(pid)][0]
myport = cluster_info[str(pid)][1]

allProcesses = []
initAllProcesses = []
elec_lock = threading.Lock()
masterFlag_lock = threading.Lock()
initFlag_lock = threading.Lock()
process_lock = threading.Lock()

isMaster = False
timeserver = None

initFlag = False

global outter_server_address
global outter_server_list

global outter_masterAddr

class TimeServer(threading.Thread):
    offset = 0 
    def run(self):
        self.BerkleyTime()
    def BerkleyTime(self):
        """
        Implementation of the berkley algorthim,
        keeps track of other proceseses
        """
        global allProcesses
        print os.getpid(), "Master initializing."
        
        masterFlag_lock.acquire()
        isMasterSnapshot = isMaster
        masterFlag_lock.release()

        while True:
            time.sleep(5)
            if isMasterSnapshot:
                times = []
                process_lock.acquire()
                count = 0
                for process in allProcesses:
                    try:
                        proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))

                        t_self_tempt = os.times()[4]
                        # begin of rtt
                        t0 = time.time()
                        t_tempt = proxy.getTime()
                        t1 = time.time()
                        # end of rtt
                        times.append(t_tempt - (t1-t0)/2.0 - t_self_tempt)
                        count += 1
                    except Exception as e:
                        #removeProcess(process)
                        times.append(0)
                        continue
                if count > 0:
                    average = sum(times)/count
                else:
                    average = 0

                count = 0
                for process in allProcesses:
                    try:
                        proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str(process[1]))
                        #index = allProcesses.index(process)
                        #print "Setting offset for process", ()
                        proxy.setOffset(times[count] - average)
                    except IndexError:
                        pass
                    except Exception as e:
                        pass
                        #index = allProcesses.index(process)
                        #removeProcess(process)
                        #del times[index]
                    count += 1
                process_lock.release()

            masterFlag_lock.acquire()
            isMasterSnapshot = isMaster
            masterFlag_lock.release()

class ServerRequestThread(threading.Thread):
    """
    Launches xml async server
    """
    def run(self):
        global port
        global myport
        global pid

        print "starting server on", myport
        server = SimpleXMLRPCServer(('',myport)) # single thread server
        #server = AsyncXMLRPCServer(('', myport), SimpleXMLRPCRequestHandler)
        server.register_function(election, "election")
        server.register_function(amongstTheLiving, "amongstTheLiving")
        server.register_function(registerProcess, "registerProcess")
        server.register_function(setOffset, "setOffset")
        server.register_function(getTime, "getTime")
        server.register_function(getOffset, "getOffset")
        server.register_function(amIMaster, "amIMaster")
        server.register_function(remoteClaimIWon, "remoteClaimIWon")
        server.serve_forever()

class heartbeat(threading.Thread):
    def run(self):
        global masterIP
        global port
        global myport
        global allProcesses
        global myipAddress
        global pid
        global cluster_info
        global initFlag
        self.proxy = xmlrpclib.ServerProxy("http://" + masterIP + ":"+ str( port )) #proxy to master port
            
        # initialization
        for i in cluster_info:
            allProcesses = registerProcess(cluster_info[i][0], cluster_info[i][1], int(i))
        
        initAllProcesses = allProcesses[:]

        initFlag_lock.acquire()
        initFlag = True
        initFlag_lock.release()
        election()
        while True:
            time.sleep(15+(np.random.rand(1)[0]-0.5)*5)

            masterFlag_lock.acquire()
            isMasterSnapshot = isMaster
            masterFlag_lock.release()

            # our election algorithm guarantees the correctness under the following assumptions:
            # 1. if machine a connects with machine b, then a, and b must be mutually connected
            # 2. if a connects with b, and c, then b must connects with c
            if isMasterSnapshot:
                # in case "Some disconnected process reconnects; you can test this by letting a machine go off line and then reconnect it"
                print '++++++Master Show Time'
                largest_master_flag = True
                two_masters_flag = False

                process_lock.acquire()
                for process in allProcesses: # check whether disconnected process reconnect.
                    try:
                        proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":"+ str( process[1] ))
                        IamMaster = proxy.amIMaster()
                        if IamMaster and process[2] != pid:
                            two_masters_flag = True
                        if IamMaster and process[2] > pid:
                            largest_master_flag = False
                            break
                    except:
                        continue
                process_lock.release()

                if largest_master_flag and two_masters_flag:
                    time.sleep(2)
                    election()

            else:
                print '++++++Slave Show Time'
                # in case "The master get disconnected"
                try:
                    print "Contacting master..." # check whether the master is disconnected.
                    elec_lock.acquire()
                    masterIPSnapshort = masterIP
                    portSnapshort = port
                    elec_lock.release()

                    self.proxy = xmlrpclib.ServerProxy("http://" + masterIPSnapshort + ":"+ str( portSnapshort )) #proxy to master port                    
                    self.proxy.amongstTheLiving()
             #       process_lock.acquire()
             #       allProcesses = self.proxy.registerProcess(myipAddress, myport, pid)
             #       process_lock.release()
                except Exception as e:
                    print e
                    election()

def amongstTheLiving(x):
    return True
            
def registerProcess(ipAddress,port,pid):
    """
    Makes the master process aware of the slave process
    Returns IP and port of other slaves.
    """
    global allProcesses
    if (ipAddress,port,pid) not in allProcesses:
        allProcesses.append((ipAddress,port,pid))
    return allProcesses

def removeProcess(process):
    allProcesses.remove(process)

def election():
    """
    Bully election algorithm
    Elects new master if the current process dies
    The logic is correct, but there are maybe a lot of unecessary repetion of election requests; it is better to remove such repetion for achieving better performance
    """
    global allProcesses
    global isMaster
    global myport
    global initFlag
    global masterIP
    global port
    
    initFlag_lock.acquire()
    initFlagSnapshot = initFlag
    initFlag_lock.release()
    if not initFlag:
        return 'NOK'

    print "Starting election: ", pid
    #print allProcesses
    winner = True

    process_lock.acquire()
    for process in allProcesses:
        if pid >= process[2]:
            continue
        try:
            proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str( process[1] ))
            result = proxy.election()
            if result == 'NOK':
                continue
            
            winner = False
            masterFlag_lock.acquire()
            isMaster = False
            masterFlag_lock.release()
            break
        except Exception as e:
            print e
            continue
    process_lock.release()
    if winner:
        print "Won Election"

        #elec_lock.acquire()
        masterFlag_lock.acquire()
        isMaster = True
        masterFlag_lock.release()

        global timeserver

        if timeserver == None:
            timeserver = TimeServer()
            timeserver.daemon = True
            timeserver.start()

        process_lock.acquire()
        for process in allProcesses:
            print process
            if pid <= process[2]:
                continue
            try:
                proxy = xmlrpclib.ServerProxy("http://" + process[0] + ":" + str( process[1] ))
                result = proxy.remoteClaimIWon(myipAddress, myport, outter_server_address)
            except Exception as e:
                print e
                continue
        process_lock.release()
        #elec_lock.release()
        for s in outter_server_list:
            try:
                s.invalidate_whole_cache()
            except Exception as e:
                print e
                time.sleep(0.1)
                try:
                    s.invalidate_whole_cache()
                except:
                    pass
                
        return "IWON"
    masterFlag_lock.acquire()
    isMaster = False
    masterFlag_lock.release()
    print "Replying OK"
    return "OK"

def amIMaster():
    masterFlag_lock.acquire()
    isMasterSnapshot = isMaster
    masterFlag_lock.release()
    return isMasterSnapshot

def remoteClaimIWon(mIP, masterPort, outter_addr):
    global port
    global isMaster
    global masterIP
    global outter_masterAddr

    elec_lock.acquire()
    masterIP = mIP
    port = masterPort

    outter_masterAddr = outter_addr
    elec_lock.release()

    masterFlag_lock.acquire()
#    print fe.myipAddress
#    print fe.myport
    print 'I am claimed not master'
    isMaster = False
    masterFlag_lock.release()
    return True

def amongstTheLiving():
    return True

def setOffset(offset):
#    print os.getpid(), "offset set to:", offset
    
    TimeServer.offset = offset
    return True

def getOffset():
    return os.times()[4] - TimeServer.offset

def getTime():
    return os.times()[4]

def SetupServer(address, server_list):
    global outter_server_address
    global outter_server_list

    outter_server_address = address
    outter_server_list = server_list
    s = ServerRequestThread()
    s.daemon = True
    try:
        s.start()
    except Exception as e:
        #print 'wzd'
        print e
        return False
    time.sleep(5)

    h = heartbeat()
    h.daemon = True
    h.start()
    return True

def getIsMasterFlag():
    global isMaster
    masterFlag_lock.acquire()
    isMasterSnapshot = isMaster
    masterFlag_lock.release()

    return isMasterSnapshot

def getMasterAddress():
    global masterIP
    global port
    elec_lock.acquire()
    masterIPSnapshort = masterIP
    portSnapshort = port

    masterOutterAddrSnapshort = outter_masterAddr
    elec_lock.release()
    return ((masterIPSnapshort, portSnapshort), masterOutterAddrSnapshort)

if __name__ == '__main__':
    SetupServer()
    time.sleep(10)
    while True:
        time.sleep(2)
        masterFlag_lock.acquire()
        isMasterSnapshot = isMaster
        masterFlag_lock.release()
        if not isMasterSnapshot:
            print 'offset of ', pid, ' is ', getOffset()
        else:
            print 'I am master with id ', pid
