#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

server_ip0 = '172.31.5.245#2181^172.31.0.84#2181^172.31.3.165#2181'
# server_ip0 = '172.31.3.165#2181'
server_ip1 = '172.31.6.77#2181^172.31.4.131#2181^172.31.9.164#2181'

client_hostnames = ['ec2-52-91-91-222.compute-1.amazonaws.com', 'ec2-52-23-230-123.compute-1.amazonaws.com', 'ec2-34-204-47-224.compute-1.amazonaws.com', 'ec2-54-89-148-10.compute-1.amazonaws.com', 'ec2-54-236-221-121.compute-1.amazonaws.com', 'ec2-34-229-165-39.compute-1.amazonaws.com', 'ec2-52-90-24-65.compute-1.amazonaws.com', 'ec2-34-228-59-94.compute-1.amazonaws.com', 'ec2-52-90-59-112.compute-1.amazonaws.com', 'ec2-54-89-74-254.compute-1.amazonaws.com', 'ec2-54-205-176-30.compute-1.amazonaws.com', 'ec2-34-229-221-167.compute-1.amazonaws.com', 'ec2-34-207-81-102.compute-1.amazonaws.com', 'ec2-34-224-39-195.compute-1.amazonaws.com']

server_hostnames0 = 'ec2-107-20-96-117.compute-1.amazonaws.com,ec2-34-224-40-99.compute-1.amazonaws.com,ec2-35-172-180-44.compute-1.amazonaws.com'
server_hostnames1 = 'ec2-34-239-247-86.compute-1.amazonaws.com,ec2-34-227-190-222.compute-1.amazonaws.com,ec2-34-228-142-63.compute-1.amazonaws.com'

# server_hostnames = 'ec2-54-86-26-60.compute-1.amazonaws.com,ec2-54-208-4-99.compute-1.amazonaws.com,ec2-34-229-177-100.compute-1.amazonaws.com,'

# ips = '172.31.5.245#172.31.0.84#172.31.3.165^172.31.6.77#172.31.4.131#172.31.9.164'
# ips = '172.31.5.245#172.31.0.84#172.31.3.165'
# ips1 = '172.31.5.245#172.31.0.84#172.31.3.165'
# ips2 = '172.31.6.77#172.31.4.131#172.31.9.164'
# ips = '172.31.5.245^172.31.6.77'
# ips = '172.31.5.245'
# ips = '172.31.5.245#172.31.3.165'
# ips = '172.31.5.245#172.31.3.165^172.31.6.77#172.31.9.164'
# ips = '172.31.5.245#172.31.3.165'

num_handles = len(client_hostnames)

for i in range(0, 14): #range(0, 14, 2): #[5, 0, 2]: #range(0, 6):
    print("run " + str(i))

    pk0 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", server_hostnames0, "try_ex:pkill java; pkill zkServer.sh; pkill java"])
    pk1 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", server_hostnames1, "try_ex:pkill java; pkill zkServer.sh; pkill java;"])

    pk0.wait()
    pk1.wait()
    time.sleep(1)

    p00 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", server_hostnames0, "zookeeper"])
    p01 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", server_hostnames1, "zookeeper"])

    time.sleep(2)

    ########################################

    hostnames = client_hostnames[i:]
    hostnames_len = len(hostnames) / 2
    hostnames0 = hostnames[:hostnames_len]
    hostnames1 = hostnames[hostnames_len:]
    # hostnames0 = hostnames
    # hostnames1 = []

    p1 = None
    p2 = None
    if len(hostnames0) > 0:
        hoststring0 = ""
        for name in hostnames0[:-1]:
            hoststring0 += name + ","
        hoststring0 += hostnames0[-1]
        command = 'mirror:pkill java; cd ./examples/java-zk/ && ant' + \
            ' -Dhostname\=' + server_ip0 + \
            ' -Dtesttype\=3 ' + \
            ' -Dclient_num\=#server_num' + \
            ' -Dnum_clients\=14' + \
            ' run'
        p1 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", hoststring0,
            command])


    if len(hostnames1) > 0:
        hoststring1 = ""
        for name in hostnames1[:-1]:
            hoststring1 += name + ","
        hoststring1 += hostnames1[-1]
        command = 'mirror:pkill java; cd ./examples/java-zk/ && ant' + \
            ' -Dhostname\=' + server_ip1 + \
            ' -Dtesttype\=3 ' + \
            ' -Dclient_num\=#server_num' + \
            ' -Dnum_clients\=14' + \
            ' run'
        p2 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", hoststring1,
            command])

    time.sleep(10)

    if p1 is not None and len(hostnames0) > 0:
        p1.wait()

    if p2 is not None and len(hostnames1) > 0:
        p2.wait()

    p00.kill()
    # p01.kill()

    time.sleep(5)

    # subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", server_hostnames, "kill_server"])

    print("")
    print("> ")
    print("========================================")
    print("> ----------------------------------------")
    print("========================================")
    print("> ")
    print("")
    sys.stdout.flush()


pk0 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", server_hostnames0, "try_ex:pkill java; pkill zkServer.sh; pkill java"])
pk1 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", server_hostnames1, "try_ex:pkill java; pkill zkServer.sh; pkill java;"])

pk0.wait()
pk1.wait()