#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

# server_ip = '172.31.9.164'
server_ip = '172.31.3.165:13289#172.31.6.77:13289'
client_hostnames = ['ec2-34-234-91-55.compute-1.amazonaws.com', 'ec2-54-146-67-97.compute-1.amazonaws.com', 'ec2-34-228-70-65.compute-1.amazonaws.com', 'ec2-54-209-144-1.compute-1.amazonaws.com', 'ec2-54-236-251-108.compute-1.amazonaws.com']

build = 'mirror: cd ./examples/java-client2 && ant compile'

server_hostnames = 'ec2-34-203-200-118.compute-1.amazonaws.com,ec2-34-229-164-2.compute-1.amazonaws.com'
ips = "172.31.3.165#172.31.6.77"
#ips = "172.31.5.245#172.31.3.165"


zookeep_dir = os.getcwd()
num_handles = len(client_hostnames)

for i in [.8, .9, .999, 1.1, 1.2, 1.3, 1.4]: #[0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 2.0]:
    #print("> s " + str(i))

    client_hoststring = ""
    for name in client_hostnames[:-1]:
        client_hoststring += name + ","
    client_hoststring += client_hostnames[-1]

    build_proxy = 'mirror:cd examples/java_proxy && cargo build --release'

    command = "run_chain:chain_hosts=" + ips + ",nt=t"
    print(command)
    os.chdir('/Users/j/Desktop/rust/delos-rust-replication')
    p0 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", server_hostnames, command])
    pb = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", client_hoststring, build_proxy])
    pb.wait()
    os.chdir(zookeep_dir)


    ########################################

    #pkill java;
    command = 'ex:pkill java; pkill java_proxy; cd ./examples/java-zk/ && ant' + \
    ' -Dhostname\=' + server_ip + \
    ' -Dclient_num\=#server_num' + \
    ' -Dnum_files\=' + str(i) + \
    ' -Dsend_interval\=3300' + \
    ' view'
    print(command)

    time.sleep(3)

    p1 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", client_hoststring,
        build,
        command])

    p1.wait()
    p0.kill()

    print("")
    print("> ")
    print("========================================")
    print("> ----------------------------------------")
    print("========================================")
    print("> ")
    print("")
    sys.stdout.flush()

p0 = subprocess.Popen(["fab", "-f", "./mirror_on_servers.py", "-H", server_hostnames, "kill_server"])