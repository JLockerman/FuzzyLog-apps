#!/usr/local/bin/python2.7

import os
import subprocess
import sys
import time

# server_ip = '172.31.5.245:13289#172.31.3.165:13289^172.31.6.77:13289#172.31.9.164:13289'
server_ip = '172.31.5.245:13289#172.31.3.165:13289'
client_hostnames = ['ec2-34-229-95-251.compute-1.amazonaws.com', 'ec2-54-242-35-194.compute-1.amazonaws.com', 'ec2-52-90-229-51.compute-1.amazonaws.com', 'ec2-34-229-144-94.compute-1.amazonaws.com', 'ec2-107-21-51-241.compute-1.amazonaws.com', 'ec2-35-172-215-94.compute-1.amazonaws.com', 'ec2-35-172-180-160.compute-1.amazonaws.com', 'ec2-54-209-170-86.compute-1.amazonaws.com', 'ec2-34-226-193-228.compute-1.amazonaws.com', 'ec2-54-146-158-199.compute-1.amazonaws.com', 'ec2-34-207-110-50.compute-1.amazonaws.com', 'ec2-54-172-124-53.compute-1.amazonaws.com', 'ec2-35-172-213-36.compute-1.amazonaws.com', 'ec2-52-90-2-158.compute-1.amazonaws.com']

build = 'mirror: cd ./examples/java-client2 && ant compile'

server_hostnames = 'ec2-35-172-211-39.compute-1.amazonaws.com,ec2-54-152-134-66.compute-1.amazonaws.com,ec2-34-229-17-129.compute-1.amazonaws.com,ec2-34-201-148-60.compute-1.amazonaws.com,ec2-52-87-186-207.compute-1.amazonaws.com,ec2-107-23-170-24.compute-1.amazonaws.com'
# ips = '172.31.5.245#172.31.0.84#172.31.3.165^172.31.6.77#172.31.4.131#172.31.9.164'
ips = '172.31.5.245#172.31.0.84#172.31.3.165'


zookeep_dir = os.getcwd()
num_handles = len(client_hostnames)

for i in [1]: #range(0, 15): #[5, 0, 2]: #range(0, 6):
    print("run " + str(i))

    # command = "run_chain:chain_hosts=" + ips + ",trace=fuzzy_log"
    command = "run_chain:chain_hosts=" + ips + ",nt=t"#,stats=t"
    print(command)
    os.chdir('/Users/j/Desktop/rust/delos-rust-replication')
    p0 = subprocess.Popen(["fab", "-f", "./scripts/mirror_on_servers.py", "-H", server_hostnames, command])
    os.chdir(zookeep_dir)


    ########################################


    command = 'ex:pkill java_proxy; cd ./examples/java-zk/ && ant' + \
    ' -Dhostname\=' + server_ip + \
    ' -Dcolor\=#server_num' + \
    ' -Dothercolor\=' + str(num_handles - i) + \
    ' -Dtesttype\=1' + \
    ' run'
    print(command)

    client_hoststring = ""
    for name in client_hostnames[i:-1]:
        client_hoststring += name + ","
    client_hoststring += client_hostnames[-1]

    time.sleep(5)

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
