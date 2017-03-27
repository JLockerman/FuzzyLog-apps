#!/usr/bin/python

import subprocess
import settings
from ec2_helper import *

			
def update_fuzzymap_binary(region):
	fabhost_prefix = 'ubuntu@' 
        keyfile = settings.KEYFILE[region]
        
        # update clients
        clients, client_ips = start_and_test_fuzzylog_clients(region)

        procs = []
        for c in client_ips:
                proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', fabhost_prefix + c['public'], 'update_fuzzymap_binary'])
                procs.append(proc)
        for p in procs:
                p.wait()
        stop_instances(region, clients)

def update_fuzzylog_binary(region, commit=None):
	fabhost_prefix = 'ubuntu@' 
        keyfile = settings.KEYFILE[region]

        # update servers + clients
        servers, server_ips = start_and_test_fuzzylog_servers(region)
        clients, client_ips = start_and_test_fuzzylog_clients(region)
        cmd = 'update_fuzzylog_binary'
        if commit:
                cmd += ":" + commit

        procs = [] 
        for s in server_ips + client_ips:
                proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', fabhost_prefix + s['public'], cmd])
                procs.append(proc)
        for p in procs:
                p.wait()
        stop_instances(region, servers + clients)

def main():
        update_fuzzylog_binary(settings.US_EAST_2)
        update_fuzzymap_binary(settings.US_EAST_2)

if __name__ == "__main__":
        main()
