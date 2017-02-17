#!/usr/bin/python

import boto
import boto.ec2
from boto.ec2 import EC2Connection
from boto.vpc import VPCConnection

import os
import sys
import subprocess
import time

# Default settings for starting new instances.
# XXX Should probably go into a config file. 
SUBNET_ID = 'subnet-95bce7b8' 
SECURITY_GROUP = 'sg-1d531961' 
IMAGE_ID = 'ami-71cd0567'
KEYFILE = '~/.ssh/jmfaleiro.pem'
REGION = 'us-east-1'
INSTACE_TYPE = 'm4.16xlarge'

# Returns a list of running instances. 
def get_running_instances(region):
	ec2 = boto.ec2.connect_to_region(region)
	reservations = ec2.get_all_reservations()
	running = []
	for r in reservations:
		for i in r.instances:
			if i.state == 'running':
				running.append(i)
	return running	

# Returns a list of stopped instance ids.
def get_stopped_instances(region):
	ec2 = boto.ec2.connect_to_region(region)	
	reservations = ec2.get_all_reservations()	
	stopped = []
	for r in reservations:
		for i in r.instances:
			if i.state == 'stopped':
				stopped.append(i.id)
	return stopped

# Start a list of stopped instances.
def start_instances(region, instance_ids):
	ec2 = boto.ec2.connect_to_region(region)
	instances = ec2.start_instances(instance_ids=instance_ids)
	
	started = True
	while True:
		for i in instances:
			i.update()
			if i.state != 'running':
				started = False	
		if started == False:
			started = True
			time.sleep(5)
		else:
			break
	return instances

# Launch an EC2 instance. 
def launch_instance(region, image_id, instance_type, subnet_id, security_group):
	ec2 = boto.ec2.connect_to_region(region)

	reservation = ec2.run_instances(image_id=image_id, min_count=1, max_count=1, 
		          	      instance_type=instance_type, 
			  	      subnet_id=subnet_id,
				      security_group_ids=[security_group])
	print reservation
	return reservation 

# Stop the specified list of instances.
def stop_instances(region, instance_list):
	ec2 = boto.ec2.connect_to_region(region)	
	instance_ids = list(map(lambda x: x.id, instance_list)) 
	stopped = ec2.stop_instances(instance_ids=instance_ids)
	while True:
		try_again = False
		for inst in stopped:
			inst.update()
			if inst.state != 'stopped':
				try_again = True
		if try_again == True:
			time.sleep(2)
		else:
			break
			

# Launch a CRDT applicatino process.
def launch_crdt(fabhost, keyfile, logaddr, duration, exp_range, server_id, sync_duration):
	launch_str = 'crdt_proc:' + logaddr + ',' + str(duration)
	launch_str += ',' + str(exp_range)
	launch_str += ',' + str(server_id)
	launch_str += ',' + str(sync_duration)
	print keyfile
	return subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', fabhost, launch_str])
	

# Launch fuzzy log. 
def launch_fuzzylog(fabhost, keyfile, port, nthreads=-1):
	launch_str = 'fuzzylog_proc:' + str(port) + ',' + str(nthreads)
	print keyfile
	proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', fabhost, launch_str])  
	return proc

# Run CRDT experiment on specified instances. First instance hosts the log,
# every other instance hosts clients.  
#
# Fabric only supports synchronous calls to hosts. Create a process per-remote call, 
# either corresponding to a client or server. Manage clients and servers by joining/killing
# these processes.  
def fuzzylog_exp(instances, num_clients):
	logaddr = instances[0]['private']+ ':3333'
	fabhost_prefix = 'ubuntu@' 
	keyfile = '~/.ssh/jmfaleiro.pem'
	
	os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + instances[1]['public'] + ' clean_crdt')
	os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + instances[1]['public'] + ' kill_fuzzylog')
	os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + instances[1]['public'] + ' kill_crdts')

	log_proc = launch_fuzzylog(fabhost_prefix+instances[0]['public'], keyfile, 3333) 
	time.sleep(5)	
	client_procs = []

	for i in range(0, num_clients):
		client_procs.append(launch_crdt(fabhost_prefix+instances[1]['public'], keyfile, logaddr, 150, 1000, i, 50000)) 	
		time.sleep(1)
	
	for c in client_procs:
		c.wait()
	
	log_proc.kill()

	for i in range(0, num_clients):
		os.system('scp -i ~/.ssh/jmfaleiro.pem -o StrictHostKeyChecking=no ' + fabhost_prefix+instances[1]['public'] + ':~/fuzzylog/delos-apps/examples/or-set/*.txt .')	

# Start stopped instances.
def wakeup_instances(region):
	instance_ids = get_stopped_instances(region)
	return start_instances(region, instance_ids)

# Launch a set of new EC2 instances. 
def launch_instances(num_instances, region, instance_type, image_type):
	instances = []
	reservations = []
	for i in range(0, num_instances):
		reservations.append(launch_instance(region, instance_type, image_type))
	for res in reservations:
		instances += res.instances

def test_proc(ipaddr, keyfile):
	login = 'ubuntu@' + ipaddr
	proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', login, 'ls_test'])
	return proc

def test_iteration(instance_list, keyfile):
	procs = []
	for inst in instance_list:
		procs.append(test_proc(inst['public'], keyfile))
	
	for p in procs:
		p.wait()
			
	for p in procs:
		if p.returncode != 0:
			return False 
	return True

def test_instances(instance_list, keyfile):
	while True:
		if test_iteration(instance_list, keyfile) == True:
			break 
		time.sleep(5)

def main():
	nclients = [1,4,8,12,16,20,24,28,32]
	for c in nclients:
		os.system('mkdir -p results/' + str(c))
		instance_ids = get_stopped_instances(REGION)
		instances = start_instances(REGION, instance_ids)
		instance_ips = list(map(lambda x: {'public' : x.public_dns_name, 'private' : x.private_ip_address}, instances))

		test_instances(instance_ips, KEYFILE)
		fuzzylog_exp(instance_ips, c)	
		stop_instances(REGION, instances)	
		os.system('mv *.txt results/' + str(c))

if __name__ == "__main__":
    main()
