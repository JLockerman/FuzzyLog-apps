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
IMAGE_ID='ami-31e95b27'
KEYFILE = '~/.ssh/jmfaleiro.pem'
REGION = 'us-east-1'
SERVER_INSTANCE_TYPE = 'm4.16xlarge'
CLIENT_INSTANCE_TYPE = 'c4.2xlarge'

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
			
def terminate_instances(region, instance_list):
	ec2 = boto.ec2.connect_to_region(region)
	instance_ids = list(map(lambda x: x.id, instance_list))
	terminated = ec2.terminate_instances(instance_ids=instance_ids)
	while True:
		try_again = False
		for inst in terminated:
			inst.update()
			if inst.state != 'terminated':
				try_again = True
		if try_again == True:
			time.sleep(2)
		else:
			break	 

def terminate_all_running_instances(region):
	instance_list = get_running_instances(region)
	terminate_instances(region, instance_list)

def stop_all_running_instances(region):
	instance_list = get_running_instances(region)
	stop_instances(region, instance_list)	

def launch_client_controller(fabhost, keyfile, logaddr, start_clients, num_clients, window_sz, duration):
	launch_str = 'run_crdt_clients:' + logaddr + ',' + str(start_clients) + ',' + str(num_clients) + ',' + str(window_sz) + ',' + str(duration)	
	return subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', fabhost, launch_str])

# Launch a CRDT applicatino process.
def launch_crdt(fabhost, keyfile, logaddr, duration, exp_range, server_id, sync_duration,
		num_clients, window_sz, num_rqs, sample_interval):
	launch_str = 'crdt_proc:' + logaddr + ',' + str(duration)
	launch_str += ',' + str(exp_range)
	launch_str += ',' + str(server_id)
	launch_str += ',' + str(sync_duration)
	launch_str += ',' + str(num_clients)
	launch_str += ',' + str(window_sz)
	launch_str += ',' + str(num_rqs)
	launch_str += ',' + str(sample_interval)
	return subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', fabhost, launch_str])
	


def netstat_proc(ip, keyfile, filehandle):
	login = 'ubuntu@' + ip
	proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', login, 'check_network_statistics'], stdout=filehandle, stderr=filehandle) 
	return proc

def check_statistics(server_ips, keyfile):
	os.system('mkdir -p stats')
	logfile_fmt = 's{0}_{1}' 	
	logfiles = []
	for i in range(0, len(server_ips)):
		filename = logfile_fmt.format(str(len(server_ips)), str(i))
		filehandle = open(os.path.join('stats', filename), 'a') 	
		logfiles.append(filehandle)
	
	procs = []
	for server, handle in zip(server_ips, logfiles):
		netstat_proc(server, keyfile, handle)	
	
	for p in procs:
		p.wait()

# Launch fuzzy log. 
def launch_fuzzylog(fabhost, keyfile, port, nthreads=-1):
	launch_str = 'fuzzylog_proc:' + str(port) + ',' + str(nthreads)
	proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', fabhost, launch_str])  
	return proc

# Run CRDT experiment on specified instances. First instance hosts the log,
# every other instance hosts clients.  
#
# Fabric only supports synchronous calls to hosts. Create a process per-remote call, 
# either corresponding to a client or server. Manage clients and servers by joining/killing
# these processes.  
def fuzzylog_exp(server_instances, client_instances, clients_per_instance, window_sz, duration):
	sync_duration = 300
	exp_range = 1000000
	num_rqs = 30000000
	sample_interval = 1

	logaddr = ''
	for i in range(0, len(server_instances)):
		logaddr += server_instances[i]['private']+':3333'
		if i < len(server_instances)-1:
			logaddr += '\\,'

	fabhost_prefix = 'ubuntu@' 
	keyfile = '~/.ssh/jmfaleiro.pem'
	
	for i in range(0, len(server_instances)):
		os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + server_instances[i]['public'] + ' kill_fuzzylog')

	for i in range(0, len(client_instances)):
		os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + client_instances[i]['public'] + ' clean_crdt')
		os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + client_instances[i]['public'] + ' kill_crdts')
		os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + client_instances[i]['public'] + ' enable_logging')

	log_procs = []
	for i in range(0, len(server_instances)):
		log_procs.append(launch_fuzzylog(fabhost_prefix+server_instances[i]['public'], keyfile, 3333))	
	time.sleep(5)	

	client_procs = []
	for i in range(0, len(client_instances)):
		start_c = i * clients_per_instance
		client_proc = launch_client_controller(fabhost_prefix+client_instances[i]['public'], keyfile, logaddr, start_c, clients_per_instance, window_sz, duration)	
		client_procs.append(client_proc)
		
	server_ips = list(map(lambda x: x['public'], server_instances))	

	for i in range(0, duration):
		time.sleep(1)	
		check_statistics(server_ips, keyfile) 

	for c in client_procs:
		c.wait()

	for p in log_procs:
		p.kill()	

# Start stopped instances.
def wakeup_instances(region):
	instance_ids = get_stopped_instances(region)
	return start_instances(region, instance_ids)

# Launch a set of new EC2 instances. 
def launch_instances(num_instances, instance_type, region):
	instances = []
	reservations = []
	for i in range(0, num_instances):
		reservations.append(launch_instance(region, IMAGE_ID, instance_type, SUBNET_ID, SECURITY_GROUP))
	for res in reservations:
		instances += res.instances

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


def do_expt():
	# Start up instances for experiment. 
	client_instances = launch_instances(4, CLIENT_INSTANCE_TYPE, REGION)
	# instance_ids = get_stopped_instances(REGION)
	# instances = start_instances(REGION, instance_ids)
	server_instances = launch_instances(5, SERVER_INSTANCE_TYPE, REGION)


	client_instance_ips = list(map(lambda x: {'public' : x.public_dns_name, 'private' : x.private_ip_address}, client_instances))
	server_instance_ips = list(map(lambda x: {'public' : x.public_dns_name, 'private' : x.private_ip_address}, server_instances))
	test_instances(server_instance_ips, KEYFILE)
	test_instances(client_instance_ips, KEYFILE)
	
	os.system('mkdir results')
	
	num_clients = 4
	num_warehouses = 48 
	duration = 90

	for i in range(0, 5):	
		fuzzylog_exp(server_instance_ips[0:i+1], client_instance_ips, num_clients, num_warehouses, duration)	

		result_dir = 'c' + str(num_clients) + '_s' + str(i+1) + '_w' + str(num_warehouses)
		os.system('mkdir ' + result_dir)
		for c in client_instance_ips:
			os.system('scp -r -i ~/.ssh/jmfaleiro.pem -o StrictHostKeyChecking=no ubuntu@' + c['public'] + ':~/fuzzylog/delos-apps/examples/or-set/results/* ' + result_dir)	

	instances = client_instances + server_instances
	terminate_instances(REGION, instances)	

def main():
	do_expt()


if __name__ == "__main__":
    main()
