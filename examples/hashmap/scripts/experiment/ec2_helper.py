#!/usr/bin/python

import boto
import boto.ec2
from boto.ec2 import EC2Connection
from boto.vpc import VPCConnection

import os
import sys
import subprocess
import time

import settings

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
	return reservation 

# Stop the specified list of instances.
def stop_instances(region, instance_list):
        if not instance_list:
                return
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

def get_filtered_servers(region, filters):
	ec2 = boto.ec2.connect_to_region(region)
        reservations = ec2.get_all_reservations(filters=filters)
	filtered = []
	for r in reservations:
		for i in r.instances:
		        filtered.append(i.id)
	return filtered 

def get_fuzzylog_servers(region):
        server_type = "fuzzylog server"
        filters = { "tag:Type": server_type }
        return get_filtered_servers(region, filters)

def get_fuzzylog_lockserver(region):
        server_type = "fuzzylog lockserver"
        filters = { "tag:Type": server_type }
        return get_filtered_servers(region, filters)

def get_fuzzylog_clients(region):
        server_type = "fuzzylog client"
        filters = { "tag:Type": server_type }
        return get_filtered_servers(region, filters)

def start_and_test_fuzzylog_lock_server(region):
        instance_ids = get_fuzzylog_lockserver(region)
        if not instance_ids:
                return [], []
        instances = start_instances(region, instance_ids)
        # test servers
        instance_ips = to_ip(instances)
        test_instances(instance_ips, settings.KEYFILE[region])
        return instances, instance_ips

def start_and_test_fuzzylog_servers(region):
        instance_ids = get_fuzzylog_servers(region)
        if not instance_ids:
                return [], []
        instances = start_instances(region, instance_ids)
        # test servers
        instance_ips = to_ip(instances)
        test_instances(instance_ips, settings.KEYFILE[region])
        return instances, instance_ips

def start_and_test_fuzzylog_clients(region):
        instance_ids = get_fuzzylog_clients(region)
        if not instance_ids:
                return [], []
        instances = start_instances(region, instance_ids)
        # test servers
        instance_ips = to_ip(instances)
        test_instances(instance_ips, settings.KEYFILE[region])
        return instances, instance_ips


def to_ip(instances):
        return list(map(lambda x: {'public' : x.ip_address, 'private' : x.private_ip_address}, instances))

def addr_to_str(ips, port): 
        # escape comma
        return "\,".join(["{0}:{1}".format(s['public'], port) for s in ips]) 
