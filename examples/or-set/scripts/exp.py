#!/usr/bin/python

import os
import subprocess
import time
import sys

def main():
	if os.getenv('DELOS_RUST_LOC') == None:
		print 'The DELOS_RUST_LOC environment variable must point to the top level of the delos-rust repo'
		sys.exit()
	if os.getenv('DELOS_ORSET_LOC') == None:
		print 'The DELOS_ORSET_LOC environment variable must point to the top level of the or-set example directory'	
		sys.exit()
	
	os.chdir(os.getenv('DELOS_ORSET_LOC'))
	clients = [1,4,8,12,16,20,24,28,32]
	sync_duration = [500000, 300000000]	
	for c in clients:
		for s in sync_duration:
			single_expt(c, s)				
	
# Start the fuzzy log. 
def init_fuzzy_log():
	original_dir = os.getcwd()
	prefix = os.environ['DELOS_RUST_LOC']	
	log_bin_path = os.path.join(prefix, 'servers/tcp_server')
	os.chdir(log_bin_path)
	os.system('cargo build --release') 
	proc = subprocess.Popen(['target/release/tcp_server', '3333', '-w', '10'])
	os.chdir(original_dir)
	time.sleep(5)	
	return proc
	
# Start clients.
def init_clients(num_clients, sync_duration):
	args = ['./build/or-set', '--log_addr', '127.0.0.1:3333', '--expt_duration', '120', '--expt_range', '1000', '--server_id']
	client_procs = []
	for i in range(0, num_clients):
		client_args = list(args)
		client_args.append(str(i))
		client_args.append('--sync_duration')
		client_args.append(str(sync_duration))
		proc = subprocess.Popen(client_args)
		client_procs.append(proc)
	return client_procs		
	
def get_resultdir_name(num_clients, sync_duration):
	result_dir = 'results'
	expt_dir = 'c' + str(num_clients) + '_d' + str(sync_duration)
	return os.path.join(result_dir, expt_dir)

def mv_results(num_clients, sync_duration):
	result_dir = get_resultdir_name(num_clients, sync_duration)	
	os.system('mkdir -p ' + result_dir)
	os.system('mv *.txt ' + result_dir)

# Single experiment. 
def single_expt(num_clients, sync_duration):
	os.system('rm *.txt')
	log_proc = init_fuzzy_log()
	client_procs = init_clients(num_clients, sync_duration)
	for c in client_procs:
		c.wait()
	log_proc.kill()
	mv_results(num_clients, sync_duration)

if __name__ == "__main__":
    main()
