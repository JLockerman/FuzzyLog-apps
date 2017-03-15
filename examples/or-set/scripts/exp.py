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
	
	log_ips = sys.argv[1]
	client = int(sys.argv[2])
	window_sz = int(sys.argv[3])
	os.chdir(os.getenv('DELOS_ORSET_LOC'))
	single_expt(client, window_sz, log_ip)
	# clients = [1, 4, 8, 12, 16]
	# window_sz = [32]
	# for c in clients:
 	#		for w in window_sz:
	#		single_expt(c, w, log_ip)				
	
# Start the fuzzy log. 
def init_fuzzy_log():
	original_dir = os.getcwd()
	prefix = os.environ['DELOS_RUST_LOC']	
	log_bin_path = os.path.join(prefix, 'servers/tcp_server')
	os.chdir(log_bin_path)
	os.system('cargo build --release') 
	proc = subprocess.Popen(['target/release/delos_tcp_server', '3333', '-w', '10'])
	os.chdir(original_dir)
	time.sleep(5)	
	return proc
	
# Start clients.
def init_clients(num_clients, window_sz, log_ip):
	args = ['./build/or-set', '--log_addr', log_ip, '--expt_duration', '30', '--expt_range', '1000000', 
	 	'--num_rqs', '30000000', '--sample_interval', '1', '--sync_duration', '500', '--server_id'] 
	client_procs = []
	for i in range(0, num_clients):
		client_args = list(args)
		client_args.append(str(i))
		client_args.append('--num_clients')
		client_args.append(str(num_clients))
		client_args.append('--window_sz')
		client_args.append(str(window_sz))
		proc = subprocess.Popen(client_args)
		client_procs.append(proc)
	return client_procs		
	
def get_resultdir_name(num_clients, window_sz):
	result_dir = 'results'
	expt_dir = 'c' + str(num_clients) + '_w' + str(window_sz)
	return os.path.join(result_dir, expt_dir)

def mv_results(num_clients, window_sz):
	result_dir = get_resultdir_name(num_clients, window_sz)	
	os.system('mkdir -p ' + result_dir)
	os.system('mv *.txt ' + result_dir)

# Single experiment. 
def single_expt(num_clients, window_sz, log_ip):
	os.system('rm *.txt')
	# log_proc = init_fuzzy_log()
	client_procs = init_clients(num_clients, window_sz, log_ip)
	for c in client_procs:
		c.wait()
	# log_proc.kill()
	mv_results(num_clients, window_sz)

if __name__ == "__main__":
    main()
