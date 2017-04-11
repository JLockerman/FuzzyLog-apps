#!/usr/bin/python

import os
import subprocess
import time
import sys

def main():
	head_log_ips = sys.argv[1]
	tail_log_ips = sys.argv[2]
	start_clients = int(sys.argv[3])
	clients_on_machine = int(sys.argv[4])
	window_sz = int(sys.argv[5])
	duration = int(sys.argv[6])
	total_clients = int(sys.argv[7])
	low_throughput = float(sys.argv[8])
	high_throughput = float(sys.argv[9])
	spike_start = float(sys.argv[10])
	spike_duration = float(sys.argv[11])
	single_expt(start_clients, clients_on_machine, window_sz, head_log_ips, tail_log_ips, duration, total_clients, 
		   low_throughput, 
	  	   high_throughput, 
		   spike_start,
		   spike_duration)
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
def init_clients(start_clients, num_clients, window_sz, head_log_ip, tail_log_ip, duration, total_clients, low_throughput,
		 high_throughput, spike_start, spike_duration):
	args = ['./build/or-set', '--writer', '--head_log_addr', head_log_ip, '--tail_log_addr', tail_log_ip, '--expt_range', '1000000', 
	 	'--num_rqs', '30000000', '--sample_interval', '1', '--sync_duration', '500', '--server_id'] 
	client_procs = []
	log_files = []
	for i in range(0, num_clients):
		client_args = list(args)
		client_args.append(str(i+start_clients))
		logf = str(i+start_clients) + '_log.txt'	
		log_handle = open(logf, 'w')
		log_files.append(log_handle)
		client_args.append('--num_clients')
		client_args.append(str(total_clients))
		client_args.append('--window_sz')
		client_args.append(str(window_sz))
		client_args.append('--expt_duration')
		client_args.append(str(duration))
		client_args.append(str('--low_throughput'))
		client_args.append(str(low_throughput))	
		client_args.append(str('--high_throughput'))
		client_args.append(str(high_throughput))	
		client_args.append(str('--spike_start'))
		client_args.append(str(spike_start))	
		client_args.append(str('--spike_duration'))
		client_args.append(str(spike_duration))
		proc = subprocess.Popen(client_args, stderr=log_handle, stdout=log_handle)
		client_procs.append(proc)
	return zip(client_procs, log_files)		
	
def get_resultdir_name(num_clients, window_sz):
	result_dir = 'results'
	expt_dir = 'c' + str(num_clients) + '_w' + str(window_sz)
	return os.path.join(result_dir, expt_dir)

def mv_results(num_clients, window_sz):
	result_dir = get_resultdir_name(num_clients, window_sz)	
	os.system('mkdir -p ' + result_dir)
	os.system('mv *.txt ' + result_dir)

# Single experiment. 
def single_expt(start_clients, num_clients, window_sz, head_log_ip, tail_log_ip, duration, total_clients, 
		low_throughput, high_throughput, spike_start, spike_duration):
	os.system('rm *.txt')
	# log_proc = init_fuzzy_log()
	client_procs = init_clients(start_clients, num_clients, window_sz, head_log_ip, tail_log_ip, duration, total_clients, 
				   low_throughput, high_throughput, spike_start, spike_duration)
	for c, f in client_procs:
		c.wait()
		f.close()
	# log_proc.kill()
#	mv_results(num_clients, window_sz)

if __name__ == "__main__":
    main()
