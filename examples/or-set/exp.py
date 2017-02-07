#!/bin/python

import os

def main():
	clients = [1,4,8,12,16,20,24,28,32]
	sync_duration = [1000, 300000000]	
	for c in clients:
		for s in sync_duration:
			single_expt(c, s)				
	
# Start the fuzzy log. 
def init_fuzzy_log():
	original_dir = os.getcwd()
	prefix = os.environ['DELOS_RUST_LOC']	
	log_bin_path = os.path.combine(prefix, 'servers/tcp_server')
	os.chdir(log_bin_path)
	os.system('cargo build --release') 
	proc = subprocess.Popen(['target/release/tcp_server', '3333', '-w', '10'])
	os.chdir(original_dir)
	return proc
	
# Start clients.
def init_clients(num_clients, sync_duration):
	args = ['./build/or-set', '--log_addr', '127.0.0.1:333', '--expt_duration', '120', '--expt_range', '1000', '--server_id']
	client_procs = []
	for i in range(0, num_clients):
		client_args = list(args)
		client_args.append(str(i))
		client_args.append('--sync_duration')
		client_args.append(str(sync_duration))
		proc = subprocess.Popen(args)
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
	log_proc = init_fuzzy_log()
	client_procs = init_clients(num_clients, sync_duration)
	for c in client_procs:
		c.wait()
	log_proc.kill()
	mv_results(num_cliens, sync_duration)

if __name__ == "__main__":
    main()
