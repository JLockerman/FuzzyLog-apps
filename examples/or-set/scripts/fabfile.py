from fabric.api import run 
from fabric.api import cd

def ls_test():
	run('ls')

def kill_fuzzylog():
	run('killall tcp_server')

def kill_crdts():
	run('killall or-set')

def fuzzylog_proc(port, maxthreads):
	with cd('fuzzylog/delos-rust/servers/tcp_server'):
		if maxthreads == '-1':
			run('target/release/tcp_server ' + str(port))
		else:
			run('target/release/tcp_server ' + str(port) + ' -w ' + str(maxthreads))

def clean_crdt():
	with cd('fuzzylog/delos-apps/examples/or-set'):
		run('rm *.txt')

def crdt_proc(log_addr, duration, exp_range, server_id, sync_duration,
	      num_clients, window_sz, num_rqs, sample_interval):
	with cd('fuzzylog/delos-apps/examples/or-set'):
		args = 'build/or-set '
		args += '--log_addr ' + str(log_addr) + ' '		
		args += '--expt_duration ' + str(duration) + ' ' 
		args += '--expt_range ' + str(exp_range) + ' ' 
		args += '--server_id ' + str(server_id) + ' '
		args += '--sync_duration ' + str(sync_duration)
		args += '--num_clients ' + str(num_clients)
		args += '--window_sz ' + str(window_sz)
		args += '--num_rqs ' + str(num_rqs)
		args += '--sample_interval ' + str(sample_interval)
		run(args)
