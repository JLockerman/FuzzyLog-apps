from fabric.api import run 
from fabric.api import cd
from fabric.api import prefix
def ls_test(): run('ls')

def kill_fuzzylog():
	run('killall delos_tcp_server')

def kill_crdts():
	run('killall or-set')

def fuzzylog_proc(port, server_index, numservers):
	with cd('fuzzylog/delos-rust/servers/tcp_server'):
		run('target/release/delos_tcp_server ' + str(port) + ' -ig ' + str(server_index) + ':' + str(numservers) + ' > log.txt 2>&1')

def check_network_statistics():
	run('netstat -i')

def clean_crdt():
	with cd('fuzzylog/delos-apps/examples/or-set'):
		run('rm *.txt')

def run_crdt_clients(log_addr, start_clients, num_clients, window_sz, duration, total_clients, 
		     low_throughput, 
		     high_throughput, 
		     spike_start,
		     spike_duration):
	with cd('fuzzylog/delos-apps/examples/or-set'):
		#with prefix('export RUST_LOG=fuzzy_log'):
		args = 'scripts/exp.py ' + str(log_addr) + ' ' + str(start_clients) + ' ' + str(num_clients) + ' ' + str(window_sz) + ' ' + str(duration) + ' ' + str(total_clients) + ' ' + str(low_throughput) + ' ' + str(high_throughput) + ' ' + str(spike_start) + ' ' + str(spike_duration)
		run(args)

def enable_logging():
	run('export RUST_LOG=fuzzy_log')

def crdt_proc(log_addr, duration, exp_range, server_id, sync_duration,
	      num_clients, window_sz, num_rqs, sample_interval):
	with cd('fuzzylog/delos-apps/examples/or-set'):
		with prefix('export RUST_BACKTRACE=1'):
			args = 'build/or-set '
			args += '--reader ' 
			args += '--log_addr ' + str(log_addr) + ' '		
			args += '--expt_duration ' + str(duration) + ' ' 
			args += '--expt_range ' + str(exp_range) + ' ' 
			args += '--server_id ' + str(server_id) + ' '
			args += '--sync_duration ' + str(sync_duration) + ' '
			args += '--num_rqs ' + str(num_rqs) + ' ' 
			args += '--window_sz ' + str(window_sz) + ' ' 
			args += '--sample_interval ' + str(sample_interval) + ' ' 
			args += '--num_clients ' + str(num_clients) + ' '
			args += '--low_throughput ' + str(30) + ' '
			args += '--high_throughput ' + str(30) + ' ' 
			args += '--spike_start ' + str(30) + ' ' 
			args += '--spike_duration ' + str(30) + ' '
			args += ' > getter_log.txt 2>&1'
			run(args)
