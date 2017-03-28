#!/usr/bin/python

import exp
import os
import math

def readfile(filename):
	results = []
	inpt_file = open(filename)
	for line in inpt_file:
		val = float(line) 
		results.append(val)
	inpt_file.close()
	return results

def postprocess_results(vals):
	vals.sort()
	median_idx = len(vals)/2
	low_idx = int(len(vals)*0.05)
	high_idx = int(len(vals)*0.95)
	return [vals[median_idx]/float(1000), vals[low_idx]/float(1000), vals[high_idx]/float(1000)]

def throughput_time(clients_per_machine, num_machines, window, server_count):
	dir_fmt = 'c{0}_s{1}_w{2}'
	file_fmt = '{0}.txt'
	results = []
	outfile = 'append_throughput.txt'
	outline = '{0}\n'
	
	dirname = dir_fmt.format(str(clients_per_machine), str(server_count), str(window))
	num_clients = clients_per_machine*num_machines
	temp = []
	for c in range(0, num_clients):
		filename = file_fmt.format(str(c))
		temp.append(readfile(os.path.join(dirname, filename)))	
	
	zipped = zip(*temp)
	summed = list(map(sum, zipped))
	
	outhandle = open(outfile, 'w')
	for s in summed:
		outhandle.write(outline.format(str(s)))
	outhandle.close()
	
def vary_servers(clients_per_machine, num_machines, window, server_list):
	outer_fmt = 'c{0}_s{1}_w{2}'
	inner_fmt = 'c{0}_w{1}'
	file_fmt = '{0}.txt'
	results = []
	outfile = 'vary_servers.txt'
	outline = '{0} {1} {2} {3}\n'

	for s in server_list:
		outer_dir = outer_fmt.format(str(clients_per_machine), str(s), str(window))
	#	inner_dir = inner_fmt.format(str(clients_per_machine), str(window))
		
		num_clients = clients_per_machine*num_machines
		temp = []
		for c in range(0, num_clients):
			filename = file_fmt.format(str(c))			
			temp.append(readfile(os.path.join(outer_dir, filename)))	
		
		zipped = zip(*temp)								
		summed = list(map(sum, zipped))
		results.append(postprocess_results(summed))	
	
	tmp = zip(server_list, results)			
	outhandle = open(outfile, 'w')
	for s, r in tmp:
		l = outline.format(str(s), str(r[0]), str(r[1]), str(r[2]))
		outhandle.write(l)
	outhandle.close()	

def latency_proc(results, filename):
	outfile = open(filename, 'w')
	results.sort()	
	sorted_results = list(map(lambda x: x*1000, results))	 
	diff = 1.0 / len(results)	
	acc = 0.0	
	for r in sorted_results:
		outfile.write(str(acc) + ' ' + str(r) + '\n')
		acc += diff

	outfile.close()			
	

def get_single_server_info(filename):
	handle = open(filename)
	lines = handle.readlines()
	io_readings = []
	for l in lines:
		pieces = l.split()	
		if 'eth0' in pieces:
			idx = pieces.index('eth0')		
			io_count = int(pieces[idx+3])
			io_readings.append(io_count)
	handle.close()
	return io_readings[len(io_readings)-1] - io_readings[0]

def get_server_stats(server_count):
	filename_format = 's{0}_{1}' 
	server_info = []
	for s in range(0, server_count):
		filename = filename_format.format(str(server_count), str(s))				
		server_info.append(get_single_server_info(os.path.join('stats', filename)))	
	return server_info	

def postprocess_server_stats(server_list):
	results = []
	for s in server_list:
		results.append(' '.join(str(x) for x in get_server_stats(s)))
	
	filename = 'server_stats.txt'
	f = open(filename, 'w')
	for s, r in zip(server_list, results):
		f.write(str(s) + ' ' + r + '\n')	
	f.close()

def latency_window(num_clients, window_sz):
	result_dir = exp.get_resultdir_name(num_clients, window_sz)
	result_files = []
	results = []
	for c in range(0, num_clients):
		temp = str(c) + '_latency.txt'
		result_files.append(temp)
	for f in result_files:
		results += readfile(os.path.join(result_dir, f))
	latency_proc(results, 'c' + str(num_clients) + '_w' + str(window_sz) + '_latency.txt')


def vary_window_single(num_clients, window_sizes):
	result_dict = {}
	for w in window_sizes:
		result_dir = exp.get_resultdir_name(num_clients, w) 
		result_files = []
		scaled_array = []
		for c in range(0, num_clients):
			temp = str(c) + ".txt"
			result_files.append(temp)
		for f in result_files:
			scaled_array.append(readfile(os.path.join(result_dir, f)))
		zipped_array = zip(*scaled_array)	
		results = map(sum, zipped_array)
		post_r = postprocess_results(results)
		result_dict[w] = post_r
	return result_dict

def vary_clients_window(window_size, client_range):
	result_dict = {}
	for num_clients in client_range:
		result_dir = exp.get_resultdir_name(num_clients, window_size)
		result_files =	[]
		scaled_array = [] 
		for c in range(0, num_clients):
			temp = str(c) + ".txt"
			result_files.append(temp) 
		for f in result_files:
			scaled_array.append(readfile(os.path.join(result_dir, f)))
		zipped_array = zip(*scaled_array)
		results = map(sum, zipped_array)
		post_r = postprocess_results(results)
		result_dict[num_clients] = post_r
	return result_dict

def single_expt(num_clients, sync_duration):
	result_dir = exp.get_resultdir_name(num_clients, sync_duration)
	result_files = os.listdir(result_dir)	
	result_array = []
	scaled_array = []
	for f in result_files:
		cur_array = readfile(os.path.join(result_dir, f))	
		result_array += cur_array	
		scaled_array.append(cur_array)	
	zipped_array = zip(*scaled_array)
	scaled_array = map(sum, zipped_array)	
	return postprocess_results(scaled_array), postprocess_results(result_array)	


def expt_results(client_range, sync_duration):
	results = {} 
	for c in client_range:
		results[c] = single_expt(c, sync_duration) 
	local_file = open("d" + str(sync_duration) + "_local.txt", 'w')
	scaled_file = open('d' + str(sync_duration) + '.txt', 'w')
	
	keys = results.keys()
	keys.sort()
	print results
	for k in keys:
		scaled, local = results[k]
		scaled_line = str(k) + ' ' + str(scaled[0]) + ' ' + str(scaled[1]) + ' ' + str(scaled[2]) + '\n'
		local_line = str(k) + ' ' + str(local[0]) + ' ' + str(local[1]) + ' ' + str(local[2]) + '\n'
		local_file.write(local_line)		
		scaled_file.write(scaled_line)

	local_file.close()

def window_results(client_range, window_sizes):
	results = {}
	vary_clients_results = {}
	for c in client_range:
		results[c] = vary_window_single(c, window_sizes)
	for c in client_range:
		outfile_name = "clients_" + str(c) + ".txt"		
		outfile = open(outfile_name, "w")
		keys = results[c].keys()
		keys.sort()
		for k in keys:
			cur_line = str(k) + ' ' + str(results[c][k][0]) + ' ' + str(results[c][k][1]) + ' ' + str(results[c][k][2]) + '\n'			
			outfile.write(cur_line)
		outfile.close()
		
	for w in window_sizes:
		vary_clients_results[w] = vary_clients_window(w, client_range)

	for w in window_sizes:
		outfile_name = "window_" + str(w) + ".txt"
		outfile = open(outfile_name, "w")
		keys = vary_clients_results[w].keys()
		keys.sort()
		for k in keys:
			cur_line = str(k) + ' ' + str(vary_clients_results[w][k][0]) + ' ' + str(vary_clients_results[w][k][1]) + ' ' + str(vary_clients_results[w][k][2]) + '\n'	
			outfile.write(cur_line)
		outfile.close()


def main():

	if os.getenv('DELOS_ORSET_LOC') == None:
		print 'The DELOS_ORSET_LOC environment variable must point to the top level of the or-set example directory'	
		sys.exit()
	
	os.chdir(os.getenv('DELOS_ORSET_LOC'))
	client_range = [1,4,8,12,16]
	window_sizes = [32]	
	window_results(client_range, window_sizes)
	#for c in client_range:
	#	for w in window_sizes:
	#		latency_window(c, w)

if __name__ == "__main__":
    main()
	
	
