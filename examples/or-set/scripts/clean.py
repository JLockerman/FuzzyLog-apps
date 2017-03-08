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
	return results

def postprocess_results(vals):
	vals.sort()
	median_idx = len(vals)/2
	low_idx = int(len(vals)*0.05)
	high_idx = int(len(vals)*0.95)
	return [vals[median_idx]/float(1000), vals[low_idx]/float(1000), vals[high_idx]/float(1000)]
	
def latency_proc(results, filename):
	outfile = open(filename, 'w')
	results.sort()	
	sorted_results = list(map(lambda x: x*100, results))	 
	diff = 1.0 / len(results)	
	acc = 0.0	
	for r in sorted_results:
		outfile.write(str(acc) + ' ' + str(r) + '\n')
		acc += diff

	outfile.close()			
	
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
		results = []
		for c in range(0, num_clients):
			temp = str(c) + ".txt"
			result_files.append(temp)
		for f in result_files:
			results += readfile(os.path.join(result_dir, f))
		post_r = postprocess_results(results)
		result_dict[w] = post_r
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
	for c in client_range:
		results[c] = vary_window_single(c, window_sizes)
	
	for c in client_range:
		outfile_name = "throughput_" + str(c) + ".txt"		
		outfile = open(outfile_name, "w")
		keys = results[c].keys()
		keys.sort()
		for k in keys:
			cur_line = str(k) + ' ' + str(results[c][k][0]) + ' ' + str(results[c][k][1]) + ' ' + str(results[c][k][2]) + '\n'			
			outfile.write(cur_line)
		outfile.close()
	

def main():

	if os.getenv('DELOS_ORSET_LOC') == None:
		print 'The DELOS_ORSET_LOC environment variable must point to the top level of the or-set example directory'	
		sys.exit()
	
	os.chdir(os.getenv('DELOS_ORSET_LOC'))
	client_range = [1]
	window_sizes = [1,2,4,8,16,32]	
	window_results(client_range, window_sizes)
	for c in client_range:
		for w in window_sizes:
			latency_window(c, w)

if __name__ == "__main__":
    main()
	
	
