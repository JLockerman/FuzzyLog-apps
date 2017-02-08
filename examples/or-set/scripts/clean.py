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
	return results[10:110]

def postprocess_results(vals):
    sample_mean = sum(vals) / float(len(vals))
    dev = 0
    for v in vals:
        dev += (sample_mean - v) * (sample_mean - v)
    s = math.sqrt(dev / float(len(vals)))
    conf_min = sample_mean - 1.96 * s
    conf_max = sample_mean + 1.96 * s
    return [sample_mean/float(1000), conf_min/float(1000), conf_max/float(1000)]

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

def main():
	os.chdir('..')
	client_range = [1,4,8,12,16]	
	sync_duration = [500, 300000000]
	
	for s in sync_duration:
		expt_results(client_range, s)	

if __name__ == "__main__":
    main()
	
	
