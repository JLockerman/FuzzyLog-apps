#!/usr/bin/python

import os
import sys
import os.path
import threading
import numpy
import time
from fabric.api import run


# Command to Fuzzymap
fmt_cmd = "../build/hashmap {0} {1} {2} {3}"
tmp_result_dir = 'result'

def main():
    do_scalability_experiment()

def do_scalability_experiment():
    results = test_single_append([1, 2, 4, 8, 16, 32], 100)
    write_output(results, 'single_append.txt')

def do_multiappend_experiment():
    results = test_multiappend([0, 20, 40, 60, 80, 100], 100)
    write_output(results, 'multiappend.txt')

def write_output(results, filename):
    with open(filename, 'w') as f:
        keys = results.keys()
        keys.sort()
        for k in keys:
            samples = results[k]
            mean, low, high = postprocess_results(samples)
            line = str(k) + ' ' + str(mean) + ' ' + str(low) + ' ' + str(high) + '\n'
            f.write(line) 

def postprocess_results(vals):
    vals.sort()
    median_idx = len(vals)/2
    low_idx = int(len(vals)*0.05)
    high_idx = int(len(vals)*0.95)
    return [vals[median_idx]/float(1000), vals[low_idx]/float(1000), vals[high_idx]/float(1000)]


# Run single append test (with all stat)
def test_single_append(num_clients, sample):
    summary = {}
    for num_client in num_clients:
        s = [] 
        for i in range(sample):
            results = measure_single_append(num_client)
            print 'Num client: {0}, Run {1}'.format(num_client, i)
            val = calculate_metric(results)
            logging('single_append', num_client, i, val)
            s.append(val)
        summary[num_client] = s
    return summary

# Run multiappend test (with all stat)
def test_multiappend(multiappend_percents, sample):
    summary = {}
    for multiappend_percent in multiappend_percents:
        s = [] 
        for i in range(sample):
            results = measure_multiappend(multiappend_percent)
            print 'Multiappend percent: {0}, Run {1}'.format(multiappend_percent, i)
            val = calculate_metric(results)
            logging('multi_append', multiappend_percent, i, val)
            s.append(val)
        summary[multiappend_percent] = s
    return summary


# Run single append test (by number of clients)
def measure_single_append(num_client):
    preprocess()
    run_single_append_test(num_client)
    results = gather_raw_data(tmp_result_dir)
    postprocess()
    return results

# Run multiappend test
def measure_multiappend(percent):
    preprocess()
    run_multiappend_test(percent)
    results = gather_raw_data(tmp_result_dir)
    postprocess()
    return results
       
def calculate_metric(results):
    return sum(results) 

# Preprocess
def preprocess():
    install_prerequisites()
    start_fuzzylog_server() 
    make_result_directory(tmp_result_dir)

def install_prerequisites():
    # fabric
    return

def start_fuzzylog_server():
    os.system('fab start_fuzzylog')
    time.sleep(5)

def make_result_directory(dirname):
    os.mkdir(dirname)

# Run single append test 
def run_single_append_test(num_client):   
    clients = [(10000, i) for i in range(num_client)]
    run_single_append_clients(clients)

# Run multiappend test
def run_multiappend_test(multiappend_percent):
    clients = []
    clients.append((10000, 0, 1, multiappend_percent)) 
    clients.append((10000, 1, 0, multiappend_percent))
    run_multi_append_clients(clients)

# Gather all data from a given directory
def gather_raw_data(dirname):
    results = []
    for f in os.listdir(dirname):
        filename = os.path.join(dirname, f)
        with open(filename) as fd:
            contents = fd.readlines()
        results.append(float(contents[0])) 
    return results 

# Postprocess
def postprocess(): 
    remove_result_directory(tmp_result_dir)
    stop_fuzzylog_server()

def remove_result_directory(dirname):
    for f in os.listdir(dirname):
        os.remove(os.path.join(dirname, f))
    os.rmdir(dirname)

def stop_fuzzylog_server():
    os.system('fab stop_fuzzylog')

# For long running experiment, it's useful to keep intermediate data into log
def logging(*args):
    args = map(lambda x: str(x), args)
    line = " ".join(args)
    with open('experiment.log', 'w') as f:
        f.write(line + "\n") 

# Client threads
class SingleAppendClient(threading.Thread):
    def __init__(self, op_count, append_color):
        threading.Thread.__init__(self)
        self._op_count= op_count
        self._append_color = append_color

    def run(self):
        cmd = fmt_cmd.format(self._append_color, self._append_color, self._op_count, 0)
        os.system(cmd)

class MultiAppendClient(threading.Thread):
    def __init__(self, op_count, local_color, remote_color, remote_percent): 
        threading.Thread.__init__(self) 
        self._op_count = op_count
        self._local_color = local_color
        self._remote_color = remote_color
        self._remote_percent = remote_percent

    def run(self):
        multi_op_count = int(self._op_count * self._remote_percent / 100.0)
        single_op_count = self._op_count - multi_op_count 
        cmd = fmt_cmd.format(self._local_color, self._remote_color, single_op_count, multi_op_count)
        os.system(cmd)

# Run single append clients 
def run_single_append_clients(clients):
    threads = []
    for op_cnt, color in clients:
        thread = SingleAppendClient(op_cnt, color)  # only single color operation
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()

# Run single append clients 
def run_multi_append_clients(clients):
    threads = []
    for op_cnt, local_color, remote_color, remote_percent in clients:
        thread = MultiAppendClient(op_cnt, local_color, remote_color, remote_percent)  # only single color operation
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
