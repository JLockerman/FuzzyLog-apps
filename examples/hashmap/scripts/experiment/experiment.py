#!/usr/bin/python

import os
import sys
import os.path
import threading
import numpy
import time
from fabric.api import run


# Command to Fuzzymap
async_test_cmd = "../build/hashmap --log_addr={0} --expt_range={1} --client_id={2} --workload={3} --async --window_size={4}"
sync_test_cmd = "../build/hashmap --log_addr={0} --expt_range={1} --client_id={2} --workload={3}"
tmp_result_dir = 'result'

SERVER1 = '52.15.156.76:9990'
SERVER2 = '52.14.1.7:9990'
SERVER3 = '52.14.27.81:9990'

LOG_ADDR = SERVER1 + "," + SERVER2 + "," + SERVER3
EXPT_RANGE = 10000
OP_COUNT = 1000

def main():
    do_scalability_experiment(True, 32)
    #do_multiappend_experiment(True, 32)

def do_scalability_experiment(async, window_size):
    results = test_single_append([1, 2, 4, 8, 16, 32], 100, async, window_size)
    write_output(results, 'single_append.txt')

def do_multiappend_experiment(async, window_size):
    results = test_multiappend([20], 1, async, window_size)
    #results = test_multiappend([0, 20, 40, 60, 80, 100], 100, async, window_size)
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
def test_single_append(num_clients, sample, async, window_size):
    summary = {}
    for num_client in num_clients:
        s = [] 
        for i in range(sample):
            results = measure_single_append(num_client, async, window_size)
            print 'Num client: {0}, Run {1}'.format(num_client, i)
            val = calculate_metric(results)
            logging('single_append', num_client, i, val)
            s.append(val)
        summary[num_client] = s
    return summary

# Run multiappend test (with all stat)
def test_multiappend(multiappend_percents, sample, async, window_size):
    summary = {}
    for multiappend_percent in multiappend_percents:
        s = [] 
        for i in range(sample):
            multi_op_count = int(OP_COUNT * multiappend_percent / 100.0) 
            single_op_count = OP_COUNT - multi_op_count 
            workload_client1 = "0={s},0:1={m}".format(s=single_op_count, m=multi_op_count)
            workload_client2 = "1={s},0:1={m}".format(s=single_op_count, m=multi_op_count)

            results = measure_multiappend(workload_client1, workload_client2, async, window_size)
            print 'Multiappend percent: {0}, Run {1}'.format(multiappend_percent, i)
            val = calculate_metric(results)
            logging('multi_append', multiappend_percent, i, val)
            s.append(val)
        summary[multiappend_percent] = s
    return summary


# Run single append test (by number of clients)
def measure_single_append(*args):
    preprocess()
    run_single_put_test(*args)
    results = gather_raw_data(tmp_result_dir)
    postprocess()
    return results

# Run multiappend test
def measure_multiappend(*args):
    preprocess()
    run_multi_put_test(*args)
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
def run_single_put_test(num_client, async, window_size):   
    clients = []
    for i in range(num_client):
        workload = "{color}={op_count}".format(color=i, op_count=OP_COUNT)
        clients.append((LOG_ADDR, EXPT_RANGE, i, workload, async, window_size))
    run_clients(clients)

# Run multiappend test
def run_multi_put_test(workload_client1, workload_client2, async, window_size):
    clients = []
    clients.append((LOG_ADDR, EXPT_RANGE, 0, workload_client1, async, window_size)) 
    clients.append((LOG_ADDR, EXPT_RANGE, 1, workload_client2, async, window_size))
    run_clients(clients)

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
class FuzzyMapClient(threading.Thread):
    def __init__(self, log_addr, expt_range, client_id, workload, async, window_size):
        threading.Thread.__init__(self)
        self._log_addr = log_addr
        self._expt_range = expt_range
        self._client_id = client_id
        self._workload = workload
        self._async = async
        self._window_size = window_size 
         
    def run(self):
        if self._async:
                cmd = async_test_cmd.format(self._log_addr, self._expt_range, self._client_id, self._workload, self._window_size)
        else:
                cmd = sync_test_cmd.format(self._log_addr, self._expt_range, self._client_id, self._workload)
        os.system(cmd)

# Run single append clients 
def run_clients(clients):
    threads = []
    for c in clients:
        thread = FuzzyMapClient(*c)
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
