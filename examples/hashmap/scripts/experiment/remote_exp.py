#!/usr/bin/python

import boto
import boto.ec2
from boto.ec2 import EC2Connection
from boto.vpc import VPCConnection

import os
import sys
import subprocess
import time

import settings
from ec2_helper import *

			
def launch_fuzzymap(fabhost, keyfile, logaddr, exp_range, exp_duration, client_id, workload, async, window_size):
        launch_str = 'fuzzymap_proc:' + logaddr
        launch_str += ',' + str(exp_range)
        launch_str += ',' + str(exp_duration)
        launch_str += ',' + str(client_id)
        launch_str += ',' + workload
        launch_str += ',' + str(async)
        launch_str += ',' + str(window_size) 
        return subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', fabhost, launch_str])

# Launch fuzzy log. 
def launch_fuzzylog(fabhost, keyfile, port, nthreads=-1):
	launch_str = 'fuzzylog_proc:' + str(port) + ',' + str(nthreads)
	proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', fabhost, launch_str])  
	return proc

def get_fuzzylog_servers(region):
        server_type = "fuzzylog server"
        filters = { "tag:Type": server_type }
        return get_filtered_servers(region, filters)

def get_fuzzylog_lockserver(region):
        server_type = "fuzzylog lockserver"
        filters = { "tag:Type": server_type }
        return get_filtered_servers(region, filters)

def get_fuzzylog_clients(region):
        server_type = "fuzzylog client"
        filters = { "tag:Type": server_type }
        return get_filtered_servers(region, filters)

def start_and_test_fuzzylog_servers(region):
        instance_ids = get_fuzzylog_lockserver(region) + get_fuzzylog_servers(region)
        instances = start_instances(region, instance_ids)
        # test servers
        instance_ips = to_ip(instances)
        test_instances(instance_ips, settings.KEYFILE[region])
        return instances

def start_and_test_fuzzylog_clients(region):
        instance_ids = get_fuzzylog_clients(region)
        instances = start_instances(region, instance_ids)
        # test servers
        instance_ips = to_ip(instances)
        test_instances(instance_ips, settings.KEYFILE[region])
        return instances

def fuzzymap_single_put_test(server_ips, client_ips, num_clients, window_size):
	fabhost_prefix = 'ubuntu@' 
        keyfile = settings.KEYFILE[0]   # deprecated
        fuzzylog_port = settings.FUZZYLOG_PORT
        
        # cleanup servers and clients 
        for s in server_ips:
                os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + s['public'] + ' kill_fuzzylog')

        for c in client_ips:
                os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + c['public'] + ' clean_fuzzymap')

        # run funzzylog
        server_procs = []
        for s in server_ips:
	        proc = launch_fuzzylog(fabhost_prefix + s['public'], keyfile, fuzzylog_port) 
                server_procs.append(proc) 
        time.sleep(5)

        # clients parameters
        logaddr = addr_to_str(server_ips, fuzzylog_port)
        expt_range = settings.EXPERIMENT_RANGE 
        expt_duration = settings.EXPERIMENT_DURATION 
        op_count = settings.OPERATION_PER_CLIENT

        # run clients
        client_procs = []
        round_robin = [i % len(client_ips) for i in range(num_clients)]
        async = True
        for i in range(num_clients):
                workload = "put@{color}\={op_count}".format(color=i+1, op_count=op_count)
                proc = launch_fuzzymap(fabhost_prefix + client_ips[round_robin[i]]['public'], keyfile, logaddr, expt_range, expt_duration, i, workload, async, window_size)
                time.sleep(0.1)
                client_procs.append(proc) 

        for c in client_procs:
                c.wait()

        # finish clients

        # gather statistics
        outdir = 'tmp'
        cleanup_dir(outdir)
        os.system('mkdir ' + outdir)
        
        for i in set(round_robin):
                src = client_ips[i]['public'] + ':~/fuzzylog/delos-apps/examples/hashmap/*.txt'
                dest = outdir + '/.'
                cmd = 'scp -i {key} -o StrictHostKeyChecking=no {src} {dest}'.format(key=keyfile, src=src, dest=dest)
                os.system(cmd) 
        results, num_files = gather_raw_data(outdir)
        cleanup_dir(outdir)

        aggr_throughputs = calculate_metric(results)
        # For debugging
        for thr in aggr_throughputs:
                logging('scalability_w%d' % window_size, num_clients, thr)

        return aggr_throughputs

def fuzzymap_multiput_test(server_ips, client_ips, percent, window_size):
        assert len(client_ips) >= 2, "At lease 2 instances are required for this test."

	fabhost_prefix = 'ubuntu@' 
        keyfile = settings.KEYFILE[0]           # deprecated
        fuzzylog_port = settings.FUZZYLOG_PORT
        
        # cleanup servers and clients 
        for s in server_ips:
                os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + s['public'] + ' kill_fuzzylog')

        for c in client_ips:
                os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + c['public'] + ' clean_fuzzymap')

        # run funzzylog
        server_procs = []
        for s in server_ips:
	        proc = launch_fuzzylog(fabhost_prefix + s['public'], keyfile, fuzzylog_port) 
                server_procs.append(proc) 
        time.sleep(5)

        # clients parameters
        logaddr = addr_to_str(server_ips, fuzzylog_port)
        expt_range = settings.EXPERIMENT_RANGE 
        expt_duration = settings.EXPERIMENT_DURATION 
        op_count = settings.OPERATION_PER_CLIENT

        # run clients
        client_procs = []
        async = True

        # operation ratio
        multiput_op = int(op_count * percent / 100.0)
        singleput_op = int(op_count - multiput_op)

        # client1
        workload = "put@100\={single}\,put@100:200\={multi}".format(single=singleput_op, multi=multiput_op)
        proc = launch_fuzzymap(fabhost_prefix + client_ips[0]['public'], keyfile, logaddr, expt_range, expt_duration, 100, workload, async, window_size)
        client_procs.append(proc) 
        time.sleep(0.1)

        # client2
        workload = "put@200\={single}\,put@100:200\={multi}".format(single=singleput_op, multi=multiput_op)
        proc = launch_fuzzymap(fabhost_prefix + client_ips[1]['public'], keyfile, logaddr, expt_range, expt_duration, 200, workload, async, window_size)
        client_procs.append(proc) 

        for c in client_procs:
                c.wait()

        # finish clients

        # gather statistics
        outdir = 'tmp'
        cleanup_dir(outdir)
        os.system('mkdir ' + outdir)
        
        for i in [0,1]:
                src = client_ips[i]['public'] + ':~/fuzzylog/delos-apps/examples/hashmap/*.txt'
                dest = outdir + '/.'
                cmd = 'scp -i {key} -o StrictHostKeyChecking=no {src} {dest}'.format(key=keyfile, src=src, dest=dest)
                os.system(cmd) 
        results, num_files = gather_raw_data(outdir)
        cleanup_dir(outdir)

        aggr_throughputs = calculate_metric(results)
        # For debugging
        for thr in aggr_throughputs:
                logging('multiput_w%d' % window_size, percent, thr)

        return aggr_throughputs


# Gather all data from a given directory
def gather_raw_data(dirname):
        results = []
        num_files = 0
        for f in os.listdir(dirname):
                filename = os.path.join(dirname, f)
                print filename
                with open(filename) as fd:
                        contents = fd.readlines()
                contents = map(lambda x: float(x), contents)
                results.append(contents)
                num_files += 1
        results = zip(*results)
        return results[10:110], num_files

def cleanup_dir(dirname):
        if os.path.isdir(dirname) and os.path.exists(dirname):
                for f in os.listdir(dirname):
                        filename = os.path.join(dirname, f)
                        os.remove(filename)
                os.rmdir(dirname)

def calculate_metric(results):
        return [sum(sample) for sample in results]

# For long running experiment, it's useful to keep intermediate data into log
def logging(*args):
        args = map(lambda x: str(x), args)
        line = " ".join(args)
        with open('remote_exp.log', 'a') as f:
                f.write(line + "\n") 

def write_output(results, filename):
    with open('../data/' + filename, 'w') as f:
        keys = results.keys()
        keys.sort()
        for k in keys:
            samples = results[k]
            mean, low, high = postprocess_results(samples)
            line = str(k) + ' ' + str(mean) + ' ' + str(low) + ' ' + str(high) + '\n'
            f.write(line) 

def postprocess_results(vals):
    # FIXME: this should not happen
    if (len(vals) == 0):
        return [0.0, 0.0, 0.0]

    vals.sort()
    median_idx = len(vals)/2
    low_idx = int(len(vals)*0.05)
    high_idx = int(len(vals)*0.95)
    return [vals[median_idx]/float(1000), vals[low_idx]/float(1000), vals[high_idx]/float(1000)]

def test_scalability(region, clients, window_size):
        summary = {}
        for c in clients: 
                # start servers
                servers = start_and_test_fuzzylog_servers(region)
                clients = start_and_test_fuzzylog_clients(region)

                # TODO: do experiment 
                server_ips = to_ip(servers)
                client_ips = to_ip(clients)
                samples = fuzzymap_single_put_test(server_ips, client_ips, c, window_size)

                # stop servers
                stop_instances(region, servers)
                stop_instances(region, clients)
                summary[c] = samples
        write_output(summary, 'scale_w{0}.txt'.format(window_size))

def test_multiput_txn(region, multiput_percents, window_size):
        summary = {}
        for p in multiput_percents: 
                # start servers
                servers = start_and_test_fuzzylog_servers(region)
                clients = start_and_test_fuzzylog_clients(region)

                # TODO: do experiment 
                server_ips = to_ip(servers)
                client_ips = to_ip(clients)
                samples = fuzzymap_multiput_test(server_ips, client_ips, p, window_size)

                # stop servers
                stop_instances(region, servers)
                stop_instances(region, clients)
                summary[p] = samples
        write_output(summary, 'multiput_w{0}.txt'.format(window_size))


def update_fuzzymap_binary(region):
	fabhost_prefix = 'ubuntu@' 
        keyfile = settings.KEYFILE[region]
        
        # update clients
        clients = start_and_test_fuzzylog_clients(region)
        client_ips = to_ip(clients)
        for c in client_ips:
                os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + c['public'] + ' update_fuzzymap_binary')
        stop_instances(region, clients)

def update_fuzzylog_binary(region, commit=None):
	fabhost_prefix = 'ubuntu@' 
        keyfile = settings.KEYFILE[region]

        # update servers + clients
        servers = start_and_test_fuzzylog_servers(region) + start_and_test_fuzzylog_clients(region)
        server_ips = to_ip(servers)
        cmd = 'update_fuzzylog_binary'
        if commit:
                cmd += ":" + commit
        for s in server_ips:
                os.system('fab -D -i ' + keyfile + ' -H ' + fabhost_prefix + s['public'] + ' ' + cmd)
        stop_instances(region, servers)

def main():
        #test_scalability(clients=[1], window_size=32)
        #test_multiput_txn(multiput_percents=[0,20,40,60,80,100], window_size=32)
        update_fuzzylog_binary(settings.US_EAST_2)
        #update_fuzzymap_binary(settings.US_EAST_2)
        pass

if __name__ == "__main__":
        main()
