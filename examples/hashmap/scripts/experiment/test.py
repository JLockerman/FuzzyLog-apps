import unittest
from nose_parameterized import parameterized
import ec2_helper as ec2
import settings
import os
import subprocess
import time
import random
from collections import defaultdict, Counter
import shutil
import pprint

class FuzzyMapTestCase(unittest.TestCase):
        results = []

        def __init__(self, *args, **kwargs):
                unittest.TestCase.__init__(self, *args, **kwargs)
                self.fabhost_prefix = 'ubuntu@' 
                self.keyfile = settings.KEYFILE
                self.fuzzylog_port = settings.FUZZYLOG_PORT
                self.expt_range = settings.EXPERIMENT_RANGE 
                self.expt_duration = settings.EXPERIMENT_DURATION 
                self.op_count = settings.OPERATION_PER_CLIENT
                self.regions = settings.REGIONS
                self.servers = {}
                self.server_ips = {}
                self.clients = {}
                self.client_ips = {}

        def up_and_running_instances(self):
                for region in self.regions:
                        servers, server_ips = ec2.start_and_test_fuzzylog_servers(region)
                        self.servers[region] = servers
                        self.server_ips[region] = server_ips
                        
                for region in self.regions:
                        clients, client_ips = ec2.start_and_test_fuzzylog_clients(region)
                        self.clients[region] = clients
                        self.client_ips[region] = client_ips

                # cleanup servers
                for region in self.server_ips.keys():
                        for s in self.server_ips[region]:
                                self.kill_fuzzylog(s['public'], self.keyfile[region])

                # cleanup clients
                for region in self.client_ips.keys():
                        for c in self.client_ips[region]:
                                self.clean_fuzzymap(c['public'], self.keyfile[region])


        def setUp(self):
                # run instances
                self.up_and_running_instances()

                # run funzzylog
                server_procs = []
                for region in self.server_ips.keys():
                        total_num_servers_in_group = len(self.server_ips[region])
                        for idx_in_group, s in enumerate(self.server_ips[region]):
                                proc = self.launch_fuzzylog(s['public'], self.keyfile[region], idx_in_group, total_num_servers_in_group)
                                server_procs.append(proc) 
                time.sleep(5)

        def tearDown(self):
                for region in self.servers.keys():
                        ec2.stop_instances(region, self.servers[region])
                for region in self.clients.keys():
                        ec2.stop_instances(region, self.clients[region])

        
        # -----------------
        # Helper functions
        # -----------------
        def get_log_addr(self, log_server_ips):
                log_addr = ec2.addr_to_str(log_server_ips, self.fuzzylog_port)
                return log_addr

        # -----------------
        # Remote executions 
        # -----------------
        def kill_fuzzylog(self, fabhost, keyfile):
                os.system('fab -D -i ' + keyfile + ' -H ' + self.fabhost_prefix + fabhost + ' kill_fuzzylog')

        def clean_fuzzymap(self, fabhost, keyfile):
                os.system('fab -D -i ' + keyfile + ' -H ' + self.fabhost_prefix + fabhost + ' clean_fuzzymap')

        def launch_fuzzylog(self, fabhost, keyfile, index_in_group, total_num_servers_in_group, nthreads=-1):
                launch_str = 'fuzzylog_proc:' + str(self.fuzzylog_port) + ',' + str(nthreads) + ',' + str(index_in_group) + ',' + str(total_num_servers_in_group)
                proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', self.fabhost_prefix + fabhost, launch_str])  
                return proc	

        def launch_atomicmap(self, fabhost, keyfile, logaddr, client_id, workload, async, window_size, replication=False):
                launch_str = 'atomicmap_proc:' + logaddr
                launch_str += ',' + str(self.expt_range)
                launch_str += ',' + str(self.expt_duration)
                launch_str += ',' + str(client_id)
                launch_str += ',' + workload
                launch_str += ',' + str(async)
                launch_str += ',' + str(window_size) 
                launch_str += ',' + str(replication) 
                return subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', self.fabhost_prefix + fabhost, launch_str])

        def launch_capmap(self, fabhost, keyfile, logaddr, expt_duration, num_clients, client_id, workload, txn_rate, async, window_size, protocol, role=None, replication=False):
                launch_str = 'capmap_proc:' + logaddr
                launch_str += ',' + str(self.expt_range)
                launch_str += ',' + str(expt_duration)
                launch_str += ',' + str(num_clients)
                launch_str += ',' + str(client_id)
                launch_str += ',' + workload
                launch_str += ',' + str(txn_rate)
                launch_str += ',' + str(async)
                launch_str += ',' + str(window_size)
                launch_str += ',' + str(protocol)
                launch_str += ',' + str(role)
                launch_str += ',' + str(replication) 
                return subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', self.fabhost_prefix + fabhost, launch_str])
 
        # ---------------
        # Data management
        # ---------------

        # Called for every test case
        # gather_throughput: cleanup_dir ==> gather_raw_data ==> cleanup_dir ==> calculate_metric
        def gather_throughput(self, interested):
                outdir = 'tmp'
                self.download_output_files(interested, outdir)
                results, num_files = self.gather_raw_data(outdir)
                self.cleanup_dir(outdir)

                aggr_throughputs = self.calculate_metric(results)
                return aggr_throughputs

        def download_output_files(self, interested, outdir):
                self.cleanup_dir(outdir)
                os.system('mkdir ' + outdir)
                for region, i in interested:
                        src = self.client_ips[region][i]['public'] + ':~/fuzzylog/delos-apps/examples/hashmap/*.txt'
                        dest = outdir + '/.'
                        cmd = 'scp -i {key} -o StrictHostKeyChecking=no {src} {dest}'.format(key=self.keyfile[region], src=src, dest=dest)
                        os.system(cmd) 
        
        def cleanup_dir(self, dirname):
                if os.path.isdir(dirname) and os.path.exists(dirname):
                        for f in os.listdir(dirname):
                                filename = os.path.join(dirname, f)
                                os.remove(filename)
                        os.rmdir(dirname)

        def gather_raw_data(self, dirname):
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
                return results[10:110], num_files       # XXX Assume that there are 120 data points

        def calculate_metric(self, results):
                return [sum(sample) for sample in results]

        # Called once after all test cases finished
        @staticmethod
        def write_output(filename):
                results = FuzzyMapTestCase.results
                with open('../data/%s.txt' % filename, 'w') as f:
                        for r in results:
                                samples = r[-1]
                                mean, low, high = FuzzyMapTestCase.postprocess_results(samples)
                                line = ' '.join(map(lambda x: str(x), r[:-1])) + ' ' + str(mean) + ' ' + str(low) + ' ' + str(high) + '\n'
                                f.write(line) 

        @staticmethod
        def postprocess_results(vals):
                # FIXME: this should not happen
                if (len(vals) == 0):
                    return [0.0, 0.0, 0.0]

                vals.sort()
                median_idx = len(vals)/2
                low_idx = int(len(vals)*0.05)
                high_idx = int(len(vals)*0.95)
                return [vals[median_idx]/float(1000), vals[low_idx]/float(1000), vals[high_idx]/float(1000)]

        def get_multiput_workload(self, exclude, num_clients, target):
                picked_counter = Counter()
                pool = range(1, num_clients+1)
                while True:
                        picked = random.choice(pool)
                        if exclude == picked:
                                continue
                        picked_counter[picked] += 1
                        if sum(picked_counter.values()) == target:
                                break

                workloads = []
                for c2, op_cnt in dict(picked_counter).iteritems():
                        w = "put@{c1}:{c2}\={op_cnt}".format(c1=exclude, c2=c2, op_cnt=op_cnt)
                        workloads.append(w)

                return "\,".join(workloads)

        # Logging (For debugging purpose, it's useful to keep intermediate data into log)
        def logging(self, *args):
                args = map(lambda x: str(x), args)
                line = " ".join(args)
                with open('test.log', 'a') as f:
                        f.write(line + "\n") 


class ScalabilityTestCase(FuzzyMapTestCase):

        # (async, window_size, num_clients, multiput_percent, num_servers)
        @parameterized.expand([
                (True, 80, 1, 0.0, 8),

                (True, 80, 2, 0.0, 8),
                (True, 80, 2, 0.1, 8),
                (True, 80, 2, 1.0, 8),
                (True, 80, 2, 10.0, 8),
                (True, 80, 2, 100.0, 8),

                (True, 80, 4, 0.0, 8),
                (True, 80, 4, 0.1, 8),
                (True, 80, 4, 1.0, 8),
                (True, 80, 4, 10.0, 8),
                (True, 80, 4, 100.0, 8),

                (True, 80, 8, 0.0, 8),
                (True, 80, 8, 0.1, 8),
                (True, 80, 8, 1.0, 8),
                (True, 80, 8, 10.0, 8),
                (True, 80, 8, 100.0, 8),

                (True, 80, 16, 0.0, 8),
                (True, 80, 16, 0.1, 8),
                (True, 80, 16, 1.0, 8),
                (True, 80, 16, 10.0, 8),
                (True, 80, 16, 100.0, 8),

                (True, 80, 32, 0.0, 8),
                (True, 80, 32, 0.1, 8),
                (True, 80, 32, 1.0, 8),
                (True, 80, 32, 10.0, 8),
                (True, 80, 32, 100.0, 8),
        ])
        def test_scalability(self, async, window_size, num_clients, multiput_percent, num_servers):
                # run clients
                test_region = settings.REGIONS[0]
                client_procs = []
                client_ips = self.client_ips[test_region]
                available_servers = self.server_ips[test_region]
                self.assertTrue(num_servers <= len(available_servers), 'Requested servers: %d, Available servers: %d' % (num_servers, len(available_servers)))

                log_addr = self.get_log_addr(self.server_ips[test_region][:num_servers])
                round_robin = [i % len(client_ips) for i in range(num_clients)]
                # operation ratio
                if num_clients > 1: 
                        multiput_op = int(self.op_count * multiput_percent / 100.0)
                        singleput_op = int(self.op_count - multiput_op)
                elif num_clients == 1:
                        multiput_op = 0
                        singleput_op = self.op_count

                for i in range(num_clients):
                        # single put workload
                        workload = "put@{color}\={op_count}".format(color=i+1, op_count=singleput_op)
                        # multiput workload
                        if multiput_op > 0:
                                multiput_workload = self.get_multiput_workload(i+1, num_clients, multiput_op)
                                workload = workload + "\," + multiput_workload
                        proc = self.launch_atomicmap(client_ips[round_robin[i]]['public'], self.keyfile[test_region], log_addr, i, workload, async, window_size)
                        time.sleep(0.1)
                        client_procs.append(proc) 

                for c in client_procs:
                        c.wait()

                # gather statistics
                interested = [(test_region, i) for i in set(round_robin)]
                thr = self.gather_throughput(interested)
                FuzzyMapTestCase.results.append((async, window_size, num_clients, multiput_percent, num_servers, thr))

        @classmethod
        def tearDownClass(cls):
                FuzzyMapTestCase.write_output('scalability')


class NetworkPartitionTestCase(FuzzyMapTestCase):

        def __init__(self, *args, **kwargs):
                FuzzyMapTestCase.__init__(self, *args, **kwargs)
                self.target_log_servers = []
                self.replication = False

        def setUp(self):
                # run instances
                self.up_and_running_instances()

                # run funzzylog
                server_procs = []
                log_server = self.server_ips[settings.US_EAST_2][0]
                proc = self.launch_fuzzylog(log_server['public'], self.keyfile[settings.US_EAST_2], 0, 2)
                self.target_log_servers.append(log_server)
                
                log_server = self.server_ips[settings.AP_NORTHEAST_1][0]
                proc = self.launch_fuzzylog(log_server['public'], self.keyfile[settings.AP_NORTHEAST_1], 1, 2)
                self.target_log_servers.append(log_server)

                server_procs.append(proc) 
                time.sleep(5)


        # (async, protocol, 
        #               (writer1_region, writer1_idx, writer1_window_size, writer1_txn_rate, writer1_duration),
        #               (writer2_region, writer2_idx, writer2_window_size, writer2_txn_rate, writer2_duration),
        #               (reader1_region, reader1_idx, reader1_duration),
        #               (reader2_region, reader2_idx, reader2_duration),
        # output_file_prefix)
        @parameterized.expand([
                (True, 2, 
                        (settings.REGIONS[0], 0, 1, 100, 15),           # writer1 in site1
                        (settings.REGIONS[1], 0, 1, 100, 15),           # writer2 in site2 
                        (settings.REGIONS[0], 1, 15),                   # reader1 in site1 
                        (settings.REGIONS[1], 1, 15),                   # reader2 in site2 
                'diff_site'),                                           # client1/server1 and client2/server2 are in different sites
        ])
        def test_network_partition(self, async, protocol, writer1, writer2, reader1, reader2, output_file_prefix):
                # log server
                log_addr = self.get_log_addr(self.target_log_servers)

                # writer
                writer1_region, writer1_idx, writer1_window_size, writer1_txn_rate, writer1_duration = writer1
                writer1_ip = self.client_ips[writer1_region][writer1_idx]

                writer2_region, writer2_idx, writer2_window_size, writer2_txn_rate, writer2_duration = writer2
                writer2_ip = self.client_ips[writer2_region][writer2_idx]

                client_procs = []
                # operation ratio
                put_op_count = self.op_count

                # writer1 at site1
                workload = "put@2-1\={put_op_count}".format(put_op_count=put_op_count)
                proc = self.launch_capmap(writer1_ip['public'], self.keyfile[writer1_region], log_addr, writer1_duration, 4, 1, workload, writer1_txn_rate, async, writer1_window_size, protocol, 'primary', self.replication)
                client_procs.append(proc) 
                time.sleep(0.1)

                # writer2 at site2 
                workload = "put@1-2\={put_op_count}".format(put_op_count=put_op_count)
                proc = self.launch_capmap(writer2_ip['public'], self.keyfile[writer2_region], log_addr, writer2_duration, 4, 2, workload, writer2_txn_rate, async, writer2_window_size, protocol, 'secondary', self.replication)
                client_procs.append(proc) 

                # reader
                if reader1:
                        reader1_region, reader1_idx, reader1_duration = reader1
                        reader1_ip = self.client_ips[reader1_region][reader1_idx]
                        workload = "get@2-1\=1"
                        proc = self.launch_capmap(reader1_ip['public'], self.keyfile[reader1_region], log_addr, reader1_duration, 4, 3, workload, 0, async, 1, protocol, 'primary', self.replication)
                        client_procs.append(proc) 
                        time.sleep(0.1)

                if reader2:
                        reader2_region, reader2_idx, reader2_duration = reader2
                        reader2_ip = self.client_ips[reader2_region][reader2_idx]
                        workload = "get@1-2\=1"
                        proc = self.launch_capmap(reader2_ip['public'], self.keyfile[reader2_region], log_addr, reader2_duration, 4, 4, workload, 0, async, 1, protocol, 'secondary', self.replication)
                        client_procs.append(proc) 

                for c in client_procs:
                        c.wait()

                # gather statistics
                outdir = 'tmp'
                self.download_output_files([
                                (writer1_region, writer1_idx), 
                                (writer2_region, writer2_idx),
                                (reader1_region, reader1_idx),
                                (reader2_region, reader2_idx),
                ], outdir)
                self.copy_files(outdir, '../data/', output_file_prefix)

        def copy_files(self, src_dir, dest_dir, prefix):
                for f in os.listdir(src_dir):
                        src = os.path.join(src_dir, f)
                        dest = os.path.join(dest_dir, prefix + '_' + f)
                        shutil.copyfile(src, dest)


class ReplicatedNetworkPartitionTestCase(NetworkPartitionTestCase):
	
        def __init__(self, *args, **kwargs):
                NetworkPartitionTestCase.__init__(self, *args, **kwargs)
                self.num_chain_servers = 2
                self.replication = True
                self.replication_length = 2

        def launch_fuzzylog_replication(self, fabhost, keyfile, index_in_group, total_num_servers_in_group, upstream_server, downstream_server, nthreads=-1):
                launch_str = 'fuzzylog_proc:' + str(self.fuzzylog_port) + ',' + str(nthreads) + ',' + str(index_in_group) + ',' + str(total_num_servers_in_group) + ',' + str(upstream_server) + ',' + str(downstream_server)
                proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', self.fabhost_prefix + fabhost, launch_str])  
                return proc
		
        def run_chain(self, region, chain_servers, idx_in_logical_partition):
                head = None
                tail = None
                for i, log_server in enumerate(chain_servers):
                        if i == 0:
                                # head: down
                                down = chain_servers[i+1]['private']
                                proc = self.launch_fuzzylog_replication(log_server['public'], self.keyfile[region], idx_in_logical_partition, self.num_chain_servers, upstream_server=None, downstream_server=down)
                                # add head to param
                                head = log_server

                        elif i == self.replication_length - 1:
                                # tail: up
                                up = chain_servers[i-1]['private'] + ':' + str(self.fuzzylog_port)
                                proc = self.launch_fuzzylog_replication(log_server['public'], self.keyfile[region], idx_in_logical_partition, self.num_chain_servers, upstream_server=up, downstream_server=None)
                                # add tail to param
                                tail = log_server

                        else:
                                # middle: up, down
                                down = chain_servers[i+1]['private']
                                up = chain_servers[i-1]['private'] + ':' + str(self.fuzzylog_port)
                                proc = self.launch_fuzzylog_replication(log_server['public'], self.keyfile[region], idx_in_logical_partition, self.num_chain_servers, upstream_server=up, downstream_server=down)
                return head, tail

        def setUp(self):
                # run instances
                self.up_and_running_instances()
                pprint.pprint(self.server_ips)

                self.assertTrue(len(self.server_ips[settings.US_EAST_2]) >= self.replication_length) 
                self.assertTrue(len(self.server_ips[settings.AP_NORTHEAST_1]) >= self.replication_length) 

                region1 = settings.US_EAST_2
                chain_servers = self.server_ips[region1][:self.replication_length]
                head1, tail1 = self.run_chain(region1, chain_servers, 0)

                region2 = settings.AP_NORTHEAST_1
                chain_servers = self.server_ips[region2][:self.replication_length]
                head2, tail2 = self.run_chain(region2, chain_servers, 1)
                
                self.target_log_servers.append(head1)
                self.target_log_servers.append(head2)
                self.target_log_servers.append(tail1)
                self.target_log_servers.append(tail2)

                time.sleep(5)


if __name__ == '__main__':
        #suite = unittest.defaultTestLoader.loadTestsFromTestCase(ScalabilityTestCase)
        #suite = unittest.defaultTestLoader.loadTestsFromTestCase(NetworkPartitionTestCase)
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(ReplicatedNetworkPartitionTestCase)
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
