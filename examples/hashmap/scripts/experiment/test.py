import unittest
from nose_parameterized import parameterized
import ec2_helper as ec2
import settings
import os
import subprocess
import time
import random
from collections import defaultdict, Counter

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
                self.lock_server_region = settings.US_EAST_2
                self.lock_server = None
                self.lock_server_ip = None
                self.servers = {}
                self.server_ips = {}
                self.clients = {}
                self.client_ips = {}

        def setUp(self):
                lock_server, lock_server_ip = ec2.start_and_test_fuzzylog_lock_server(self.lock_server_region)
                self.assertTrue(len(lock_server) == 1, 'One lock server should exist')
                self.assertTrue(len(lock_server_ip) == 1, 'One lock server should exist')
                self.lock_server = lock_server 
                self.lock_server_ip = lock_server_ip 
 
                for region in self.regions:
                        servers, server_ips = ec2.start_and_test_fuzzylog_servers(region)
                        self.servers[region] = servers
                        self.server_ips[region] = server_ips
                        
                for region in self.regions:
                        clients, client_ips = ec2.start_and_test_fuzzylog_clients(region)
                        self.clients[region] = clients
                        self.client_ips[region] = client_ips

                # cleanup servers
                self.kill_fuzzylog(self.lock_server_ip[0]['public'], self.keyfile[self.lock_server_region])
                for region in self.server_ips.keys():
                        for s in self.server_ips[region]:
                                self.kill_fuzzylog(s['public'], self.keyfile[region])

                # cleanup clients
                for region in self.client_ips.keys():
                        for c in self.client_ips[region]:
                                self.clean_fuzzymap(c['public'], self.keyfile[region])

                # run funzzylog
                server_procs = []
                proc = self.launch_fuzzylog(self.lock_server_ip[0]['public'], self.keyfile[self.lock_server_region])
                server_procs.append(proc) 
                for region in self.server_ips.keys():
                        for s in self.server_ips[region]:
                                proc = self.launch_fuzzylog(s['public'], self.keyfile[region])
                                server_procs.append(proc) 
                time.sleep(5)

        def tearDown(self):
                ec2.stop_instances(self.lock_server_region, self.lock_server)
                for region in self.servers.keys():
                        ec2.stop_instances(region, self.servers[region])
                for region in self.clients.keys():
                        ec2.stop_instances(region, self.clients[region])

        
        # -----------------
        # Helper functions
        # -----------------
        def get_log_addr(self, log_server_ips):
                log_addr = ec2.addr_to_str(self.lock_server_ip + log_server_ips, self.fuzzylog_port)
                return log_addr

        # -----------------
        # Remote executions 
        # -----------------
        def kill_fuzzylog(self, fabhost, keyfile):
                os.system('fab -D -i ' + keyfile + ' -H ' + self.fabhost_prefix + fabhost + ' kill_fuzzylog')

        def clean_fuzzymap(self, fabhost, keyfile):
                os.system('fab -D -i ' + keyfile + ' -H ' + self.fabhost_prefix + fabhost + ' clean_fuzzymap')

        def launch_fuzzylog(self, fabhost, keyfile, nthreads=-1):
                launch_str = 'fuzzylog_proc:' + str(self.fuzzylog_port) + ',' + str(nthreads)
                proc = subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', self.fabhost_prefix + fabhost, launch_str])  
                return proc
			
        def launch_fuzzymap(self, fabhost, keyfile, logaddr, txn_version, client_id, workload, async, window_size):
                launch_str = 'fuzzymap_proc:' + logaddr
                launch_str += ',' + str(txn_version)
                launch_str += ',' + str(self.expt_range)
                launch_str += ',' + str(self.expt_duration)
                launch_str += ',' + str(client_id)
                launch_str += ',' + workload
                launch_str += ',' + str(async)
                launch_str += ',' + str(window_size) 
                return subprocess.Popen(['fab', '-D', '-i', keyfile, '-H', self.fabhost_prefix + fabhost, launch_str])
        
        # ---------------
        # Data management
        # ---------------

        # Called for every test case
        # gather_throughput: cleanup_dir ==> gather_raw_data ==> cleanup_dir ==> calculate_metric
        def gather_throughput(self, interested):
                outdir = 'tmp'
                self.cleanup_dir(outdir)
                os.system('mkdir ' + outdir)
                for region, i in interested:
                        src = self.client_ips[region][i]['public'] + ':~/fuzzylog/delos-apps/examples/hashmap/*.txt'
                        dest = outdir + '/.'
                        cmd = 'scp -i {key} -o StrictHostKeyChecking=no {src} {dest}'.format(key=self.keyfile[region], src=src, dest=dest)
                        os.system(cmd) 
                results, num_files = self.gather_raw_data(outdir)
                self.cleanup_dir(outdir)

                aggr_throughputs = self.calculate_metric(results)
                return aggr_throughputs
        
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

        # (async, txn_version, window_size, num_clients, multiput_percent, num_servers)
        @parameterized.expand([
                (True, 2, 80, 1, 0.0, 7),
                (True, 2, 80, 2, 0.0, 7),
                (True, 2, 80, 2, 0.1, 7),
                (True, 2, 80, 2, 1.0, 7),
                (True, 2, 80, 2, 10.0, 7),
                (True, 2, 80, 2, 100.0, 7),
                (True, 2, 80, 4, 0.0, 7),
                (True, 2, 80, 4, 0.1, 7),
                (True, 2, 80, 4, 1.0, 7),
                (True, 2, 80, 4, 10.0, 7),
                (True, 2, 80, 4, 100.0, 7),
                (True, 2, 80, 8, 0.0, 7),
                (True, 2, 80, 8, 0.1, 7),
                (True, 2, 80, 8, 1.0, 7),
                (True, 2, 80, 8, 10.0, 7),
                (True, 2, 80, 8, 100.0, 7),
       #        (True, 2, 80, 16, 0.0, 7),
       #        (True, 2, 80, 16, 0.1, 7),
       #        (True, 2, 80, 16, 1.0, 7),
       #        (True, 2, 80, 16, 10.0, 7),
       #        (True, 2, 80, 16, 100.0, 7),
       #        (True, 2, 80, 32, 0.0, 7),
       #        (True, 2, 80, 32, 0.1, 7),
       #        (True, 2, 80, 32, 1.0, 7),
       #        (True, 2, 80, 32, 10.0, 7),
       #        (True, 2, 80, 32, 10.0, 7),
        ])
        def test_scalability(self, async, txn_version, window_size, num_clients, multiput_percent, num_servers):
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
                        proc = self.launch_fuzzymap(client_ips[round_robin[i]]['public'], self.keyfile[test_region], log_addr, txn_version, i, workload, async, window_size)
                        time.sleep(0.1)
                        client_procs.append(proc) 

                for c in client_procs:
                        c.wait()

                # gather statistics
                interested = [(test_region, i) for i in set(round_robin)]
                thr = self.gather_throughput(interested)
                FuzzyMapTestCase.results.append((async, txn_version, window_size, num_clients, multiput_percent, num_servers, thr))

        @classmethod
        def tearDownClass(cls):
                FuzzyMapTestCase.write_output('scalability')


class CausalConsistencyTestCase(FuzzyMapTestCase):

        # (async, window_size, get_op_percent)
        @parameterized.expand([
                (True, 32, 0),
        ])
        def test_causal(self, async, window_size, get_op_percent):
                # a pair of client/server at site 1, another pair of client/server at site 2 
                self.assertTrue(len(settings.REGIONS) >= 2, 'At least 2 regions should be set')
                site_one = settings.REGIONS[0]
                site_two = settings.REGIONS[1]
                site_one_client_ips = self.client_ips[site_one]
                site_two_client_ips = self.client_ips[site_two]
                self.assertTrue(len(site_one_client_ips) >= 1, 'At least 1 client machines are required at %s' % site_one)
                self.assertTrue(len(site_two_client_ips) >= 1, 'At least 1 client machines are required at %s' % site_two)
                log_addr = self.get_log_addr([self.server_ips[site_one][0], self.server_ips[site_two][0]])

                client_procs = []
                # operation ratio
                get_op_count = int(self.op_count * get_op_percent / 100.0)
                put_op_count = int(self.op_count - get_op_count)

                # client at site 1
                workload = "put@100-200\={put_op_count}\,get@100:200\={get_op_count}".format(put_op_count=put_op_count, get_op_count=get_op_count)
                proc = self.launch_fuzzymap(site_one_client_ips[0]['public'], self.keyfile[site_one], log_addr, 1, 100, workload, async, window_size)
                client_procs.append(proc) 
                time.sleep(0.1)

                # client2
                workload = "put@200-100\={put_op_count}\,get@100:200\={get_op_count}".format(put_op_count=put_op_count, get_op_count=get_op_count)
                proc = self.launch_fuzzymap(site_two_client_ips[0]['public'], self.keyfile[site_two], log_addr, 1, 200, workload, async, window_size)
                client_procs.append(proc) 

                for c in client_procs:
                        c.wait()

                # gather statistics
                thr = self.gather_throughput(interested = [(site_one, 0), (site_two, 0)])
                FuzzyMapTestCase.results.append((async, window_size, get_op_percent, thr))

        @classmethod
        def tearDownClass(cls):
                FuzzyMapTestCase.write_output('causal')


if __name__ == '__main__':
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(ScalabilityTestCase)
        #suite = unittest.defaultTestLoader.loadTestsFromTestCase(CausalConsistencyTestCase)
        runner = unittest.TextTestRunner(verbosity=2)
        runner.run(suite)
