#!/usr/bin/python

import os
import sys
import os.path
import threading
import numpy


fmt_cmd = "build/hashmap {0} {1} {2} {3} >> {4}"
outfile_scalability = "gnuplot/scalability.data"
outfile_multi = "gnuplot/multi.data"

OPERATION_COUNT = 10000
NUM_CLIENTS = 8
REPEAT_COUNT = 5

class MapClient(threading.Thread):
    def __init__(self, operation_count, colors, percent_of_multi_operation):
        threading.Thread.__init__(self)
        self.operation_count = operation_count
        self.colors = colors 
        self.percent_of_multi_operation = percent_of_multi_operation

    def run(self):
        multi_operation_count = int(self.operation_count * self.percent_of_multi_operation)
        single_operation_count = self.operation_count - multi_operation_count 
        # run
        cmd = fmt_cmd.format(self.colors[0], self.colors[1], single_operation_count, multi_operation_count, "tmp{0}.txt".format(self.colors[0]))
        os.system(cmd)


def measure_throughput(clients):
    # execute
    threads = []
    for operation_cnt, colors, percent_of_multi_operation in clients:
        thread = MapClient(operation_cnt, colors, percent_of_multi_operation)  # only single color operation
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()

    # read output
    summary = []
    for o, c, p in clients:
        tmpfile = "tmp{0}.txt".format(c[0])
        with open(tmpfile) as f:
            content = f.readlines()
        content = [x.strip() for x in content]
        summary.append(content[0]) 

    # compute throughput
    throughput = sum([float(data.split()[1]) for data in summary])  

    # remove tmp files
    for o, c, p in clients:
        os.system("rm tmp{0}.txt".format(c[0])) 
    
    return throughput


def plot_scalability():
    os.system("rm {0}".format(outfile_scalability))
    os.system("touch {0}".format(outfile_scalability))

    results = []
    for num_client in range(NUM_CLIENTS):
        num_client += 1
        clients = [(OPERATION_COUNT, (i, i), 0.0) for i in range(num_client)]

        # Repeat N times to compute 
        throughputs = []
        for i in range(REPEAT_COUNT):
                throughput = measure_throughput(clients)
                throughputs.append(throughput)
        
        results.append((num_client, numpy.mean(throughputs), numpy.std(throughputs)))

    with open(outfile_scalability, "w") as f:
        for num_client, mean_throughput, std_throughput in results:
            f.write("{0} {1} {2}\n".format(num_client, mean_throughput, std_throughput))
    os.chdir("gnuplot")
    os.system("./eps_gen_scalability.sh")


def plot_multi_operation():
    os.system("rm {0}".format(outfile_multi))
    os.system("touch {0}".format(outfile_multi))

    NUM_CLIENTS_FOR_MULTI = 2

    results = []
    for percent in range(0, 110, 10):
        clients = []
        clients.append((OPERATION_COUNT, (0, 1), percent/100.0))
        clients.append((OPERATION_COUNT, (1, 0), percent/100.0))
        throughput = measure_throughput(clients)
        results.append((percent, throughput))

    with open(outfile_multi, "w") as f:
        for percent, throughput in results:
            f.write("{0} {1}\n".format(percent, throughput))
    os.chdir("gnuplot")
    os.system("./eps_gen_multi.sh")


def main():
    plot_scalability()
    #plot_multi_operation()


if __name__ == "__main__":
    main()
