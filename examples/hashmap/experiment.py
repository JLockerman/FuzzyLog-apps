#!/usr/bin/python

import os
import sys
import os.path
import threading


fmt_cmd = "build/hashmap {0} {1} {2} >> {3}"
outfile = "gnuplot/sample.data"

OPERATION_COUNT = 10000
NUM_CLIENTS = 6

class MapClient(threading.Thread):
    def __init__(self, operation_count, color):
        threading.Thread.__init__(self)
        self.operation_count = operation_count
        self.color = color

    def run(self):
        run_client(self.operation_count, self.color)

def run_client(op_cnt, color):
    # single
    cmd = fmt_cmd.format(op_cnt, color, color, "tmp{0}.txt".format(color))
    os.system(cmd)

def experiment(num_clients):
    # execute
    threads = []
    for color in range(num_clients):
        thread = MapClient(OPERATION_COUNT, color)
        thread.start()
        threads.append(thread)

    for t in threads:
        t.join()

    # read output
    summary = []
    for color in range(num_clients):
        tmpfile = "tmp{0}.txt".format(color)
        with open(tmpfile) as f:
            content = f.readlines()
        content = [x.strip() for x in content]
        summary.append(content[0]) 

    # compute throughput
    throughput = sum([float(data.split()[1]) for data in summary])  

    # remove tmp files
    for color in range(num_clients):
        os.system("rm tmp{0}.txt".format(color)) 
    
    return throughput

def main():
    os.system("rm {0}".format(outfile))
    os.system("touch {0}".format(outfile))

    results = []
    for i in range(16):
        num_client = i + 1
        throughput = experiment(num_client)
        results.append((num_client, throughput))

    with open(outfile, "w") as f:
        for num_client, throughput in results:
            f.write("{0} {1}\n".format(num_client, throughput))
    os.chdir("gnuplot")
    os.system("./eps_gen.sh . .")


if __name__ == "__main__":
    main()
