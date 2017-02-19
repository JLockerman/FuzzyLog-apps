import numpy as np
import math
import os

def parse(infile, outfile):
        with open(infile) as f:
                data = f.readlines()

        data = map(lambda x: float(x) * 1000.0, data)
        counts, bins = np.histogram(data, bins=20, range=(0, 1))

        with open(outfile, 'w') as f:
                for i, c in enumerate(counts):
                        line = str("{0:.2f}".format(bins[i])) + " " + str(c)
                        line += '\n'
                        f.write(line)

parse('latency_async.txt', 'latency_async.data')
parse('latency_sync.txt', 'latency_sync.data')

# plot
os.system('./plot_script_latency.plt')

os.remove('latency_async.data')
os.remove('latency_sync.data')
