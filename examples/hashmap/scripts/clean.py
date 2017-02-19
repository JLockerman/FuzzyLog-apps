import numpy as np
import math
import os

with open('latency.txt') as f:
        data = f.readlines()

data = map(lambda x: float(x) * 1000.0, data)
counts, bins = np.histogram(data, bins=20, range=(0, 1))

with open('latency.data', 'w') as f:
        for i, c in enumerate(counts):
                line = str("{0:.2f}".format(bins[i])) + " " + str(c)
                line += '\n'
                f.write(line)

os.remove('latency.txt')

# plot
os.system('./plot_script_latency.plt')

os.remove('latency.data')
