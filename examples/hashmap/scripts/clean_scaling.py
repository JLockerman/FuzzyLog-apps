"""
This module includes useful functions for generating various graphs.
After running test script in `experiment` directory, run this file.
This will create a temporal data file from `data` directory, and 
plotting script in `gnuplot` directory will consume it to plot a relevant graph.

"""

import numpy as np
import math
import os

class Plotter(object):
        def validate(self):
                print "Not implemented"
        def polish(self):
                print "Not implemented"
        def plot(self):
                print "Not implemented"

class LatencyPlotter(Plotter): 
        def __init__(self, infile):
                Plotter.__init__(self)
                self._infile = infile
                self._outfile = self._infile + '.out'

        def validate(self):
                # TODO: validate file name/format
                pass 

        def polish(self):
                with open(self._infile) as f:
                        data = f.readlines()

                data = map(lambda x: float(x) * 1000.0, data)
                counts, bins = np.histogram(data, bins=20, range=(0, 1))

                with open(self._outfile, 'w') as f:
                        for i, c in enumerate(counts):
                                line = str("{0:.2f}".format(bins[i])) + " " + str(c)
                                line += '\n'
                                f.write(line)

        def plot(self):
                os.chdir('gnuplot')
                os.system('./plot_script_latency.plt')
                os.chdir('.')
                os.remove(self._outfile)

if __name__ == '__main__':
        plotter = LatencyPlotter('latency_async.txt') 
