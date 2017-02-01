#!/usr/bin/python

import os
import sys
import os.path


fmt_cmd = "build/hashmap {0} {1} {2} {3} >> {4}"
outfile = "gnuplot/sample.data"

RECORD_COUNT = 1000000
OPERATION_COUNT = 10000
NUM_WORKERS = 16

def main():
    os.system("rm {0}".format(outfile))
    os.system("touch {0}".format(outfile))
    for num_colors in range(16):
        test_by_color(RECORD_COUNT, OPERATION_COUNT, NUM_WORKERS, num_colors+1)
    os.chdir("gnuplot")
    os.system("./eps_gen.sh . .")

def test_by_color(record_cnt, op_cnt, num_workers, num_colors):
    cmd = fmt_cmd.format(record_cnt, op_cnt, num_workers, num_colors, outfile)
    os.system(cmd)

if __name__ == "__main__":
    main()
