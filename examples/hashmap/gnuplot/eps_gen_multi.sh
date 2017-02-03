#!/bin/bash

IN_FILE="multi.data"
OUT_FILE="multi.eps"
if [ -e ${IN_FILE} ];
then 
        gnuplot -e "file_in='${IN_FILE}'; file_out='${OUT_FILE}'; row_start=0; row_end=100;" synthetic_workload_multi.gnu
        echo "Input [" ${IN_FILE} "] Output [" ${OUT_FILE} "]"
else 
        echo "${IN_FILE} doesn't exist"
fi
