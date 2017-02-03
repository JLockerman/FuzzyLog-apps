#!/bin/bash

IN_FILE="scalability.data"
OUT_FILE="scalability.eps"
if [ -e ${IN_FILE} ];
then 
        gnuplot -e "file_in='${IN_FILE}'; file_out='${OUT_FILE}'; row_start=0; row_end=16;" synthetic_workload_scalability.gnu
        echo "Input [" ${IN_FILE} "] Output [" ${OUT_FILE} "]"
else 
        echo "${IN_FILE} doesn't exist"
fi
