#!/bin/bash

IN_DIR="."
OUT_DIR="."

if [ -d ${1} ] && [[ ${1} ]] && [ -d ${2} ] && [[ ${2} ]];
then
	IN_DIR=${1}
	OUT_DIR=${2}
else
	echo "Usage: $0 <input dir> <output dir>"
	echo " - Reads all *.data files in input dir. "\
	     " - Generates eps files in output dir."
	exit
fi 

echo "IN_DIR = ${IN_DIR}, OUT_DIR = ${OUT_DIR}"

for IN_FILE in ${IN_DIR}/*.data
do
	if [ -e ${IN_FILE} ];
	then 
		OUT_FILE=${IN_FILE#${IN_DIR}/}
		OUT_FILE=${OUT_DIR}/${OUT_FILE%.data}.eps
		gnuplot -e "file_in='${IN_FILE}'; file_out='${OUT_FILE}'; row_start=0; row_end=16;" synthetic_workload_throughput.gnu
		echo "Input [" ${IN_FILE} "] Output [" ${OUT_FILE} "]"
	else 
		echo "${IN_FILE} doesn't exist"
	fi
done

