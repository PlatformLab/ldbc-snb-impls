#!/bin/bash
resultsDir=${1}
binSize=100

for i in $(seq 1 8);
do
    cat ${resultsDir}/LDBC-results_log.csv | grep "Update"$i | cut -d '|' -f4 > ${resultsDir}/LDBC-results_log.update$i.latency.csv;
    $(dirname $0)/cdf.py ${resultsDir}/LDBC-results_log.update$i.latency.csv
    $(dirname $0)/pdf.py ${binSize} ${resultsDir}/LDBC-results_log.update$i.latency.csv
done


