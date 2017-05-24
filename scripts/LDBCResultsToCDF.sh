#!/bin/bash
# Given a directory containing the results output of the LDBC Social Network
# Benchmark, produces CDFs and PDFs of query latency.
#
# Notes:
#  - Currently only generates CDFs and PDFs for update queries
#
# e.g.
# ./LDBCResultsToCDF.py /path/to/ldbc_driver/results

resultsDir=${1}
binSize=100

for i in $(seq 1 8);
do
    cat ${resultsDir}/LDBC-results_log.csv | grep "Update"$i | cut -d '|' -f4 > ${resultsDir}/LDBC-results_log.update$i.latency.csv;
    $(dirname $0)/cdf.py ${resultsDir}/LDBC-results_log.update$i.latency.csv
    $(dirname $0)/pdf.py ${binSize} ${resultsDir}/LDBC-results_log.update$i.latency.csv
done


