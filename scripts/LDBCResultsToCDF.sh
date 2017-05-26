#!/bin/bash
# ./LDBCResultsToCDF.sh /path/to/ldbc_driver/results
#
# Given a directory containing the results output of the LDBC Social Network
# Benchmark, produces CDFs and PDFs of query latency.
#
# Notes:
#  - Currently only generates CDFs and PDFs for update queries

resultsDir=${1}
binSize=100

# Updates
for i in $(seq 1 8);
do
    cat ${resultsDir}/LDBC-results_log.csv | grep "LdbcUpdate"$i | cut -d '|' -f4 > ${resultsDir}/LDBC-results_log.update$i.latency.csv;
    if [ -s ${resultsDir}/LDBC-results_log.update$i.latency.csv ]
    then
      $(dirname $0)/cdf.py ${resultsDir}/LDBC-results_log.update$i.latency.csv
      $(dirname $0)/pdf.py ${binSize} ${resultsDir}/LDBC-results_log.update$i.latency.csv
    fi
done

# Short reads
for i in $(seq 1 7);
do
    cat ${resultsDir}/LDBC-results_log.csv | grep "LdbcShortQuery"$i | cut -d '|' -f4 > ${resultsDir}/LDBC-results_log.shortread$i.latency.csv;
    if [ -s ${resultsDir}/LDBC-results_log.shortread$i.latency.csv ]
    then
      $(dirname $0)/cdf.py ${resultsDir}/LDBC-results_log.shortread$i.latency.csv
      $(dirname $0)/pdf.py ${binSize} ${resultsDir}/LDBC-results_log.shortread$i.latency.csv
    fi
done

for i in $(seq 1 14);
do
    cat ${resultsDir}/LDBC-results_log.csv | grep "LdbcQuery"$i | cut -d '|' -f4 > ${resultsDir}/LDBC-results_log.longread$i.latency.csv;
    if [ -s ${resultsDir}/LDBC-results_log.longread$i.latency.csv ]
    then
      $(dirname $0)/cdf.py ${resultsDir}/LDBC-results_log.longread$i.latency.csv
      $(dirname $0)/pdf.py ${binSize} ${resultsDir}/LDBC-results_log.longread$i.latency.csv
    fi
done
