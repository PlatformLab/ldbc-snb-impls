#!/bin/bash
# ./GraphLoader.sh /path/to/ldbc/snb/social_network basic+udp:host=192.168.1.101,port=12246 ldbcsnbsfXXXX
#
# Loads an LDBC SNB graph into TorcDB

# Argument parameters
dataDir=$1
coordLoc=$2
graphName=$3

# Edit these parameters as necessary
masters=4
threads=1
txSize=32

mvn exec:java -Dexec.mainClass="net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader" -Dexec.args="--coordLoc $coordLoc --masters $masters --graphName $graphName --numLoaders 1 --loaderIdx 0 --numThreads $threads --txSize $txSize --reportInt 2 --reportFmt OFDT nodes $dataDir";
mvn exec:java -Dexec.mainClass="net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader" -Dexec.args="--coordLoc $coordLoc --masters $masters --graphName $graphName --numLoaders 1 --loaderIdx 0 --numThreads $threads --txSize $txSize --reportInt 2 --reportFmt OFDT edges $dataDir";
mvn exec:java -Dexec.mainClass="net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader" -Dexec.args="--coordLoc $coordLoc --masters $masters --graphName $graphName --numLoaders 1 --loaderIdx 0 --numThreads $threads --txSize $txSize --reportInt 2 --reportFmt OFDT props $dataDir";
