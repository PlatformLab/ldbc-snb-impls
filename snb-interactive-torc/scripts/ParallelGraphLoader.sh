#!/bin/bash
#
# ./ParallelGraphLoader.sh /path/to/ldbc/snb/social_network basic+udp:host=192.168.1.101,port=12246 ldbcsnbsfXXXX
#
# Loads an LDBC SNB graph into TorcDB in parallel using multiple machines, each
# in a separate tmux window for progress monitoring.
#
# Notes:
# - GraphLoaders instances are assigned whole files to load. Therefore to
# increase parallelism, it is a good idea to partition the files and use the
# --splitSfx parameter to GraphLoader. For this use FilePartitioner.sh (see
# comments for usage details).

# Argument parameters
dataDir=$1
coordLoc=$2
graphName=$3

# Edit these parameters as necessary
masters=4
numLoaders=4
numThreads=4
txSize=32
#txRetries=10
#txBackoff=1000
#splitSfx=".part%04d"
reportInt=2
reportFmt="OFDT"

# Directory of graph loader repository.
pushd `dirname $0`/.. > /dev/null                                               
rootDir=`pwd`                                                                   
popd > /dev/null                                                                

# Create an array of the client hostnames
i=0
for j in {75..80}
do
  hosts[i]=rc$j
  (( i++ ))
done

# Create a new window with the appropriate number of panes.
tmux new-window -n GraphLoader
for (( i=0; i<$numLoaders-1; i++ ))
do
  tmux split-window -h
  tmux select-layout tiled
done

# Setup the panes for loading but stop before executing GraphLoader.
for mode in nodes edges props
do
  rm -rf ./pids
  for (( i=0; i<$numLoaders; i++ ))
  do
    tmux select-pane -t $i
    tmux send-keys "echo \"Loading $mode\"" C-m
    if [ -z ${splitSfx+x} ]
    then
      tmux send-keys "ssh ${hosts[i]} \"cd $rootDir; mvn exec:java -Dexec.mainClass=\\\"net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader\\\" -Dexec.args=\\\"--coordLoc $coordLoc --masters $masters --graphName $graphName --numLoaders $numLoaders --loaderIdx $i --numThreads $numThreads --txSize $txSize --reportInt $reportInt --reportFmt $reportFmt $mode $dataDir\\\"; exit\" &" C-m
    else
      tmux send-keys "ssh ${hosts[i]} \"cd $rootDir; mvn exec:java -Dexec.mainClass=\\\"net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader\\\" -Dexec.args=\\\"--coordLoc $coordLoc --masters $masters --graphName $graphName --numLoaders $numLoaders --loaderIdx $i --numThreads $numThreads --txSize $txSize --splitSfx \\\"$splitSfx\\\" --reportInt $reportInt --reportFmt $reportFmt $mode $dataDir\\\"; exit\" &" C-m
    fi
    tmux send-keys "echo \$! >> ./pids" C-m
  done

  for pid in $(cat pids)
  do
    while [ -e /proc/$pid ]
    do
      sleep 1
    done
  done

  echo "All Done!"
done
