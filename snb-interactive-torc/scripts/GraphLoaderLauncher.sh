#!/bin/bash
#
# Loading to RAMCloud instance:
# ./GraphLoaderLauncher.sh /path/to/ldbc/snb/social_network basic+udp:host=192.168.1.101,port=12246 ldbcsnbsfXXXX
#
# Writing directly to image files:
# ./GraphLoaderLauncher.sh /path/to/ldbc/snb/social_network /path/to/image/output/dir ldbcsnbsfXXXX
#
# Loads an LDBC SNB graph into TorcDB in parallel using multiple machines, each
# in a separate tmux window for progress monitoring. If a coordinator locator
# string is given as the second parameter, then the graph is uploaded into the
# RAMCloud instance given by that coordinator. If a path is given, GraphLoader
# is set to output image files into the given directory.
#
# Notes:
# - GraphLoaders instances are assigned whole files to load. Therefore to
# increase parallelism, it is a good idea to partition the files and use the
# - GraphLoaders generating images currently all use the same file names, so the
# directory to which they write image files should be machine local. 

# Argument parameters
dataDir=$1
destination=$2
graphName=$3

# Edit these parameters as necessary
masters=40
numLoaders=8
numThreads=1
txSize=128
#txRetries=10
#txBackoff=1000
reportInt=2
reportFmt="LFDT"

# Directory of graph loader repository.
pushd `dirname $0`/.. > /dev/null                                               
rootDir=`pwd`                                                                   
popd > /dev/null                                                                

# Get full path of data directory in case given directory is relative
pushd $dataDir > /dev/null
dataDir=`pwd`
popd > /dev/null

# Create an array of the client hostnames available for launching GraphLoader
# instances.
i=0
for j in {01..10}
do
  hosts[i]=rc$j
  (( i++ ))
done

# Create a new window with the appropriate number of panes.
tmux new-window -n GraphLoader
for (( i=0; i<$numLoaders-1; i++ ))
do
  tmux split-window -h
  tmux select-layout tiled > /dev/null
done

# Setup the panes for loading but stop before executing GraphLoader.
#for mode in nodes edges props
for mode in nodes
do
  rm -rf ./pids
  echo -n "Executing $mode phase... "
  for (( i=0; i<$numLoaders; i++ ))
  do
    tmux send-keys -t GraphLoader.$i "echo \"Loading $mode\"" C-m
    tmux send-keys -t GraphLoader.$i "ssh ${hosts[i]}" C-m
    if [[ $destination == *"host="* ]]
    then
      tmux send-keys -t GraphLoader.$i "cd $rootDir; mvn exec:java -Dexec.mainClass=\"net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader\" -Dexec.args=\"--coordLoc $destination --masters $masters --graphName $graphName --numLoaders $numLoaders --loaderIdx $i --numThreads $numThreads --txSize $txSize --reportInt $reportInt --reportFmt $reportFmt $mode $dataDir\"" C-m
    else
      tmux send-keys -t GraphLoader.$i "cd $rootDir; mvn exec:java -Dexec.mainClass=\"net.ellitron.ldbcsnbimpls.interactive.torc.util.GraphLoader\" -Dexec.args=\"--writeImages --outputDir $destination --graphName $graphName --numLoaders $numLoaders --loaderIdx $i --numThreads $numThreads --txSize $txSize --reportInt $reportInt --reportFmt $reportFmt $mode $dataDir\"" C-m
    fi
#    tmux send-keys -t GraphLoader.$i "echo \$! >> ./pids" C-m
  done

#  for pid in $(cat pids)
#  do
#    while [ -e /proc/$pid ]
#    do
#      sleep 1
#    done
#  done
#
#  echo "Done!"
done

rm -rf ./pids
