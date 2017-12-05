#!/bin/bash
#
# ./ImageMakerLauncher.sh /path/to/ldbc/snb/social_network graph_name output_dir [mode]
#
# Takes an LDBC SNB graph dataset as input and creates RAMCloud image files that
# represent what the graph would be in RAMCloud had it been uploaded via TorcDB.
# These image files can be uploaded by the RAMCloudUtils/TableImageUploader
# tool. Option "mode" can be set to "nodes" or "edges" to load only nodes or
# edges, respectively. Otherwise nodes and edges are both loaded.
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

# Argument parameters
dataDir=$1
graphName=$2
outputDir=$3
mode=${4:-"all"}

# Edit these parameters as necessary
numLoaders=16
numThreads=1
reportInt=2
reportFmt="LFDT"

# Get full path of data directory in case given directory is relative
dataDir="$( cd "$dataDir" ; pwd -P )"

# Create an array of the client hostnames available for launching ImageMaker
# instances.
i=0
for j in {01..16}
do
  hosts[i]=rc$j
  (( i++ ))
done

# Create a new window with the appropriate number of panes.
tmux new-window -n ImageMaker
for (( i=0; i<$numLoaders-1; i++ ))
do
  tmux split-window -h
  tmux select-layout tiled > /dev/null
done

# Setup the panes for loading but stop before executing ImageMaker.
#for mode in nodes edges props
for (( i=0; i<$numLoaders; i++ ))
do
  tmux send-keys -t ImageMaker.$i "echo \"Loading $mode\"" C-m
  tmux send-keys -t ImageMaker.$i "ssh ${hosts[i]}" C-m
  tmux send-keys -t ImageMaker.$i "cd ${SCRIPTPATH}/..; mvn exec:java -Dexec.mainClass=\"net.ellitron.ldbcsnbimpls.interactive.torc.util.ImageMaker\" -Dexec.args=\"--mode $mode --outputDir $outputDir --graphName $graphName --numLoaders $numLoaders --loaderIdx $i --numThreads $numThreads --reportInt $reportInt --reportFmt $reportFmt $dataDir\"; exit" C-m
done
