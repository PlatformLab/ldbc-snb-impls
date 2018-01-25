#!/bin/bash
#
# ./ImageFileDistributor.sh /path/to/image_files /remote/destination/dir

SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

# Argument parameters
sourceDir=$1
remoteDir=$2

# Change hosts as necessary. Files are evenly distributed across nodes by size.
i=0
for j in {01..80}
do
  hosts[i]=rc$j
  (( i++ ))
done

# Get full path of data directory in case given directory is relative
sourceDir="$( cd "$sourceDir" ; pwd -P )"

# Get information on the files.
fileCount=0
for file in $(ls $sourceDir)
do
  (( fileCount++ ))
done

(( quo = fileCount / ${#hosts[@]} ))
(( rem = fileCount % ${#hosts[@]} ))

hostIndex=0
if (( $hostIndex < $rem ))
then
  (( hostFileCount = quo + 1 ))
else
  (( hostFileCount = quo ))
fi

for file in $(ls $sourceDir)
do
  fileList=("${fileList[@]}" "$sourceDir/$file")

  if (( ${#fileList[@]} == $hostFileCount ))
  then
    echo "Copying $hostFileCount files to ${hosts[hostIndex]}..."
    time ssh ${hosts[hostIndex]} "cp ${fileList[@]} $remoteDir"
    fileList=()
    (( hostIndex++ ))
    if (( $hostIndex < $rem ))
    then
      (( hostFileCount = quo + 1 ))
    else
      (( hostFileCount = quo ))
    fi
  fi
done
