#!/bin/bash
# A convenience script for splitting LDBC SNB dataset files for parallel
# loading by the GraphLoader. This script will preserve the first line (header)
# of the original file in each of the split parts so that they can be processed
# independently.
#
# Example Usage:
# cd /path/to/social_network/
# for file in $(ls); do /path/to/splitter.sh $file 1000000; done
# 

file=$1
lines=$2
export file
split_filter () { { head -n 1 $file; cat; } > "$FILE"; }
export -f split_filter
tail -n +2 $file | split --lines=$lines --filter=split_filter -d --suffix-length=4 - ${file}.part
