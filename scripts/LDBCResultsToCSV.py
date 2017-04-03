#!/usr/bin/python

import json
import sys

with open(sys.argv[1]) as data_file:    
    data = json.load(data_file)

print "Query, Count, Mean, Min, Max, 50th, 90th, 95th, 99th"
for query in data["all_metrics"]:
    stats = query["run_time"]
    print query["name"] + ",",
    print str(stats["count"]) + ",",
    print str(stats["mean"]) + ",",
    print str(stats["min"]) + ",",
    print str(stats["max"]) + ",",
    print str(stats["50th_percentile"]) + ",",
    print str(stats["90th_percentile"]) + ",",
    print str(stats["95th_percentile"]) + ",",
    print str(stats["99th_percentile"])
