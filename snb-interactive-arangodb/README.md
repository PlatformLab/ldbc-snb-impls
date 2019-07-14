ArangoDB LDBC SNB Interactive Workload Implementation
=====================================================

== Implementation Status ==
* [ArangoDB](https://arangodb.com/)
  * Implementation Status:
    * Complex Read Queries: 0/14
    * Short Read Queries: 7/7
    * Update Queries: 8/8

== Instructions ==

LDBC SNB SF0001 dataset for ArangoDB is available for download
[here](https://www.dropbox.com/s/nplg3du0npzav7e/ldbc_snb_sf0001-arangodb-2019-07-13.tar.gz?dl=0).
Includes a data import script.

Can use the `QueryTester` tool in
[snb-interactive-tools](https://github.com/PlatformLab/ldbc-snb-impls/tree/master/snb-interactive-tools)
to run individual queries implemented here and see the results. Please modify the `pom.xml` file to
avoid other dependencies if you just want to use it for ArangoDB. It was designed to be able to run
queries against any database you configure in `config/querytester.properties`. If you use the
`--repeat` option, the query will run the number of times that you specify and report the latency
distribution:

```
$ mvn exec:exec -Dexec.qtargs="shortquery2 933 10 --repeat 10"
Query:
LdbcShortQuery2PersonPosts{personId=933, limit=10}
cmd=[shortquery2 933 10 --repeat 10]
Query Stats:
  Units:            MILLISECONDS
  Count:            10
  Min:              69
  Max:              131
  Mean:             77
  25th Percentile:  70
  50th Percentile:  72
  75th Percentile:  73
  90th Percentile:  131
  95th Percentile:  131
  99th Percentile:  131
```

