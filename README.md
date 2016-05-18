LDBC SNB Workload Implementations
=================================

This repository contains implementations of the [Linked Data Benchmark
Council](http://www.ldbcouncil.org/)'s [Social Network
Benchmark](http://www.ldbcouncil.org/benchmarks/snb) for various target
databases. It also includes utilities for loading SNB datasets into the target
database.

## Current Workload Implementations ##
### Interactive ###
* [Torc](https://github.com/ellitron/torc) 
  * Implementation Status:
    * Complex Read Queries: 1/14 (passing validation)
    * Short Read Queries: 7/7 (passing validation)
    * Update Queries: 8/8 (passing validation)
* [TitanDB](https://github.com/thinkaurelius/titan)
  * Implementation Status:
    * Complex Read Queries: 1/14
    * Short Read Queries: 7/7
    * Update Queries: 8/8
* [Neo4j](https://github.com/ellitron/ldbc-snb-impls/tree/master/snb-interactive-neo4j)
  * Implementation Status: Complete
    * Complex Read Queries: 14/14 (passing validation)
    * Short Read Queries: 7/7 (passing validation)
    * Update Queries: 8/8 (passing validation)
