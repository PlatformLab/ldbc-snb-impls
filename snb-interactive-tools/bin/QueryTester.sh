#!/bin/bash
mvn exec:java -Dexec.mainClass="net.ellitron.ldbcsnbimpls.interactive.tools.QueryTester" -Dexec.args="$*"
