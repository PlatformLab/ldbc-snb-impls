/* 
 * Copyright (C) 2015-2016 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.ellitron.ldbcsnbimpls.interactive.neo4j.util;

import com.ldbc.driver.DbException;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import java.util.Map;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDbConnectionState;
import org.docopt.Docopt;

/**
 * A utility for running individual queries for testing purposes.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class QueryTester {

  private static final String doc =
      "QueryTester: A utility for running individual queries for testing "
      + "purposes.\n"
      + "\n"
      + "Usage:\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery1 <personId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery2 <personId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery3 <personId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery4 <messageId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery5 <messageId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery6 <messageId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery7 <messageId>\n"
      + "  QueryTester (-h | --help)\n"
      + "  QueryTester --version\n"
      + "\n"
      + "Options:\n"
      + "  --host=<host> Host IP address of Neo4j webserver [default: 127.0.0.1].\n"
      + "  --port=<port> Port of Neo4j webserver [default: 7474].\n"
      + "  -h --help     Show this screen.\n"
      + "  --version     Show version.\n"
      + "\n";

  public static void main(String[] args) throws DbException {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("QueryTester 1.0").parse(args);

    String host;
    if (opts.containsKey("<host>")) {
      host = (String) opts.get("<host>");
    } else {
      host = "127.0.0.1";
    }

    String port;
    if (opts.containsKey("<port>")) {
      port = (String) opts.get("<port>");
    } else {
      port = "7474";
    }

    Neo4jDbConnectionState dbConnectionState =
        new Neo4jDbConnectionState(host, port);

    ResultReporter resultReporter =
        new ResultReporter.SimpleResultReporter(
            new ConcurrentErrorReporter());

    if ((Boolean) opts.get("shortquery1")) {
      Long id = Long.decode((String) opts.get("<personId>"));
      LdbcShortQuery1PersonProfile operation =
          new LdbcShortQuery1PersonProfile(id);

      new Neo4jDb.LdbcShortQuery1PersonProfileHandler()
          .executeOperation(operation, dbConnectionState, resultReporter);
      
      System.out.println(resultReporter.result().toString());
    }
  }
}
