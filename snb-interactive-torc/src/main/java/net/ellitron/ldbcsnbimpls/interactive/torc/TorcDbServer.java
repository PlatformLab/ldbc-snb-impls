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
package net.ellitron.ldbcsnbimpls.interactive.torc;

import org.docopt.Docopt;

import java.util.Map;

/**
 * A multithreaded server that executes LDBC SNB Interactive Workload queries
 * against TorcDB on behalf of remote clients. 
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcDbServer {

  private static final String doc =
      "TorcDbServer: A multithreaded server that executes LDBC SNB\n"
      + "Interactive Workload queries against TorcDB on behalf of remote\n"
      + "clients.\n"
      + "\n"
      + "Usage:\n"
      + "  TorcDbServer [options] COORDLOC GRAPHNAME\n"
      + "  TorcDbServer (-h | --help)\n"
      + "  TorcDbServer --version\n"
      + "\n"
      + "Arguments:\n"
      + "  COORDLOC   RAMCloud coordinator locator string.\n"
      + "  GRAPHNAME  Name of TorcDB graph to execute queries against.\n"
      + "\n"
      + "Options:\n"
      + "  --numThreads=<n>  The number of worker threads for executing\n"
      + "                    query requests. [default: 1].\n"
      + "  --verbose         Print verbose output to stdout.\n"
      + "  -h --help         Show this screen.\n"
      + "  --version         Show version.\n"
      + "\n";
 
  private final String coordinatorLocator;
  private final String graphName;
  private final int numThreads;

  public TorcDbServer(String coordinatorLocator,
                      String graphName,
                      int numThreads) {
    this.coordinatorLocator = coordinatorLocator;
    this.graphName = graphName;
    this.numThreads = numThreads;
  }

  public static void main(String[] args) {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("TorcDbServer 1.0").parse(args);

    System.out.println(opts);
  }

}
