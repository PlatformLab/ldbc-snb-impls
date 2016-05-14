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
package net.ellitron.ldbcsnbimpls.interactive.neo4j;

import com.ldbc.driver.DbConnectionState;

import java.io.IOException;
import java.util.Map;

/**
 * Encapsulates the state of a connection to a Neo4j database. An instance of
 * this object is created on benchmark initialization, and then subsequently
 * passed to each query on execution. The encapsulated state is made
 * thread-local, so that even if this object is shared among threads, each
 * thread has its own connection state to the database.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jDbConnectionState extends DbConnectionState {

  private ThreadLocal<Neo4jTransactionDriver> driver;

  public Neo4jDbConnectionState(Map<String, String> props) {
    
    /*
     * Extract parameters from properties map.
     */
    String host;
    if (props.containsKey("host")) {
      host = props.get("host");
    } else {
      host = "127.0.0.1";
    }

    String port;
    if (props.containsKey("port")) {
      port = props.get("port");
    } else {
      port = "7474";
    }
    
    this.driver = ThreadLocal.withInitial(() -> {
      return new Neo4jTransactionDriver(host, port);
    });
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Returns the Neo4jTransactionDriver for the calling thread. Each thread
   * gets its own private instance of a Neo4jTransactionDriver.
   *
   * @return Thread-local Neo4jTransactionDriver.
   */
  public Neo4jTransactionDriver getTxDriver() {
    return driver.get();
  }
}
