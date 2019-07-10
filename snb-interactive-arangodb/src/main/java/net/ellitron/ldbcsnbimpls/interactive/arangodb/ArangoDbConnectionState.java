/* 
 * Copyright (C) 2019 Stanford University
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
package net.ellitron.ldbcsnbimpls.interactive.arangodb;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.LoadBalancingStrategy;

import com.ldbc.driver.DbConnectionState;

import java.io.IOException;
import java.util.Map;

/**
 * Encapsulates the state of a connection to an ArangoDB database. An instance of
 * this object is created on benchmark initialization, and then subsequently
 * passed to each query on execution. It is essentially a wrapper for the ArangoDB
 * client driver, which maintains all connection state to the ArangoDB cluster.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class ArangoDbConnectionState extends DbConnectionState {

  private ArangoDB driver;
  private ArangoDatabase db;

  public ArangoDbConnectionState(Map<String, String> props) {
    
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

    String graphName;
    if (props.containsKey("graph")) {
      graphName = props.get("graph");
    } else {
      graphName = "default";
    }

    this.driver = new ArangoDB.Builder()
      .host(host, Integer.decode(port))
      .maxConnections(8)
      .loadBalancingStrategy(LoadBalancingStrategy.ROUND_ROBIN)
      .acquireHostList(true)
      .build();

    this.db = driver.db(graphName);
  }

  /**
   * Closes the connection to the cluster.
   */
  @Override
  public void close() throws IOException {
    driver.shutdown();
  }

  /**
   * Returns the ArangoDB driver.
   */
  public ArangoDatabase getDatabase() {
    return db;
  }
}
