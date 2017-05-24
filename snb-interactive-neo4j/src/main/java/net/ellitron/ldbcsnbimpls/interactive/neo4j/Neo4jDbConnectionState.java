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

import static org.neo4j.driver.v1.Values.parameters;

import com.ldbc.driver.DbConnectionState;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;

import java.io.IOException;
import java.util.Map;

/**
 * Encapsulates the state of a connection to a Neo4j database. An instance of
 * this object is created on benchmark initialization, and then subsequently
 * passed to each query on execution. It is essentially a wrapper for the Neo4j
 * Bolt driver, which maintains all connection state to the Neo4j cluster.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jDbConnectionState extends DbConnectionState {

  private Driver driver;

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

    String protocol;
    if (props.containsKey("protocol")) {
      protocol = props.get("protocol");
    } else {
      protocol = "bolt";
    }

    /*
     * Configure driver to NOT use encryption (for lowest possible latency),
     * and no authentication.
     */
    this.driver = GraphDatabase.driver(protocol + "://" + host, 
        Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE)
        .toConfig());
  }

  /**
   * Closes the connection to the cluster.
   */
  @Override
  public void close() throws IOException {
    driver.close();
  }

  /**
   * Returns the encapsulated Neo4j Bolt driver.
   *
   * @return Neo4j Bolt driver.
   */
  public Driver getDriver() {
    return driver;
  }
}
