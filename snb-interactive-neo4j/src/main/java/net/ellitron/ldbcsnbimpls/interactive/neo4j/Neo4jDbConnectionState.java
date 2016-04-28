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
 * passed to each query on execution.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jDbConnectionState extends DbConnectionState {

  private Neo4jTransactionDriver driver;
  
  public Neo4jDbConnectionState(String host, String port) {
    this.driver = new Neo4jTransactionDriver(host, port);
  }

  @Override
  public void close() throws IOException {
    
  }
  
  public Neo4jTransactionDriver getTxDriver() {
    return driver;
  }
}
