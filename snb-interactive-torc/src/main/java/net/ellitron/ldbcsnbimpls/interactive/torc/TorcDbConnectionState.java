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

import net.ellitron.torc.TorcGraph;

import com.ldbc.driver.DbConnectionState;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Encapsulates the state of a connection to a RAMCloud cluster. An instance of
 * this object is created on benchmark initialization, and then subsequently
 * passed to each query on execution.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcDbConnectionState extends DbConnectionState {

  private final Graph client;
  
  public TorcDbConnectionState(Map<String, String> props) {
    BaseConfiguration config = new BaseConfiguration();
    config.setDelimiterParsingDisabled(true);
    
    /*
     * Extract parameters from properties map.
     */
    String coordinatorLocator;
    if (props.containsKey("coordinatorLocator")) {
      coordinatorLocator = props.get("coordinatorLocator");
    } else {
      coordinatorLocator = "tcp:host=127.0.0.1,port=12246";
    }

    String graphName;
    if (props.containsKey("graphName")) {
      graphName = props.get("graphName");
    } else {
      graphName = "default";
    }

    if (props.containsKey("dpdkPort")) {
      int dpdkPort = Integer.decode(props.get("dpdkPort"));
      config.setProperty(
          TorcGraph.CONFIG_DPDK_PORT,
          dpdkPort);
    } 
    
    config.setProperty(
        TorcGraph.CONFIG_COORD_LOCATOR,
        coordinatorLocator);
    
    config.setProperty(
        TorcGraph.CONFIG_GRAPH_NAME,
        graphName);

    this.client = TorcGraph.open(config);
  }

  @Override
  public void close() throws IOException {
    try {
      client.close();
    } catch (Exception ex) {
      // TODO: Fix this.
      Logger.getLogger(
          TorcDbConnectionState.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public Graph getClient() {
    return client;
  }
}
