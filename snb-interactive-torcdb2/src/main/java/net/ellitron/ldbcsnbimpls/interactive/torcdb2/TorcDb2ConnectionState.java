/* 
 * Copyright (C) 2015-2019 Stanford University
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
package net.ellitron.ldbcsnbimpls.interactive.torcdb2;

import net.ellitron.torcdb2.*;

import com.ldbc.driver.DbConnectionState;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Encapsulates the state of a connection to TorcDB2. An instance of
 * this object is created on benchmark initialization, and then subsequently
 * passed to each query on execution.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcDb2ConnectionState extends DbConnectionState {

  private final Graph graph;
  private boolean fakeComplexReads;
  private boolean fakeUpdates;
  private List<Long> personIDFeed;
  private List<Long> messageIDFeed;

  public TorcDb2ConnectionState(Map<String, String> props) {
    this.graph = new Graph(props);

    if (props.containsKey("fakeComplexReads")) {
      if (!props.containsKey("personIDsFile") || !props.containsKey("messageIDsFile"))
        throw new RuntimeException("Error: Must specify BOTH personIDs and messageIDs file");

      this.fakeComplexReads = true;

      String personIDsFilename = props.get("personIDsFile");
      this.personIDFeed = new ArrayList<>();

      try (BufferedReader br = new BufferedReader(new FileReader(personIDsFilename))) {
        String line;
        while ((line = br.readLine()) != null)
          personIDFeed.add(Long.decode(line));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }

      String messageIDsFilename = props.get("messageIDsFile");
      this.messageIDFeed = new ArrayList<>();

      try (BufferedReader br = new BufferedReader(new FileReader(messageIDsFilename))) {
        String line;
        while ((line = br.readLine()) != null)
          messageIDFeed.add(Long.decode(line));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    } else {
      this.fakeComplexReads = false;
      this.personIDFeed = null;
      this.messageIDFeed = null;
    }

    if (props.containsKey("fakeUpdates"))
      fakeUpdates = true;
    else
      fakeUpdates = false;

    System.out.println("fakeComplexReads: " + fakeComplexReads);
    System.out.println("fakeUpdates: " + fakeUpdates);
  }

  public boolean fakeComplexReads() {
    return fakeComplexReads;
  }

  public boolean fakeUpdates() {
    return fakeUpdates;
  }

  public List<Long> personIDFeed() {
    return personIDFeed;
  }

  public List<Long> messageIDFeed() {
    return messageIDFeed;
  }

  @Override
  public void close() throws IOException {
    graph.close();
  }

  public Graph getGraph() {
    return graph;
  }
}
