/* 
 * Copyright (C) 2015-2018 Stanford University
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

import com.ldbc.driver.DbConnectionState;

import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

/**
 * Encapsulates the state of a connection to a set of TorcDb2Servers.
 *
 * Notes:
 * This object is shared among multiple threads in the LDBC SNB driver, however
 * each thread in this implementation gets its own set of open socket
 * connections to each of the TorcDb2Servers.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcDb2ClientConnectionState extends DbConnectionState {

  // IPs of the TorcDb2Servers
  private final String[] serverIPs;

  // TorcDb2Server port
  private final int port;

  // Each thread has its own private open socket connections to servers.
  // Would have used a ThreadLocal object here but it's not easy to iterate over
  // a ThreadLocal to clean up state, which we need to do when close() is called
  // by the driver at the end of the workload.
  private final ConcurrentHashMap<Thread, List<Socket>> 
      threadLocalServerConnList = new ConcurrentHashMap<>();

  // Along with each thread having its own socket, it also has its own input and
  // output stream for objects.
  private final ConcurrentHashMap<Thread, List<ObjectOutputStream>> 
      threadLocalOutputStreamList = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Thread, List<ObjectInputStream>> 
      threadLocalInputStreamList = new ConcurrentHashMap<>();

  public TorcDb2ClientConnectionState(Map<String, String> props) {
    if (props.containsKey("serverIPs")) {
      this.serverIPs = props.get("serverIPs").split(",");
    } else {
      this.serverIPs = new String[] {"127.0.0.1"};
    }

    if (props.containsKey("port")) {
      this.port = Integer.decode(props.get("port"));
    } else {
      this.port = 5577;
    }
  }

  @Override
  public void close() throws IOException {
    threadLocalServerConnList.forEach((thread, sktList) -> {
      for (Socket s : sktList) {
        try {
          s.close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });

    threadLocalServerConnList.clear();
  }

  public List<Socket> getConnections() throws IOException {
    Thread us = Thread.currentThread();
    
    if (threadLocalServerConnList.get(us) == null) {
      List<Socket> sList = new ArrayList<>(serverIPs.length);
      for (String ip : serverIPs) {
        sList.add(new Socket(ip, port));
      }
      threadLocalServerConnList.put(us, sList);
    } 

    return threadLocalServerConnList.get(us);
  }

  public List<ObjectOutputStream> getObjectOutputStreams() throws IOException {
    Thread us = Thread.currentThread();
    
    if (threadLocalOutputStreamList.get(us) == null) {
      List<Socket> servers = getConnections();
      List<ObjectOutputStream> osList = 
          new ArrayList<>(servers.size());
      for (Socket s : servers) {
        osList.add(new ObjectOutputStream(s.getOutputStream()));
      }
      threadLocalOutputStreamList.put(us, osList);
    } 

    return threadLocalOutputStreamList.get(us);
  }

  public List<ObjectInputStream> getObjectInputStreams() throws IOException {
    Thread us = Thread.currentThread();
    
    if (threadLocalInputStreamList.get(us) == null) {
      List<Socket> servers = getConnections();
      List<ObjectInputStream> isList = 
          new ArrayList<>(servers.size());
      for (Socket s : servers) {
        isList.add(new ObjectInputStream(s.getInputStream()));
      }
      threadLocalInputStreamList.put(us, isList);
    } 

    return threadLocalInputStreamList.get(us);
  }
}
