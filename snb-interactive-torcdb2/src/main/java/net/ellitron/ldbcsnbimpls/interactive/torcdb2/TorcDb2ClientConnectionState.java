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

import com.ldbc.driver.Operation;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

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

  // This class keeps track of the load across TorcDb2Servers and helps us to 
  // spread the load across them.
  public class TorcDb2ServerLoadBalancer {

    // Lookup table of query type to esimated load.
    private final Map<Class<? extends Operation>, Long> queryLoadTable;

    // Tracks current server load.
    private final long[] serverEstimatedLoad;

    public TorcDb2ServerLoadBalancer(int numServers) {
      this.serverEstimatedLoad = new long[numServers];
      for (int i = 0; i < numServers; i++)
        this.serverEstimatedLoad[i] = 0L;

      this.queryLoadTable = new HashMap<>();
      this.queryLoadTable.put(LdbcQuery1.class, 32137L);
      this.queryLoadTable.put(LdbcQuery2.class, 121042L);
      this.queryLoadTable.put(LdbcQuery3.class, 38120317L);
      this.queryLoadTable.put(LdbcQuery4.class, 27856L);
      this.queryLoadTable.put(LdbcQuery5.class, 27723790L);
      this.queryLoadTable.put(LdbcQuery6.class, 3679835L);
      this.queryLoadTable.put(LdbcQuery7.class, 92L);
      this.queryLoadTable.put(LdbcQuery8.class, 208L);
      this.queryLoadTable.put(LdbcQuery9.class, 28264327L);
      this.queryLoadTable.put(LdbcQuery10.class, 294902L);
      this.queryLoadTable.put(LdbcQuery11.class, 27216L);
      this.queryLoadTable.put(LdbcQuery12.class, 138829L);
      this.queryLoadTable.put(LdbcQuery13.class, 3635L);
      this.queryLoadTable.put(LdbcQuery14.class, 78814L);
      this.queryLoadTable.put(LdbcShortQuery1PersonProfile.class, 35L);
      this.queryLoadTable.put(LdbcShortQuery2PersonPosts.class, 1441L);
      this.queryLoadTable.put(LdbcShortQuery3PersonFriends.class, 294L);
      this.queryLoadTable.put(LdbcShortQuery4MessageContent.class, 24L);
      this.queryLoadTable.put(LdbcShortQuery5MessageCreator.class, 35L);
      this.queryLoadTable.put(LdbcShortQuery6MessageForum.class, 99L);
      this.queryLoadTable.put(LdbcShortQuery7MessageReplies.class, 109L);
      this.queryLoadTable.put(LdbcUpdate1AddPerson.class, 1264L);
      this.queryLoadTable.put(LdbcUpdate2AddPostLike.class, 175L);
      this.queryLoadTable.put(LdbcUpdate3AddCommentLike.class, 181L);
      this.queryLoadTable.put(LdbcUpdate4AddForum.class, 206L);
      this.queryLoadTable.put(LdbcUpdate5AddForumMembership.class, 178L);
      this.queryLoadTable.put(LdbcUpdate6AddPost.class, 276L);
      this.queryLoadTable.put(LdbcUpdate7AddComment.class, 289L);
      this.queryLoadTable.put(LdbcUpdate8AddFriendship.class, 214L);
    }

    public synchronized int load(Operation op) {
      int minIndex = 0;
      for (int i = 0; i < serverEstimatedLoad.length; i++) {
        if (serverEstimatedLoad[i] < serverEstimatedLoad[minIndex])
          minIndex = i;
      }

      serverEstimatedLoad[minIndex] += queryLoadTable.get(op.getClass());

      return minIndex;
    }

    public synchronized void deload(Operation op, int n) {
      serverEstimatedLoad[n] -= queryLoadTable.get(op.getClass());
    }
  }

  // Tracks load across the TorcDb2Servers and helps us pick servers for queries.
  public final TorcDb2ServerLoadBalancer loadBalancer;

  public TorcDb2ClientConnectionState(Map<String, String> props) {
    if (props.containsKey("serverIPs")) {
      this.serverIPs = props.get("serverIPs").split(",");
    } else {
      this.serverIPs = new String[] {"127.0.0.1"};
    }

    this.loadBalancer = new TorcDb2ServerLoadBalancer(serverIPs.length);

    if (props.containsKey("port")) {
      this.port = Integer.decode(props.get("port"));
    } else {
      this.port = 5577;
    }
  }

  @Override
  public void close() throws IOException {
    System.out.println("Closing TorcDb2Server connections...");
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
