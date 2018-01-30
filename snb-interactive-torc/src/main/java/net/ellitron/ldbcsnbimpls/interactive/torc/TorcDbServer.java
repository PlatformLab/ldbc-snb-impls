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

import net.ellitron.ldbcsnbimpls.interactive.torc.TorcDb.*;
import net.ellitron.ldbcsnbimpls.interactive.torc.TorcDbClient.*;
import net.ellitron.ldbcsnbimpls.interactive.torc.LdbcQueryResultsSerializable.*;
import net.ellitron.ldbcsnbimpls.interactive.torc.LdbcQueriesSerializable.*;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import org.docopt.Docopt;

import java.io.*;
import java.net.*;
import java.util.*;

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
      + "  --port=<n>        Port on which to listen for new connections.\n"
      + "                    [default: 5577].\n"
      + "  --verbose         Print verbose output to stdout.\n"
      + "  -h --help         Show this screen.\n"
      + "  --version         Show version.\n"
      + "\n";

  /**
   * Thread that listens for connections and spins off new threads to serve
   * client connections.
   */
  private static class ListenerThread implements Runnable {

    // Port on which we listen for incoming connections.
    private final int port;

    // Passed off to each client thread for executing queries.
    private final TorcDbConnectionState connectionState;
    private final Map<Class<? extends Operation>, OperationHandler> 
        queryHandlerMap;
    private final ConcurrentErrorReporter concurrentErrorReporter;

    public ListenerThread(int port, TorcDbConnectionState connectionState,
        Map<Class<? extends Operation>, OperationHandler> queryHandlerMap,
        ConcurrentErrorReporter concurrentErrorReporter) {
      this.port = port;
      this.connectionState = connectionState;
      this.queryHandlerMap = queryHandlerMap;
      this.concurrentErrorReporter = concurrentErrorReporter;
    }

    @Override
    public void run() {
      try {
        ServerSocket server = new ServerSocket(port);

        System.out.println("Listening on: " + server.toString());

        while (true) {
          Socket client = server.accept();

          System.out.println("Client connected: " + client.toString());

          Thread clientThread = new Thread(new ClientThread(client, 
               concurrentErrorReporter, connectionState, queryHandlerMap));

          clientThread.start();
        }

//        server.close();
      } catch (Exception e) {

      }
    }
  }

  /**
   * Thread that receives requests from clients, executes them, and returns a
   * response. Handles requests for the lifetime of the connection to the
   * client.
   */
  private static class ClientThread implements Runnable {

    private final Socket client;
    private final ConcurrentErrorReporter concurrentErrorReporter;
    private final ResultReporter resultReporter;
    private final TorcDbConnectionState connectionState;
    private final Map<Class<? extends Operation>, OperationHandler> 
        queryHandlerMap;

    public ClientThread(Socket client, 
        ConcurrentErrorReporter concurrentErrorReporter, 
        TorcDbConnectionState connectionState,
        Map<Class<? extends Operation>, OperationHandler> queryHandlerMap) {
      this.client = client;
      this.concurrentErrorReporter = concurrentErrorReporter;
      this.resultReporter = 
          new ResultReporter.SimpleResultReporter(concurrentErrorReporter);
      this.connectionState = connectionState;
      this.queryHandlerMap = queryHandlerMap;
    }

    public void run() {
      try {
        ObjectInputStream in = new ObjectInputStream(client.getInputStream());
        ObjectOutputStream out = 
            new ObjectOutputStream(client.getOutputStream());

        while (true) {
          Object query = in.readObject();

          if (query instanceof LdbcQuery1Serializable) {
            LdbcQuery1 op = ((LdbcQuery1Serializable) query).getQuery();

            queryHandlerMap.get(op.getClass()).executeOperation(op, 
                connectionState, resultReporter);
            List<LdbcQuery1Result> result = 
                (List<LdbcQuery1Result>) resultReporter.result();

            List<LdbcQuery1ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery1ResultSerializable(v));
            });

            out.writeObject(resp);
            out.flush();
          } else if (query instanceof LdbcQuery2Serializable) {
            LdbcQuery2 op = ((LdbcQuery2Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery3Serializable) {
            LdbcQuery3 op = ((LdbcQuery3Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery4Serializable) {
            LdbcQuery4 op = ((LdbcQuery4Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery5Serializable) {
            LdbcQuery5 op = ((LdbcQuery5Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery6Serializable) {
            LdbcQuery6 op = ((LdbcQuery6Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery7Serializable) {
            LdbcQuery7 op = ((LdbcQuery7Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery8Serializable) {
            LdbcQuery8 op = ((LdbcQuery8Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery9Serializable) {
            LdbcQuery9 op = ((LdbcQuery9Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery10Serializable) {
            LdbcQuery10 op = ((LdbcQuery10Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery11Serializable) {
            LdbcQuery11 op = ((LdbcQuery11Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery12Serializable) {
            LdbcQuery12 op = ((LdbcQuery12Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery13Serializable) {
            LdbcQuery13 op = ((LdbcQuery13Serializable) query).getQuery();

          } else if (query instanceof LdbcQuery14Serializable) {
            LdbcQuery14 op = ((LdbcQuery14Serializable) query).getQuery();

          } else {
            throw new RuntimeException("Unrecognized query type.");
          }
        }
      } catch (Exception e) {

      }
    }
  }

  public static void main(String[] args) throws Exception {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("TorcDbServer 1.0").parse(args);

    // Arguments.
    final String coordinatorLocator = (String) opts.get("COORDLOC");
    final String graphName = (String) opts.get("GRAPHNAME");
    final int port = Integer.decode((String) opts.get("--port"));

    System.out.println(String.format("TorcDbServer: {coordinatorLocator: %s, "
        + "graphName: %s, port: %d}",
        coordinatorLocator,
        graphName,
        port));
   
    // Connect to database. 
    Map<String, String> props = new HashMap<>();
    props.put("coordinatorLocator", coordinatorLocator);
    props.put("graphName", graphName);
    System.out.println("Connecting to TorcDB...");
    TorcDbConnectionState connectionState = new TorcDbConnectionState(props);

    // Create mapping from op type to op handler for processing requests.
    Map<Class<? extends Operation>, OperationHandler> queryHandlerMap = 
        new HashMap<>();
    queryHandlerMap.put(LdbcQuery1.class, new TorcDb.LdbcQuery1Handler());

    // Presumably for reporting LDBC driver errors.
    ConcurrentErrorReporter concurrentErrorReporter = 
        new ConcurrentErrorReporter();

    // Listener thread accepts connections and spawns client threads.
    Thread listener = new Thread(new ListenerThread(port, connectionState,
          queryHandlerMap, concurrentErrorReporter));
    listener.start();
    listener.join();
  }
}
