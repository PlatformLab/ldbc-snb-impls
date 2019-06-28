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
package net.ellitron.ldbcsnbimpls.interactive.torcdb2;

import net.ellitron.ldbcsnbimpls.interactive.torcdb2.TorcDb2.*;
import net.ellitron.ldbcsnbimpls.interactive.torcdb2.TorcDb2Client.*;
import net.ellitron.ldbcsnbimpls.interactive.torcdb2.LdbcSerializableQueriesAndResults.*;

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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;


/**
 * A multithreaded server that executes LDBC SNB Interactive Workload queries
 * against TorcDB2 on behalf of remote clients. 
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcDb2Server {

  private static final String doc =
      "TorcDb2Server: A multithreaded server that executes LDBC SNB\n"
      + "Interactive Workload queries against TorcDB2 on behalf of remote\n"
      + "clients.\n"
      + "\n"
      + "Usage:\n"
      + "  TorcDb2Server [options] COORDLOC GRAPHNAME\n"
      + "  TorcDb2Server (-h | --help)\n"
      + "  TorcDb2Server --version\n"
      + "\n"
      + "Arguments:\n"
      + "  COORDLOC   RAMCloud coordinator locator string.\n"
      + "  GRAPHNAME  Name of TorcDB2 graph to execute queries against.\n"
      + "\n"
      + "Options:\n"
      + "  --port=<n>        Port on which to listen for new connections.\n"
      + "                    [default: 5577].\n"
      + "  --dpdkPort=<n>    DPDK port to use for connecting to servers.\n"
      + "                    [default: -1].\n"
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
    private final TorcDb2ConnectionState connectionState;
    private final Map<Class<? extends Operation>, OperationHandler> 
        queryHandlerMap;
    private final ConcurrentErrorReporter concurrentErrorReporter;
    private int clientID = 1;
    private final BufferedWriter latencyFile;

    public ListenerThread(int port, TorcDb2ConnectionState connectionState,
        Map<Class<? extends Operation>, OperationHandler> queryHandlerMap,
        ConcurrentErrorReporter concurrentErrorReporter,
        BufferedWriter latencyFile) {
      this.port = port;
      this.connectionState = connectionState;
      this.queryHandlerMap = queryHandlerMap;
      this.concurrentErrorReporter = concurrentErrorReporter;
      this.latencyFile = latencyFile;
    }

    @Override
    public void run() {
      try {
        ServerSocket server = new ServerSocket(port);

        System.out.println("Listening on: " + server.toString());

        Lock lock = new ReentrantLock();

        AtomicInteger numClientThreads = new AtomicInteger(0);

        while (true) {
          Socket client = server.accept();

          System.out.println("Client connected: " + client.toString());

          Thread clientThread = new Thread(new ClientThread(client, 
               concurrentErrorReporter, connectionState, queryHandlerMap,
               clientID, lock, latencyFile, numClientThreads));

          clientThread.start();

          clientID++;
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
    private final TorcDb2ConnectionState connectionState;
    private final Map<Class<? extends Operation>, OperationHandler> 
        queryHandlerMap;
    private final int clientID;
    private final Lock lock;
    private final BufferedWriter latencyFile;
    private final AtomicInteger numClientThreads;

    public ClientThread(Socket client, 
        ConcurrentErrorReporter concurrentErrorReporter, 
        TorcDb2ConnectionState connectionState,
        Map<Class<? extends Operation>, OperationHandler> queryHandlerMap,
        int clientID, 
        Lock lock,
        BufferedWriter latencyFile,
        AtomicInteger numClientThreads) {
      this.client = client;
      this.concurrentErrorReporter = concurrentErrorReporter;
      this.resultReporter = 
          new ResultReporter.SimpleResultReporter(concurrentErrorReporter);
      this.connectionState = connectionState;
      this.queryHandlerMap = queryHandlerMap;
      this.clientID = clientID;
      this.lock = lock;
      this.latencyFile = latencyFile;
      this.numClientThreads = numClientThreads;
    }

    public void run() {
      try {
        numClientThreads.incrementAndGet();

        ObjectInputStream in = new ObjectInputStream(client.getInputStream());
        ObjectOutputStream out = 
            new ObjectOutputStream(client.getOutputStream());

        while (client.isConnected()) {
          Object query = in.readObject();

          if (query instanceof LdbcQuery1Serializable) {
            LdbcQuery1 op = ((LdbcQuery1Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op, 
                connectionState, resultReporter);

            List<LdbcQuery1Result> result = 
                (List<LdbcQuery1Result>) resultReporter.result();

            List<LdbcQuery1ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery1ResultSerializable(v));
            });
            
            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush();
          } else if (query instanceof LdbcQuery2Serializable) {
            LdbcQuery2 op = ((LdbcQuery2Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery2Result> result = 
                (List<LdbcQuery2Result>) resultReporter.result();

            List<LdbcQuery2ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery2ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery3Serializable) {
            LdbcQuery3 op = ((LdbcQuery3Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery3Result> result = 
                (List<LdbcQuery3Result>) resultReporter.result();

            List<LdbcQuery3ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery3ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery4Serializable) {
            LdbcQuery4 op = ((LdbcQuery4Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery4Result> result = 
                (List<LdbcQuery4Result>) resultReporter.result();

            List<LdbcQuery4ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery4ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery5Serializable) {
            LdbcQuery5 op = ((LdbcQuery5Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery5Result> result = 
                (List<LdbcQuery5Result>) resultReporter.result();

            List<LdbcQuery5ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery5ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery6Serializable) {
            LdbcQuery6 op = ((LdbcQuery6Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery6Result> result = 
                (List<LdbcQuery6Result>) resultReporter.result();

            List<LdbcQuery6ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery6ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery7Serializable) {
            LdbcQuery7 op = ((LdbcQuery7Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery7Result> result = 
                (List<LdbcQuery7Result>) resultReporter.result();

            List<LdbcQuery7ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery7ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery8Serializable) {
            LdbcQuery8 op = ((LdbcQuery8Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery8Result> result = 
                (List<LdbcQuery8Result>) resultReporter.result();

            List<LdbcQuery8ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery8ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery9Serializable) {
            LdbcQuery9 op = ((LdbcQuery9Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery9Result> result = 
                (List<LdbcQuery9Result>) resultReporter.result();

            List<LdbcQuery9ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery9ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery10Serializable) {
            LdbcQuery10 op = ((LdbcQuery10Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery10Result> result = 
                (List<LdbcQuery10Result>) resultReporter.result();

            List<LdbcQuery10ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery10ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery11Serializable) {
            LdbcQuery11 op = ((LdbcQuery11Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery11Result> result = 
                (List<LdbcQuery11Result>) resultReporter.result();

            List<LdbcQuery11ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery11ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery12Serializable) {
            LdbcQuery12 op = ((LdbcQuery12Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery12Result> result = 
                (List<LdbcQuery12Result>) resultReporter.result();

            List<LdbcQuery12ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery12ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery13Serializable) {
            LdbcQuery13 op = ((LdbcQuery13Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            LdbcQuery13Result result = 
                (LdbcQuery13Result) resultReporter.result();

            LdbcQuery13ResultSerializable resp = 
                new LdbcQuery13ResultSerializable(result);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcQuery14Serializable) {
            LdbcQuery14 op = ((LdbcQuery14Serializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcQuery14Result> result = 
                (List<LdbcQuery14Result>) resultReporter.result();

            List<LdbcQuery14ResultSerializable> resp = new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcQuery14ResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcShortQuery1PersonProfileSerializable) {
            LdbcShortQuery1PersonProfile op = 
                ((LdbcShortQuery1PersonProfileSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            LdbcShortQuery1PersonProfileResult result = 
                (LdbcShortQuery1PersonProfileResult) resultReporter.result();

            LdbcShortQuery1PersonProfileResultSerializable resp = 
                new LdbcShortQuery1PersonProfileResultSerializable(result);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcShortQuery2PersonPostsSerializable) {
            LdbcShortQuery2PersonPosts op = 
                ((LdbcShortQuery2PersonPostsSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcShortQuery2PersonPostsResult> result = 
                (List<LdbcShortQuery2PersonPostsResult>) resultReporter.result();

            List<LdbcShortQuery2PersonPostsResultSerializable> resp = 
                new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcShortQuery2PersonPostsResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcShortQuery3PersonFriendsSerializable) {
            LdbcShortQuery3PersonFriends op = 
                ((LdbcShortQuery3PersonFriendsSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcShortQuery3PersonFriendsResult> result = 
                (List<LdbcShortQuery3PersonFriendsResult>) resultReporter.result();

            List<LdbcShortQuery3PersonFriendsResultSerializable> resp = 
                new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcShortQuery3PersonFriendsResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcShortQuery4MessageContentSerializable) {
            LdbcShortQuery4MessageContent op = 
                ((LdbcShortQuery4MessageContentSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            LdbcShortQuery4MessageContentResult result = 
                (LdbcShortQuery4MessageContentResult) resultReporter.result();

            LdbcShortQuery4MessageContentResultSerializable resp = 
                new LdbcShortQuery4MessageContentResultSerializable(result);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcShortQuery5MessageCreatorSerializable) {
            LdbcShortQuery5MessageCreator op = 
                ((LdbcShortQuery5MessageCreatorSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            LdbcShortQuery5MessageCreatorResult result = 
                (LdbcShortQuery5MessageCreatorResult) resultReporter.result();

            LdbcShortQuery5MessageCreatorResultSerializable resp = 
                new LdbcShortQuery5MessageCreatorResultSerializable(result);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcShortQuery6MessageForumSerializable) {
            LdbcShortQuery6MessageForum op = 
                ((LdbcShortQuery6MessageForumSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            LdbcShortQuery6MessageForumResult result = 
                (LdbcShortQuery6MessageForumResult) resultReporter.result();

            LdbcShortQuery6MessageForumResultSerializable resp = 
                new LdbcShortQuery6MessageForumResultSerializable(result);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcShortQuery7MessageRepliesSerializable) {
            LdbcShortQuery7MessageReplies op = 
                ((LdbcShortQuery7MessageRepliesSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            List<LdbcShortQuery7MessageRepliesResult> result = 
                (List<LdbcShortQuery7MessageRepliesResult>) resultReporter.result();

            List<LdbcShortQuery7MessageRepliesResultSerializable> resp = 
                new ArrayList<>();
            result.forEach((v) -> {
              resp.add(new LdbcShortQuery7MessageRepliesResultSerializable(v));
            });

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(resp);
            out.flush(); 
          } else if (query instanceof LdbcUpdate1AddPersonSerializable) {
            LdbcUpdate1AddPerson op = 
                ((LdbcUpdate1AddPersonSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(LdbcNoResultSerializable.INSTANCE);
            out.flush(); 
          } else if (query instanceof LdbcUpdate2AddPostLikeSerializable) {
            LdbcUpdate2AddPostLike op = 
                ((LdbcUpdate2AddPostLikeSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(LdbcNoResultSerializable.INSTANCE);
            out.flush(); 
          } else if (query instanceof LdbcUpdate3AddCommentLikeSerializable) {
            LdbcUpdate3AddCommentLike op = 
                ((LdbcUpdate3AddCommentLikeSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(LdbcNoResultSerializable.INSTANCE);
            out.flush(); 
          } else if (query instanceof LdbcUpdate4AddForumSerializable) {
            LdbcUpdate4AddForum op = 
                ((LdbcUpdate4AddForumSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(LdbcNoResultSerializable.INSTANCE);
            out.flush(); 
          } else if (query instanceof LdbcUpdate5AddForumMembershipSerializable) {
            LdbcUpdate5AddForumMembership op = 
                ((LdbcUpdate5AddForumMembershipSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(LdbcNoResultSerializable.INSTANCE);
            out.flush(); 
          } else if (query instanceof LdbcUpdate6AddPostSerializable) {
            LdbcUpdate6AddPost op = 
                ((LdbcUpdate6AddPostSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(LdbcNoResultSerializable.INSTANCE);
            out.flush(); 
          } else if (query instanceof LdbcUpdate7AddCommentSerializable) {
            LdbcUpdate7AddComment op = 
                ((LdbcUpdate7AddCommentSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(LdbcNoResultSerializable.INSTANCE);
            out.flush(); 
          } else if (query instanceof LdbcUpdate8AddFriendshipSerializable) {
            LdbcUpdate8AddFriendship op = 
                ((LdbcUpdate8AddFriendshipSerializable) query).unpack();

            lock.lock();
            System.out.println(String.format("Client %d executing %s", clientID, query.toString()));
            long startTime = System.nanoTime();
            queryHandlerMap.get(op.getClass()).executeOperation(op,
                connectionState, resultReporter);

            System.out.println(String.format("Client %d executed query in %d us", clientID, (System.nanoTime() - startTime)/1000));

            latencyFile.append(String.format("%s,%d\n", query.toString(), (System.nanoTime() - startTime)/1000));
            latencyFile.flush();
            lock.unlock();

            out.writeObject(LdbcNoResultSerializable.INSTANCE);
            out.flush(); 
          } else {
            throw new RuntimeException("Unrecognized query type.");
          }
        }

        System.out.println("Client disconnected: " + client.toString());

        int clients = numClientThreads.decrementAndGet();

        if (clients == 0) {
          System.out.println("All clients have disconnected");
          latencyFile.close();
        }

      } catch (Exception e) {

      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 1)
     args = args[0].split("\\s+");

    Map<String, Object> opts =
        new Docopt(doc).withVersion("TorcDb2Server 1.0").parse(args);

    // Arguments.
    final String coordinatorLocator = (String) opts.get("COORDLOC");
    final String graphName = (String) opts.get("GRAPHNAME");
    final int port = Integer.decode((String) opts.get("--port"));
    final int dpdkPort = Integer.decode((String) opts.get("--dpdkPort"));

    System.out.println(String.format("TorcDb2Server: {coordinatorLocator: %s, "
        + "graphName: %s, port: %d, dpdkPort: %d}",
        coordinatorLocator,
        graphName,
        port,
        dpdkPort));
   
    // Connect to database. 
    Map<String, String> props = new HashMap<>();
    props.put("coordinatorLocator", coordinatorLocator);
    props.put("graphName", graphName);
    if (dpdkPort != -1)
      props.put("dpdkPort", (String) opts.get("--dpdkPort"));
    System.out.println("Connecting to TorcDB2...");
    TorcDb2ConnectionState connectionState = new TorcDb2ConnectionState(props);

    // Create mapping from op type to op handler for processing requests.
    Map<Class<? extends Operation>, OperationHandler> queryHandlerMap = 
        new HashMap<>();
    queryHandlerMap.put(LdbcQuery1.class, new TorcDb2.LdbcQuery1Handler());
    queryHandlerMap.put(LdbcQuery2.class, new TorcDb2.LdbcQuery2Handler());
    queryHandlerMap.put(LdbcQuery3.class, new TorcDb2.LdbcQuery3Handler());
    queryHandlerMap.put(LdbcQuery4.class, new TorcDb2.LdbcQuery4Handler());
    queryHandlerMap.put(LdbcQuery5.class, new TorcDb2.LdbcQuery5Handler());
    queryHandlerMap.put(LdbcQuery6.class, new TorcDb2.LdbcQuery6Handler());
    queryHandlerMap.put(LdbcQuery7.class, new TorcDb2.LdbcQuery7Handler());
    queryHandlerMap.put(LdbcQuery8.class, new TorcDb2.LdbcQuery8Handler());
    queryHandlerMap.put(LdbcQuery9.class, new TorcDb2.LdbcQuery9Handler());
    queryHandlerMap.put(LdbcQuery10.class, new TorcDb2.LdbcQuery10Handler());
    queryHandlerMap.put(LdbcQuery11.class, new TorcDb2.LdbcQuery11Handler());
    queryHandlerMap.put(LdbcQuery12.class, new TorcDb2.LdbcQuery12Handler());
    queryHandlerMap.put(LdbcQuery13.class, new TorcDb2.LdbcQuery13Handler());
    queryHandlerMap.put(LdbcQuery14.class, new TorcDb2.LdbcQuery14Handler());
    queryHandlerMap.put(LdbcShortQuery1PersonProfile.class, 
        new TorcDb2.LdbcShortQuery1PersonProfileHandler());
    queryHandlerMap.put(LdbcShortQuery2PersonPosts.class, 
        new TorcDb2.LdbcShortQuery2PersonPostsHandler());
    queryHandlerMap.put(LdbcShortQuery3PersonFriends.class, 
        new TorcDb2.LdbcShortQuery3PersonFriendsHandler());
    queryHandlerMap.put(LdbcShortQuery4MessageContent.class, 
        new TorcDb2.LdbcShortQuery4MessageContentHandler());
    queryHandlerMap.put(LdbcShortQuery5MessageCreator.class, 
        new TorcDb2.LdbcShortQuery5MessageCreatorHandler());
    queryHandlerMap.put(LdbcShortQuery6MessageForum.class, 
        new TorcDb2.LdbcShortQuery6MessageForumHandler());
    queryHandlerMap.put(LdbcShortQuery7MessageReplies.class, 
        new TorcDb2.LdbcShortQuery7MessageRepliesHandler());
    queryHandlerMap.put(LdbcUpdate1AddPerson.class, 
        new TorcDb2.LdbcUpdate1AddPersonHandler());
    queryHandlerMap.put(LdbcUpdate2AddPostLike.class, 
        new TorcDb2.LdbcUpdate2AddPostLikeHandler());
    queryHandlerMap.put(LdbcUpdate3AddCommentLike.class, 
        new TorcDb2.LdbcUpdate3AddCommentLikeHandler());
    queryHandlerMap.put(LdbcUpdate4AddForum.class, 
        new TorcDb2.LdbcUpdate4AddForumHandler());
    queryHandlerMap.put(LdbcUpdate5AddForumMembership.class, 
        new TorcDb2.LdbcUpdate5AddForumMembershipHandler());
    queryHandlerMap.put(LdbcUpdate6AddPost.class, 
        new TorcDb2.LdbcUpdate6AddPostHandler());
    queryHandlerMap.put(LdbcUpdate7AddComment.class, 
        new TorcDb2.LdbcUpdate7AddCommentHandler());
    queryHandlerMap.put(LdbcUpdate8AddFriendship.class, 
        new TorcDb2.LdbcUpdate8AddFriendshipHandler());
    
    // Presumably for reporting LDBC driver errors.
    ConcurrentErrorReporter concurrentErrorReporter = 
        new ConcurrentErrorReporter();

    BufferedWriter latencyFile = 
      Files.newBufferedWriter(Paths.get("latency.csv"), StandardCharsets.UTF_8, 
          StandardOpenOption.CREATE, StandardOpenOption.APPEND);

    // Listener thread accepts connections and spawns client threads.
    Thread listener = new Thread(new ListenerThread(port, connectionState,
          queryHandlerMap, concurrentErrorReporter, latencyFile));
    listener.start();
    listener.join();
  }
}
