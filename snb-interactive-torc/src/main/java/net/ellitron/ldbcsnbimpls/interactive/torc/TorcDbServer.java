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

import org.docopt.Docopt;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    public ListenerThread(int port) {
      this.port = port;
    }

    @Override
    public void run() {
      try {
        ServerSocket server = new ServerSocket(port);

        System.out.println("Listening on: " + server.toString());

        while (true) {
          Socket client = server.accept();

          System.out.println("Client connected: " + client.toString());

          Thread clientThread = new Thread(new ClientThread(client));

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

    public ClientThread(Socket client) {
      this.client = client;
    }

    public void run() {
      try {
        ObjectInputStream in = new ObjectInputStream(client.getInputStream());
        ObjectOutputStream out = 
            new ObjectOutputStream(client.getOutputStream());

        System.out.println("Client waiting for input");

        while (true) {
          String objectReceived = (String) in.readObject();

          System.out.println("Received: " + objectReceived);
        }
      } catch (Exception e) {

      }
    }
  }

  public static void main(String[] args) throws Exception {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("TorcDbServer 1.0").parse(args);

    final String coordinatorLocator = (String) opts.get("COORDLOC");
    final String graphName = (String) opts.get("GRAPHNAME");
    final int port = Integer.decode((String) opts.get("--port"));

    final Set clientConnections = new HashSet<>();

    System.out.println(String.format("TorcDbServer: {coordinatorLocator: %s, "
        + "graphName: %s, port: %d}",
        coordinatorLocator,
        graphName,
        port));

    Thread listener = new Thread(new ListenerThread(port));
    listener.start();
    listener.join();
  }
}
