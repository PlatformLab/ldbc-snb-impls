/* 
 * Copyright (C) 2016 Stanford University
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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

import java.util.ArrayList;
import java.util.List;
import java.io.StringReader;
import java.net.ConnectException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for driving the execution of transactions against a remote Neo4j
 * server over the Neo4j REST API. An application uses the driver by queuing a
 * sequence of statements in the driver, then calling methods to execute those
 * statements in different ways.
 * <p>
 * In addition to supporting typical interactive style transactions where the
 * application executes statements and views results in a loop before
 * ultimately deciding to commit or rollback, the driver also supports "blind"
 * transactions where the application composes one or more statements and both
 * executes and commits in one request to the server. In certain simple
 * situations (for instance, adding a node to the graph) this can save a round
 * trip to the database and save latency.
 * <p>
 * Special Notes:<br>
 * <ul>
 * <li>This class is not thread-safe. Such thread-safety would have no benefit
 * since Neo4j (v2.3.3) does not support concurrent requests accessing the same
 * transaction.</li>
 * <li>The driver does not currently support authentication with the server.
 * Authentication must be turned off (see Neo4j documentation).</li>
 * <li>Open transactions on the Neo4j server are automatically rolled back
 * after a timeout period of inactivity. Be sure to adjust your timeout
 * configuration value to meet your application needs.</li>
 * </ul>
 * <p>
 * TODO:<br>
 * <ul>
 * <li>Add functionality for asynchronous execution of queries (maybe something
 * like an execAsync and waitAsync method pair).</li>
 * <li>Add way for client to authenticate itself with the server over HTTP</li>
 * <li>Add way for client to authenticate itself with the server over
 * HTTPS</li>
 * <li>Add logging</li>
 * <li>Add exceptions</li>
 * <li>Remove duplicated code in exec, execAndCommit, and commit</li>
 * </ul>
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jTransactionDriver {

  private static final Logger logger =
      LoggerFactory.getLogger(Neo4jTransactionDriver.class);

  // Jersey client instance. Used to create WebResource objects, which are in
  // turn used to send requests and return responses to/from the server. This 
  // object is expensive to create, so we reuse a single instance for all 
  // operations.
  private final Client client;

  // The root URI of all other URIs used to drive transactions on the server.
  private final String serverRootURI;

  // URI of the current open transaction on the server.
  // null means no transaction is currently open.
  private String currentTxLocation = null;

  // Statements are staged here, in enqueue order, before execution.
  private List<Neo4jCypherStatement> statementQueue = new ArrayList<>();

  /**
   * Construct a Neo4jTransactionDriver.
   *
   * @param host IP address of the Neo4j HTTP web server.
   * @param port Port of the Neo4j HTTP web server.
   */
  public Neo4jTransactionDriver(String host, String port) {
    this.client = Client.create();
    this.serverRootURI = "http://" + host + ":" + port + "/db/data";
  }

  /**
   * Add one or more statements to the driver statement queue for future
   * execution. When executed, statements are always executed in FIFO order. A
   * call to exec, execAndCommit, or rollback will generally have the effect of
   * clearing the statement queue unless otherwise noted.
   *
   * @param stmts Neo4jCypherStatement(s) to enqueue for later execution.
   */
  public void enqueue(Neo4jCypherStatement... stmts) {
    for (int i = 0; i < stmts.length; i++) {
      statementQueue.add(stmts[i]);
    }
  }

  /**
   * Execute enqueued statements in the current transaction context, clear the
   * statement queue, and return the results. If there is no currently open
   * transaction context then a new one is opened on the server automatically.
   * Subsequent calls will then execute newly enqueued statements in the same
   * transaction until a call to either commit or rollback is made. If called
   * with an empty statement queue, this method will return immediately with an
   * empty results list.
   * <p>
   * If an error is encountered and an exception is thrown, the driver leaves
   * itself in a "reset" state (cleared statement queue, cleared transaction
   * state). Future operations will execute in a new transaction.
   * <p>
   * Usage Notes:<br>
   * This method is used to execute statements in an interactive fashion where
   * the results can be viewed before executing more statements and eventually
   * deciding whether to commit the transaction or to roll it back. Such use
   * looks like the following:<br>
   * <code>
   * //... do work<br>
   * driver.enqueue(s1)<br>
   * //... do work<br>
   * driver.enqueue(s2, s3)<br>
   * results = driver.exec()<br>
   * // analyze results {s1: results[0], s2: results[1], s3: results[2]}<br>
   * // generate new statements<br>
   * driver.enqueue(s4, s5, s6)<br>
   * results = driver.exec()<br>
   * // analyze results<br>
   * // decide to commit (or rollback)<br>
   * driver.commit(); // or driver.rollback()<br>
   * </code> <br>
   * When interactive use is not needed, {@link #execAndCommit()} can be used
   * to execute statements and commit the transaction in one method call.
   *
   * @return List of results, where the ordering matches the statement enqueue
   * ordering in the driver (that is, the nth result is for the nth enqueued
   * statement). Each result is a map from column titles to columns.
   *
   * @throws Neo4jCommunicationException If there is a communication problem
   * with the server.
   * @throws Neo4jTransactionException If there was a problem with executing
   * one or more statements on the server.
   */
  public List<Map<String, String[]>> exec() {
    /*
     * If the user called exec without anything in the statement queue then
     * just return with an empty results list.
     */
    if (statementQueue.isEmpty()) {
      return Collections.emptyList();
    }

    // Figure out what the URI should be based on whether or not we have a 
    // currently open transaction context.
    String uri;
    if (currentTxLocation == null) {
      // Open a new transaction.
      uri = serverRootURI + "/transaction";
    } else {
      // Use the current transaction.
      uri = currentTxLocation;
    }

    // Construct the payload body from statements in the queue.
    String requestPayload = makeTxReqPayload();

    /*
     * We don't actually need the statements in the queue beyond this point. If
     * the execution fails for any reason, the specified behavior of the driver
     * is to be in a clean "reset" state.
     */
    statementQueue.clear();

    /*
     * Build and send HTTP request. In the event of any error, reset the driver
     * and throw a Neo4jCommunicationException.
     */
    ClientResponse response;
    try {
      response = client.resource(uri)
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .entity(requestPayload)
          .post(ClientResponse.class);
    } catch (Exception e) { // Pokemon Exception Handling
        /*
       * If sending/recieving the HTTP request/reponse encountered a failure,
       * although the transaction may be left open on the server, the driver's
       * specified behavior is to be left in a clean "reset" state, and so we
       * forget about the transaction in this case. The Neo4j server itself
       * will eventually timeout and rollback any accidentally left open
       * transactions.
       */
      currentTxLocation = null;

      throw new Neo4jCommunicationException(e);
    }

    // If this request opened a new transaction, capture the transaction URI
    // from the response. 
    if (currentTxLocation == null) {
      currentTxLocation = response.getLocation().toString();
    }

    // Parse the JSON-formatted response.
    String responsePayload = response.getEntity(String.class);
    JsonReader reader = Json.createReader(new StringReader(responsePayload));
    JsonObject responseJsonObj = reader.readObject();
    reader.close();

    // Finished with the response, can close it now.
    response.close();

    // Grab the array of results and array of errors.
    JsonArray resultsArray = responseJsonObj.getJsonArray("results");
    JsonArray errorsArray = responseJsonObj.getJsonArray("errors");

    if (!errorsArray.isEmpty()) {
      /*
       * Log all errors that are of the "ClientNotification" classification.
       * These are informational warnings and typically relate to performance,
       * but do not indicate an actual error. These are also unlikely to be
       * things a client application can be reasonably expected to respond to.
       * If there are errors of any other classification, these are interpreted
       * to be actual errors and are collected together and put into the
       * message of a Neo4jTransactionException. These errors do indicate a
       * failure of some kind and an inability to proceed normally without the
       * client application taking action (if it can, although for many such
       * errors the client application may not have the ability to respond).
       * See Neo4j documentation on "status codes" for more information.
       */

      JsonArrayBuilder actualErrorsBldr = Json.createArrayBuilder();
      boolean errorsDetected = false;
      for (int i = 0; i < errorsArray.size(); i++) {
        /*
         * Get the status code and parse its format. Status codes have the
         * following format: Neo.[Classification].[Category].[Title]
         */
        JsonObject errorObject = errorsArray.getJsonObject(i);
        String statusCode = errorObject.getString("code");
        String[] statusCodeComp = statusCode.split("\\.");

        // Log a warning if the classification is a client notification.
        if (statusCodeComp[1].equals("ClientNotification")) {
          logger.warn(String.format("Received notification message from "
              + "server: %s", errorObject.toString()));
        } else {
          logger.error(String.format("Received error message from "
              + "server: %s", errorObject.toString()));
          // Add this to the list of actual errors.
          actualErrorsBldr.add(errorObject);
          errorsDetected = true;
        }
      }

      /*
       * If there were any errors other than ClientNotifications then we need
       * to throw an exception. Something has gone terribly wrong :(
       */
      if (errorsDetected) {
        /*
         * Errors reported by the server indicate that the transaction has been
         * rolled back.
         */
        currentTxLocation = null;

        throw new Neo4jTransactionException(
            Json.createObjectBuilder()
            .add("errors", actualErrorsBldr)
            .build()
            .toString());
      }
    }

    /*
     * If we've made it this far it means all statements have executed
     * successfully and returned results without errors. Parse the array of
     * results and return.
     */
    return parseResults(resultsArray);
  }

  /**
   * Same as {@link #exec()}, except that this method will commit the
   * transaction before returning to the caller. In the case that there is no
   * currently open transaction context, this method takes advantage of Neo4j's
   * ability to execute a sequence of statements in a new transaction and
   * commit the transaction all within a single request to the server. If
   * there's an open transaction but no statements in the queue, then the
   * behavior is the same as {@link #commit()}. If there are no statements in
   * the queue and no open transaction, then the method returns immediately
   * with an empty results list.
   * <p>
   * If an error is encountered and an exception is thrown, the driver leaves
   * itself in a "reset" state (cleared statement queue, cleared transaction
   * state). Future operations will execute in a new transaction.
   * <p>
   * Usage Notes:<br>
   * This method is particularly convenient and efficient for situations where
   * the decision to commit is not dependent on statement execution results
   * (for instance, adding a node or set of nodes to the graph). A typical use
   * case might look something like this:<br>
   * <code>
   * while ((line = employees.readLine()) != null) {<br>
   * &nbsp;&nbsp;// make cypher CREATE statement for employee<br>
   * &nbsp;&nbsp;driver.enqueue(stmt);<br>
   * }<br>
   * driver.execAndCommit();<br>
   * </code>
   *
   * @return List of results, where the ordering matches the statement enqueue
   * ordering in the driver (that is, the nth result is for the nth enqueued
   * statement). Each result is a map from column titles to columns.
   *
   * @throws Neo4jCommunicationException If there is a communication problem
   * with the server.
   * @throws Neo4jTransactionException If there was a problem with executing
   * one or more statements on the server.
   */
  public List<Map<String, String[]>> execAndCommit() {
    /*
     * Special behavior in the case that the statement queue is empty. This
     * probably represents a misuse of the driver, but instead of throwing an
     * exception just do the right thing.
     */
    if (statementQueue.isEmpty()) {
      if (currentTxLocation != null) {
        // Behavior in this case is the same as commit.
        commit();
        // Didn't execute any statements, return empty results list.
        return Collections.emptyList();
      } else {
        /*
         * The statement queue is empty and there's no open transaction. In
         * this case the correct behavior is the same as the successfull commit
         * of an empty transaction (a nop transaction).
         */
        return Collections.emptyList(); // Return empty results list.
      }
    }

    // Figure out what the URI should be based on whether or not we have a 
    // currently open transaction context.
    String uri;
    if (currentTxLocation == null) {
      // Open a new transaction for immediate committing.
      uri = serverRootURI + "/transaction/commit";
    } else {
      // Use the current transaction commit location.
      uri = currentTxLocation + "/commit";
    }

    // Construct the payload body from statements in the queue.
    String requestPayload = makeTxReqPayload();

    /*
     * We don't actually need the statements in the queue beyond this point. If
     * the execution fails for any reason, the specified behavior of the driver
     * is to be in a clean "reset" state.
     */
    statementQueue.clear();

    /*
     * Build and send HTTP request. In the event of any error, reset the driver
     * and throw a Neo4jCommunicationException.
     */
    ClientResponse response;
    try {
      response = client.resource(uri)
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .entity(requestPayload)
          .post(ClientResponse.class);
    } catch (Exception e) { // Pokemon Exception Handling
      throw new Neo4jCommunicationException(e);
    } finally {
      /*
       * If sending/recieving the HTTP request/reponse encountered a failure,
       * although the transaction may be left open on the server, the driver's
       * specified behavior is to be left in a clean "reset" state, and so we
       * forget about the transaction in this case. The Neo4j server itself
       * will eventually timeout and rollback any accidentally left open
       * transactions.
       *
       * If the request got through OK, then the transaction has either been
       * successfully committed, or there was an error and it was rolled back
       * by the Neo4j server. Either way, we can forget about the transaction
       * at this point.
       */
      currentTxLocation = null;
    }

    // Parse the JSON-formatted response.
    String responsePayload = response.getEntity(String.class);
    JsonReader reader = Json.createReader(new StringReader(responsePayload));
    JsonObject responseJsonObj = reader.readObject();
    reader.close();

    // Finished with the response, can close it now.
    response.close();

    // Grab the array of results and array of errors.
    JsonArray resultsArray = responseJsonObj.getJsonArray("results");
    JsonArray errorsArray = responseJsonObj.getJsonArray("errors");

    if (!errorsArray.isEmpty()) {
      /*
       * Log all errors that are of the "ClientNotification" classification.
       * These are informational warnings and typically relate to performance,
       * but do not indicate an actual error. These are also unlikely to be
       * things a client application can be reasonably expected to respond to.
       * If there are errors of any other classification, these are interpreted
       * to be actual errors and are collected together and put into the
       * message of a Neo4jTransactionException. These errors do indicate a
       * failure of some kind and an inability to proceed normally without the
       * client application taking action (if it can, although for many such
       * errors the client application may not have the ability to respond).
       * See Neo4j documentation on "status codes" for more information.
       */

      JsonArrayBuilder actualErrorsBldr = Json.createArrayBuilder();
      boolean errorsDetected = false;
      for (int i = 0; i < errorsArray.size(); i++) {
        /*
         * Get the status code and parse its format. Status codes have the
         * following format: Neo.[Classification].[Category].[Title]
         */
        JsonObject errorObject = errorsArray.getJsonObject(i);
        String statusCode = errorObject.getString("code");
        String[] statusCodeComp = statusCode.split("\\.");

        // Log a warning if the classification is a client notification.
        if (statusCodeComp[1].equals("ClientNotification")) {
          logger.warn(String.format("Received notification message from "
              + "server: %s", errorObject.toString()));
        } else {
          logger.error(String.format("Received error message from "
              + "server: %s", errorObject.toString()));
          // Add this to the list of actual errors.
          actualErrorsBldr.add(errorObject);
          errorsDetected = true;
        }
      }

      /*
       * If there were any errors other than ClientNotifications then we need
       * to throw an exception. Something has gone terribly wrong :(
       */
      if (errorsDetected) {
        throw new Neo4jTransactionException(
            Json.createObjectBuilder()
            .add("errors", actualErrorsBldr)
            .build()
            .toString());
      }
    }

    /*
     * If we've made it this far it means all statements have executed
     * successfully and returned results without errors. Parse the array of
     * results and return.
     */
    return parseResults(resultsArray);
  }

  /**
   * Commit the current transaction. If there is no open transaction then the
   * method returns immediately.
   * <p>
   * If an error is encountered and an exception is thrown, the driver leaves
   * itself in a "reset" state (cleared statement queue, cleared transaction
   * state). Future operations will execute in a new transaction.
   *
   * @throws Neo4jCommunicationException If there is a communication problem
   * with the server.
   * @throws Neo4jTransactionException If there was a problem with executing
   * one or more statements on the server.
   */
  public void commit() {
    /*
     * Special behavior in the case that there's no open transaction. This
     * probably represents a misuse of the driver, but instead of throwing an
     * exception just do the right thing.
     */
    if (currentTxLocation == null) {
      // Behavior is as if we successfully committed a nop transaction.
      return;
    } else {
      String uri = currentTxLocation + "/commit";

      /*
       * Build and send HTTP request. In the event of any error, reset the
       * driver and throw a Neo4jCommunicationException.
       */
      ClientResponse response;
      try {
        response = client.resource(uri)
            .accept(MediaType.APPLICATION_JSON)
            .type(MediaType.APPLICATION_JSON)
            .post(ClientResponse.class);
      } catch (Exception e) { // Pokemon Exception Handling
        throw new Neo4jCommunicationException(e);
      } finally {
        /*
         * If sending/recieving the HTTP request/reponse encountered a failure,
         * although the transaction may be left open on the server, the
         * driver's specified behavior is to be left in a clean "reset" state,
         * and so we forget about the transaction in this case. The Neo4j
         * server itself will eventually timeout and rollback any accidentally
         * left open transactions.
         *
         * If the request got through OK, then the transaction has either been
         * successfully committed, or there was an error and it was rolled back
         * by the Neo4j server. Either way, we can forget about the transaction
         * at this point.
         */
        currentTxLocation = null;
      }

      // Parse the JSON-formatted response.
      String responsePayload = response.getEntity(String.class);
      JsonReader reader = Json.createReader(new StringReader(responsePayload));
      JsonObject responseJsonObj = reader.readObject();
      reader.close();

      // Finished with the response, can close it now.
      response.close();

      // Grab the array of errors.
      JsonArray errorsArray = responseJsonObj.getJsonArray("errors");

      if (!errorsArray.isEmpty()) {
        /*
         * Log all errors that are of the "ClientNotification" classification.
         * These are informational warnings and typically relate to
         * performance, but do not indicate an actual error. These are also
         * unlikely to be things a client application can be reasonably
         * expected to respond to. If there are errors of any other
         * classification, these are interpreted to be actual errors and are
         * collected together and put into the message of a
         * Neo4jTransactionException. These errors do indicate a failure of
         * some kind and an inability to proceed normally without the client
         * application taking action (if it can, although for many such errors
         * the client application may not have the ability to respond). See
         * Neo4j documentation on "status codes" for more information.
         */

        JsonArrayBuilder actualErrorsBldr = Json.createArrayBuilder();
        boolean errorsDetected = false;
        for (int i = 0; i < errorsArray.size(); i++) {
          /*
           * Get the status code and parse its format. Status codes have the
           * following format: Neo.[Classification].[Category].[Title]
           */
          JsonObject errorObject = errorsArray.getJsonObject(i);
          String statusCode = errorObject.getString("code");
          String[] statusCodeComp = statusCode.split("\\.");

          // Log a warning if the classification is a client notification.
          if (statusCodeComp[1].equals("ClientNotification")) {
            logger.warn(String.format("Received notification message from "
                + "server: %s", errorObject.toString()));
          } else {
            logger.error(String.format("Received error message from "
                + "server: %s", errorObject.toString()));
            // Add this to the list of actual errors.
            actualErrorsBldr.add(errorObject);
            errorsDetected = true;
          }
        }

        /*
         * If there were any errors other than ClientNotifications then we need
         * to throw an exception. Something has gone terribly wrong :(
         */
        if (errorsDetected) {
          throw new Neo4jTransactionException(
              Json.createObjectBuilder()
              .add("errors", actualErrorsBldr)
              .build()
              .toString());
        }
      }
    }
  }

  /**
   * Rollback current transaction. If there is no open transaction then the
   * method returns immediately.
   * <p>
   * If an error is encountered and an exception is thrown, the driver leaves
   * itself in a "reset" state (cleared statement queue, cleared transaction
   * state). Future operations will execute in a new transaction.
   * <p>
   * Note that although normally errors indicate that the transaction has been
   * automatically rolled back on the server anyway, one particular error is an
   * exception: Neo.DatabaseError.Transaction.CouldNotRollback. Although the
   * driver will reset itself so that future operations will open a new
   * transaction, if the user needs to know about this kind of error, they
   * should parse the message in the Neo4jTransactionException, or check the
   * logs.
   *
   * @throws Neo4jCommunicationException If there is a communication problem
   * with the server.
   * @throws Neo4jTransactionException If there was a problem with executing
   * one or more statements on the server.
   */
  public void rollback() {
    /*
     * Special behavior in the case that there's no open transaction. This
     * probably represents a misuse of the driver, but instead of throwing an
     * exception just do the right thing.
     */
    if (currentTxLocation == null) {
      // Behavior is as if we successfully committed a nop transaction.
      return;
    } else {
      /*
       * Build and send HTTP request. In the event of any error, reset the
       * driver and throw a Neo4jCommunicationException.
       */
      ClientResponse response;
      try {
        response = client.resource(currentTxLocation)
            .accept(MediaType.APPLICATION_JSON)
            .type(MediaType.APPLICATION_JSON)
            .delete(ClientResponse.class);
      } catch (Exception e) { // Pokemon Exception Handling
        throw new Neo4jCommunicationException(e);
      } finally {
        /*
         * If sending/recieving the HTTP request/reponse encountered a failure,
         * although the transaction may be left open on the server, the
         * driver's specified behavior is to be left in a clean "reset" state,
         * and so we forget about the transaction in this case. The Neo4j
         * server itself will eventually timeout and rollback any accidentally
         * left open transactions.
         *
         * If the request got through OK, then the transaction has been
         * successfully rolled back, unless the server responds with a
         * Neo.DatabaseError.Transaction.CouldNotRollback error. In that case
         * an exception will be thrown. We reset the driver state in this case
         * so that new transactions can be executed.
         */
        currentTxLocation = null;
      }

      // Parse the JSON-formatted response.
      String responsePayload = response.getEntity(String.class);
      JsonReader reader = Json.createReader(new StringReader(responsePayload));
      JsonObject responseJsonObj = reader.readObject();
      reader.close();

      // Finished with the response, can close it now.
      response.close();

      // Grab the array of errors.
      JsonArray errorsArray = responseJsonObj.getJsonArray("errors");

      if (!errorsArray.isEmpty()) {
        /*
         * Log all errors that are of the "ClientNotification" classification.
         * These are informational warnings and typically relate to
         * performance, but do not indicate an actual error. These are also
         * unlikely to be things a client application can be reasonably
         * expected to respond to. If there are errors of any other
         * classification, these are interpreted to be actual errors and are
         * collected together and put into the message of a
         * Neo4jTransactionException. These errors do indicate a failure of
         * some kind and an inability to proceed normally without the client
         * application taking action (if it can, although for many such errors
         * the client application may not have the ability to respond). See
         * Neo4j documentation on "status codes" for more information.
         */

        JsonArrayBuilder actualErrorsBldr = Json.createArrayBuilder();
        boolean errorsDetected = false;
        for (int i = 0; i < errorsArray.size(); i++) {
          /*
           * Get the status code and parse its format. Status codes have the
           * following format: Neo.[Classification].[Category].[Title]
           */
          JsonObject errorObject = errorsArray.getJsonObject(i);
          String statusCode = errorObject.getString("code");
          String[] statusCodeComp = statusCode.split("\\.");

          // Log a warning if the classification is a client notification.
          if (statusCodeComp[1].equals("ClientNotification")) {
            logger.warn(String.format("Received notification message from "
                + "server: %s", errorObject.toString()));
          } else {
            logger.error(String.format("Received error message from "
                + "server: %s", errorObject.toString()));
            // Add this to the list of actual errors.
            actualErrorsBldr.add(errorObject);
            errorsDetected = true;
          }
        }

        /*
         * If there were any errors other than ClientNotifications then we need
         * to throw an exception. Something has gone terribly wrong :(
         */
        if (errorsDetected) {
          throw new Neo4jTransactionException(
              Json.createObjectBuilder()
              .add("errors", actualErrorsBldr)
              .build()
              .toString());
        }
      }
    }
  }

  /**
   * Generates a JSON formatted HTTP request payload body from the statements
   * in the queue in the structure required by the server.
   * <p>
   * Server requires a JSON object that has the following structure:<br>
   * <code>
   * {"statements" : [<br>
   * {"statement" : "CREATE (n) RETURN id(n)"},<br>
   * {"statement" : "CREATE (n {props}) RETURN n", "parameters" : {"props" :
   * {"name" : "My Node"}}}<br>
   * ]}<br>
   * </code>
   *
   * @return String containing JSON object of statements.
   */
  private String makeTxReqPayload() {
    String payloadBody = "{\"statements\": [";

    for (int i = 0; i < statementQueue.size(); i++) {
      if (i > 0) {
        payloadBody += ", ";
      }

      Neo4jCypherStatement stmt = statementQueue.get(i);
      payloadBody += "{\"statement\": \"" + stmt.getStatement() + "\"";
      if (stmt.hasParameters()) {
        payloadBody += ", \"parameters\": " + stmt.getParameters();
      }
      payloadBody += "}";
    }

    payloadBody += "]}";

    return payloadBody;
  }

  /**
   * Parses a JSON array of statement execution results received from the
   * server. Each element of the JSON array is a JSON object that represents
   * execution results of a single statement. Results are in the format of a
   * table with rows and columns. This table is parsed from the JSON object as
   * a map from column titles to columns.
   *
   * @param results JSON array received from the server, where each element of
   * the array represents the execution results of a statement. (see Neo4j
   * documentation).
   *
   * @return Map of columns, where the map key is the column title.
   */
  private List<Map<String, String[]>> parseResults(JsonArray resultsArray) {
    List<Map<String, String[]>> results =
        new ArrayList<>(resultsArray.size());
    for (int i = 0; i < resultsArray.size(); i++) {
      JsonObject resultObject = resultsArray.getJsonObject(i);
      JsonArray columns = resultObject.getJsonArray("columns");
      JsonArray rows = resultObject.getJsonArray("data");

      Map<String, String[]> table = new HashMap<>();

      List<String> colNames = new ArrayList<>(columns.size());
      for (int c = 0; c < columns.size(); c++) {
        String colName = columns.getString(c);
        table.put(colName, new String[rows.size()]);
        colNames.add(colName);
      }

      for (int r = 0; r < rows.size(); r++) {
        JsonArray row = rows.getJsonObject(r).getJsonArray("row");
        for (int c = 0; c < columns.size(); c++) {
          if (row.get(c).getValueType() == ValueType.STRING) {
            table.get(colNames.get(c))[r] = row.getString(c);
          } else {
            table.get(colNames.get(c))[r] = row.get(c).toString();
          }
        }
      }

      results.add(table);
    }

    return results;
  }
}
