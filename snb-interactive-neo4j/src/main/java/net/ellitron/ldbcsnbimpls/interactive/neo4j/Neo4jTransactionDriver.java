/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ellitron.ldbcsnbimpls.interactive.neo4j;

/**
 * A class for driving the execution of transactional Cypher queries against a
 * remote Neo4j server running a transactional Cypher HTTP endpoint. The basic
 * way to use this driver is to stage one or more queries in the driver via the
 * {@link #stage(String query, String params)} method, then either call
 * {@link #commit()} immediately to execute and commit those queries in one
 * request to the server, or call {@link #exec()} to execute the staged queries
 * on the server but leave the transaction open. Subsequent calls to exec will
 * continue to execute newly staged queries in the currently open transaction
 * context, until commit or {@link #rollback()} is called. This allows the
 * client to proceed more interactively, observing the results before executing
 * more queries and eventually deciding to commit/rollback. The rollback method
 * will simply rollback the currently open transaction. After a call to either
 * commit or rollback a subsequent call to exec or commit will automatically
 * open a new transaction context (and simultaneously commit that transaction
 * in the case of commit). The driver is therefore re-usable across
 * transactions.
 * <p>
 *
 * Failure Scenarios:<br>
 * Since the operation of the driver depends on communicating over the network
 * to a remote server that has state, there are a number of basic failure
 * scenarios the user should prepare for. At a high level there are three basic
 * failure scenarios:
 * <ul>
 * <li>Cannot establish connection to the server</li>
 * <li>Connection established, but not getting a request response</li>
 * <li>Got a response, but it represents an error (for instance, in the event
 * of a malformed query, a timed-out transaction, or a server error)</li>
 * </ul>
 * These failure cases are documented in more detail in the documentation for
 * the methods.
 * <p>
 *
 * Special Notes:<br>
 * <ul>
 * <li>This class is not thread-safe. Although a thread can have many drivers,
 * a driver can have only one thread using it.</li>
 * <li>The driver does not currently support authentication with the server.
 * Authentication must be turned off (see Neo4j documentation).</li>
 * </ul>
 * <p>
 *
 * TODO:<br>
 * <ul>
 * <li>Add functionality for asynchronous execution of queries (maybe something
 * like an execAsync and waitAsync method pair).</li>
 * <li>Add way for client to authenticate itself with the server over HTTP</li>
 * <li>Add way for client to authenticate itself with the server over
 * HTTPS</li>
 * </ul>
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jTransactionDriver {

  /**
   * Construct a Neo4jTransactionDriver.
   *
   * @param host IP address of the Neo4j HTTP web server.
   * @param port Port of the Neo4j HTTP web server.
   */
  public Neo4jTransactionDriver(String host, String port) {

  }

  /**
   * Stage a cypher query for later execution. Many queries can be staged for
   * execution, in which case they are executed in the same order in which they
   * were staged.
   *
   * @param query String representation of a cypher query.<br>
   * Ex: "CREATE (n {props}) RETURN n"
   * @param params JSON formatted String of the parameters to the query. If the
   * query takes no parameters, this argument should be null.<br>
   * Ex: "{ \"props\": { \"name\": \"Bob\" } }"
   */
  public void stage(String query, String params) {

  }

  /**
   * Execute staged queries in the order that they were staged. If there is an
   * open transaction context, these queries are executed within that context.
   * Otherwise a new transaction is opened on the server. The results for each
   * query are returned in an array of JSON formatted Strings, where their
   * order in the array matches their execution order.
   * <p>
   * Example:<br>
   * <code>
   * driver.stage("CREATE (n) RETURN id(n)");
   * driver.stage("CREATE (n {props}) RETURN n",
   *              "{ \"props\": { \"name\": \"Bob\" } }");
   * String[] results = driver.exec();
   * </code>
   *
   * @return Array of JSON formatted Strings where the ith String represents
   * the execution result of the ith staged query.
   */
  public String[] exec() {
    return null;
  }

  public String[] commit() {
    return null;
  }

  public void rollback() {

  }
}
