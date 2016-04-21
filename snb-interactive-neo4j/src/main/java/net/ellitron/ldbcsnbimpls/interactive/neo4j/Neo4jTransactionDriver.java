/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ellitron.ldbcsnbimpls.interactive.neo4j;

/**
 * A class for driving the execution of transactional Cypher statements against
 * a remote Neo4j server running a transactional Cypher HTTP endpoint. The
 * driver is designed to be used primarily in one of two ways, interactive
 * style or batch style. In interactive style the application executes one or
 * more statements and views the results before executing more statements and
 * eventually committing the transaction (or rolling it back). An example may
 * be reading a subgraph in one statement, computing page-rank, then writing
 * page rank values to the vertices in a second statement. In batch style the
 * application doesn't need to see intermediate results, and so opens a new
 * transaction, executes one or more statements, and commits the transaction in
 * a single request to the server. An example may be adding a "likes" edge in
 * the graph between a specific user and a specific post, which can be
 * expressed in a single cypher statement.
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
 * <li>Open transactions on the Neo4j server are automatically rolled back
 * after a timeout period. Be sure to adjust your timeout configuration value
 * to meet your application needs.</li>
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
   * Execute statements in the current transaction context and return the
   * results. If there is no currently open transaction context, a new one is
   * started on the server. Subsequent calls will continue to execute
   * statements in the same transaction. Result ordering in the returned array
   * matches the statement ordering in the arguments.
   * <p>
   * Usage Notes:<br>
   * This method is used to execute statements in an interactive fashion where
   * the results can be viewed before executing more statements and eventually
   * deciding whether to commit the transaction or roll it back. Note, however,
   * that Neo4j transactions have a timeout value configured on the server, and
   * if enough time passes without activity then the transaction is
   * automatically rolled back. The timeout is reset each time statement(s) are
   * executed.
   *
   * @param stmts Neo4jStatement(s) to execute. Statements are executed in the
   * order they appear in the arguments.
   *
   * @return Array of results, where the ordering matches with the statement
   * ordering in the arguments. That is, the ith result is for the ith
   * statement in the argument.
   */
  public Neo4jStatementResult[] exec(Neo4jStatement... stmts) {

  }

  /**
   * Execute statements in the current transaction context, commit the
   * transaction, and return the results. If there is no currently open
   * transaction context, a new one is started on the server (and subsequently
   * committed before returning). Result ordering in the returned array matches
   * the statement ordering in the arguments.
   * <p>
   * Usage Notes:<br>
   * This method is most commonly used to open a new transaction, execute one
   * or more statements, and (blindly) commit the transaction all in one-shot.
   * The Neo4j HTTP endpoint supports this combined execute and commit
   * operation in a single request to the server, affording efficiency gains
   * when the application does not need to see the execution results before
   * deciding to commit. Of course, this method can be called during a running
   * transaction as well to "top it off" with a few more statements and commit
   * the whole transaction in one request.
   *
   * @param stmts Neo4jStatement(s) to execute. Statements are executed in the
   * order they appear in the arguments.
   *
   * @return Array of results, where the ordering matches with the statement
   * ordering in the arguments. That is, the ith result is for the ith
   * statement in the argument.
   */
  public Neo4jStatementResult[] execAndCommit(Neo4jStatement... stmts) {

  }

  /**
   * Commit the currently open transaction. If there is no open transaction
   * then the method returns immediately.
   */
  public void commit() {

  }

  /**
   * Roll back the currently open transaction. If there is no open transaction
   * then the method returns immediately.
   */
  public void rollback() {

  }
}
