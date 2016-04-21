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

/**
 * A class for driving the execution of transactional Cypher statements against
 * a remote Neo4j server over the Neo4j REST API. An application uses the
 * driver by queuing a sequence of statements in the driver, then calling
 * methods to execute those statements in different ways.
 * <p>
 * In addition to supporting typical interactive style transactions where the
 * application executes statements and views results in a loop before
 * ultimately deciding to commit or rollback, the driver also supports "blind"
 * transactions where the application composes one or more statements and both
 * executes and commits in one request to the server. In certain simple
 * situations (for instance, adding a node to the graph) this can save a round
 * trip to the database and save latency.
 * <p>
 *
 * Failure Scenarios:<br>
 * Since the operation of the driver depends on communicating over the network
 * to a remote server, there are a number of basic failure scenarios the user
 * should prepare for. At a high level there are three basic failure scenarios:
 * <ul>
 * <li>Cannot establish connection to the server</li>
 * <li>Connection established, but not getting a request response</li>
 * <li>Got a response, but it represents an error (for instance a timed-out
 * transaction or a server-specific error)</li>
 * </ul>
 * These failure cases are described in more detail in the class method
 * documentation.
 * <p>
 *
 * Special Notes:<br>
 * <ul>
 * <li>This class is not thread-safe. Although a thread can have many drivers,
 * a driver can have only one thread using it.</li>
 * <li>The driver does not currently support authentication with the server.
 * Authentication must be turned off (see Neo4j documentation).</li>
 * <li>Open transactions on the Neo4j server are automatically rolled back
 * after a timeout period of inactivity. Be sure to adjust your timeout
 * configuration value to meet your application needs.</li>
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
   * Add one or more statements to the driver statement queue for future
   * execution. When executed, statements are always executed in FIFO order. A
   * call to exec, execAndCommit, or rollback will generally have the effect of
   * clearing the statement queue unless otherwise noted.
   *
   * @param stmts Neo4jStatement(s) to enqueue for later execution.
   */
  public void enqueue(Neo4jStatement... stmts) {

  }

  /**
   * Execute enqueued statements in the current transaction context, clear the
   * statement queue, and return the results. If there is no currently open
   * transaction context then a new one is opened on the server automatically.
   * Subsequent calls will then execute newly enqueued statements in the same
   * transaction until a call to either commit or rollback is made.
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
   * @return Array of results, where the ordering matches the statement enqueue
   * ordering in the driver (that is, the nth result is for the nth enqueued
   * statement).
   */
  public Neo4jStatementResult[] exec() {
    return null;
  }

  /**
   * Same as {@link #exec()}, except that this method will commit the
   * transaction before returning to the caller. In the case that there is no
   * currently open transaction context, this method takes advantage of Neo4j's
   * ability to execute a sequence of statements in a new transaction and
   * commit the transaction all within a single request to the server.
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
   * @return Array of results, where the ordering matches the statement enqueue
   * ordering in the driver (that is, the nth result is for the nth enqueued
   * statement).
   */
  public Neo4jStatementResult[] execAndCommit() {
    return null;
  }

  /**
   * Commit the current transaction. If there is no open transaction then the
   * method returns immediately.
   */
  public void commit() {

  }

  /**
   * Rollback current transaction. If there is no open transaction then the
   * method returns immediately.
   */
  public void rollback() {

  }
}
