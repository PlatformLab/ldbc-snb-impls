/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ellitron.ldbcsnbimpls.interactive.neo4j;

/**
 * A runtime exception that signals there was a problem executing a transaction
 * on the Neo4j server.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jTransactionException extends RuntimeException {

  public Neo4jTransactionException() {
    super();
  }

  public Neo4jTransactionException(String message) {
    super(message);
  }

  public Neo4jTransactionException(String message, Throwable cause) {
    super(message, cause);
  }

  public Neo4jTransactionException(Throwable cause) {
    super(cause);
  }
}
