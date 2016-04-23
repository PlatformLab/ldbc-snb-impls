/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.ellitron.ldbcsnbimpls.interactive.neo4j;

/**
 * A runtime exception that signals there was a problem communicating with the
 * Neo4j server.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jCommunicationException extends RuntimeException {

  public Neo4jCommunicationException() {
    super();
  }

  public Neo4jCommunicationException(String message) {
    super(message);
  }

  public Neo4jCommunicationException(String message, Throwable cause) {
    super(message, cause);
  }

  public Neo4jCommunicationException(Throwable cause) {
    super(cause);
  }
}
