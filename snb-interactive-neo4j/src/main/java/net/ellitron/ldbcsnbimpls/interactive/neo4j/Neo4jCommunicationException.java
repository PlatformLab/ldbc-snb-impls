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
