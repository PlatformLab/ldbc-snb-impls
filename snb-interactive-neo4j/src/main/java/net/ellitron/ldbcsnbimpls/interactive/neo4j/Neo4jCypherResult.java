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
 * Encapsulates the result of executing a cypher statement. This result could
 * be either a success result or an error result.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jCypherResult {

  private final String result;
  private final boolean error;

  /**
   * Construct a Neo4jCypherResult.
   *
   * @param result String representation of the cypher statement result.
   * @param error Boolean representing whether or not this result is an error
   * result.
   */
  public Neo4jCypherResult(String result, boolean error) {
    this.result = result;
    this.error = error;
  }

  /**
   * Getter for the result.
   *
   * @return String representation of the cypher statement result.
   */
  public String getResult() {
    return result;
  }

  /**
   * Returns whether or not this cypher statement result represents an error.
   *
   * @return Boolean representing whether or not this result is an error
   * result.
   */
  public boolean isError() {
    return error;
  }
}
