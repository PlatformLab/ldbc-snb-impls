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
 * Encapsulates a Neo4j cypher statement and its optional parameters.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jCypherStatement {

  private final String statement;
  private final String parameters;

  /**
   * Construct a Neo4jCypherStatement.
   *
   * @param statement String containing cypher statement.
   */
  public Neo4jCypherStatement(String statement) {
    this(statement, null);
  }

  /**
   * Constructor for a Neo4jCypherStatement.
   *
   * @param statement String containing cypher statement.
   * @param parameters Parameters for cypher statement.
   */
  public Neo4jCypherStatement(String statement, String parameters) {
    this.statement = statement;
    this.parameters = parameters;
  }

  /**
   * Getter for cypher statement.
   *
   * @return String representation of cypher statement.
   */
  public String getStatement() {
    return statement;
  }

  /**
   * Getting for (optional) cypher parameters. Returns null if none.
   *
   * @return String representation of cypher statement parameters. Null if
   * none.
   */
  public String getParameters() {
    return parameters;
  }
}
