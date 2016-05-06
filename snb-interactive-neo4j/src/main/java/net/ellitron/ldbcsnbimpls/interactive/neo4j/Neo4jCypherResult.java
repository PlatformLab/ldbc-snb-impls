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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

/**
 * Encapsulates the results of executing a single statement of Cypher, and
 * provides convenient methods for accessing those results. Hides the details
 * of the specific JSON format used by the server.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jCypherResult {

  private final JsonObject result;

  public Neo4jCypherResult(JsonObject result) {
    this.result = result;
  }

  public JsonArray getColumns() {
    return result.getJsonArray("columns");
  }
  
  public int rows() {
    return result.getJsonArray("data").size();
  }

  public JsonArray getRow(int i) {
    return result.getJsonArray("data").getJsonObject(i).getJsonArray("row");
  }

  /**
   * Converts this result into a map of column names to columns. Each column is 
   * represented as an array of Strings. 
   *
   * @return Map of columns, where the map key is the column name.
   */
  public Map<String, String[]> toMap() {
    JsonArray columns = result.getJsonArray("columns");
    JsonArray rows = result.getJsonArray("data");

    Map<String, String[]> map = new HashMap<>();

    List<String> colNames = new ArrayList<>(columns.size());
    for (int c = 0; c < columns.size(); c++) {
      String colName = columns.getString(c);
      map.put(colName, new String[rows.size()]);
      colNames.add(colName);
    }

    for (int r = 0; r < rows.size(); r++) {
      JsonArray row = rows.getJsonObject(r).getJsonArray("row");
      for (int c = 0; c < columns.size(); c++) {
        if (row.get(c).getValueType() == JsonValue.ValueType.STRING) {
          map.get(colNames.get(c))[r] = row.getString(c);
        } else {
          map.get(colNames.get(c))[r] = row.get(c).toString();
        }
      }
    }

    return map;
  }
  
  @Override
  public String toString() {
    return result.toString();
  }
}
