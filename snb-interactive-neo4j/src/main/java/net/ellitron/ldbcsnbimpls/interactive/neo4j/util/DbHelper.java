/* 
 * Copyright (C) 2015-2016 Stanford University
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
package net.ellitron.ldbcsnbimpls.interactive.neo4j.util;

import java.util.List;

/**
 * A collection of static methods used as helper methods in the implementation
 * of the LDBC SNB interactive workload for Neo4j.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class DbHelper {

  /**
   * Take a list of Objects and serialize it to a JSON formatted array of
   * strings.
   *
   * @param list List of objects.
   *
   * @return Serialized JSON formatted string representing this list of
   * objects.
   */
  public static String listToJsonArray(List<?> list) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) {
        sb.append(", \"").append(list.get(i).toString()).append("\"");
      } else {
        sb.append("\"").append(list.get(i).toString()).append("\"");
      }
    }
    sb.append("]");
    
    return sb.toString();
  }
}
