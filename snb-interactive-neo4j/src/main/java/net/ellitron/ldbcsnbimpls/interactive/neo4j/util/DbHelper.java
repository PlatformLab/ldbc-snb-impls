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
import java.util.ArrayList;

/**
 * A collection of static methods used as helper methods in the implementation
 * of the LDBC SNB interactive workload for Neo4j.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class DbHelper {

  /**
   * Take a list of Longs and convert it to a list of Strings. 
   *
   * @param list List of Longs.
   *
   * @return List of Strings.
   */
  public static List<String> listLongToListString(List<Long> list) {
    List<String> stringList = new ArrayList<>(list.size());
    for (Long l : list) {
      stringList.add(String.valueOf(l));
    }

    return stringList;
  }
}
