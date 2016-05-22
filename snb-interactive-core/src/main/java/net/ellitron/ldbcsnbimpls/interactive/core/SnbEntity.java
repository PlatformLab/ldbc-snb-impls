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
package net.ellitron.ldbcsnbimpls.interactive.core;

/**
 * Enumeration of all entities as defined in the LDBC Social Network Benchmark.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public enum SnbEntity {

  COMMENT("comment"),
  FORUM("forum"),
  ORGANISATION("organisation"),
  PERSON("person"),
  PLACE("place"),
  POST("post"),
  TAG("tag"),
  TAGCLASS("tagclass");

  /*
   * The name of the entity.
   */
  public final String name;

  private SnbEntity(String name) {
    this.name = name;
  }
}
