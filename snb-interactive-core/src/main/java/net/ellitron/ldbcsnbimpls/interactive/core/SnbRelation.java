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

import static net.ellitron.ldbcsnbimpls.interactive.core.SnbEntity.*;

/**
 * Enumeration of all relations as defined in the LDBC Social Network
 * Benchmark.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public enum SnbRelation {

  CONTAINEROF_FORUM_POST("containerOf", FORUM, POST, true),
  HASCREATOR_COMMENT_PERSON("hasCreator", COMMENT, PERSON, true),
  HASCREATOR_POST_PERSON("hasCreator", POST, PERSON, true),
  HASINTEREST_PERSON_TAG("hasInterest", PERSON, TAG, true),
  HASMEMBER_FORUM_PERSON("hasMember", FORUM, PERSON, true),
  HASMODERATOR_FORUM_PERSON("hasModerator", FORUM, PERSON, true),
  HASTAG_COMMENT_TAG("hasTag", COMMENT, TAG, true),
  HASTAG_FORUM_TAG("hasTag", FORUM, TAG, true),
  HASTAG_POST_TAG("hasTag", POST, TAG, true),
  HASTYPE_TAG_TAGCLASS("hasType", TAG, TAGCLASS, true),
  ISLOCATEDIN_COMMENT_PLACE("isLocatedIn", COMMENT, PLACE, true),
  ISLOCATEDIN_ORGANISATION_PLACE("isLocatedIn", ORGANISATION, PLACE, true),
  ISLOCATEDIN_PERSON_PLACE("isLocatedIn", PERSON, PLACE, true),
  ISLOCATEDIN_POST_PLACE("isLocatedIn", POST, PLACE, true),
  ISPARTOF_PLACE_PLACE("isPartOf", PLACE, PLACE, true),
  ISSUBCLASSOF_TAGCLASS_TAGCLASS("isSubclassOf", TAGCLASS, TAGCLASS, true),
  KNOWS_PERSON_PERSON("knows", PERSON, PERSON, false),
  LIKES_PERSON_COMMENT("likes", PERSON, COMMENT, true),
  LIKES_PERSON_POST("likes", PERSON, POST, true),
  REPLYOF_COMMENT_COMMENT("replyOf", COMMENT, COMMENT, true),
  REPLYOF_COMMENT_POST("replyOf", COMMENT, POST, true),
  STUDYAT_PERSON_ORGANISATION("studyAt", PERSON, ORGANISATION, true),
  WORKAT_PERSON_ORGANISATION("workAt", PERSON, ORGANISATION, true);

  /*
   * The name of the relation.
   */
  public final String name;

  /*
   * Entity at the tail of the relation.
   */
  public SnbEntity tail;

  /*
   * Entity at the head of the relation.
   */
  public SnbEntity head;

  /*
   * Directionality of the relation (directed/undirected).
   */
  public boolean directed;

  private SnbRelation(String name, SnbEntity tail, SnbEntity head,
      boolean directed) {
    this.name = name;
    this.tail = tail;
    this.head = head;
    this.directed = directed;
  }
}
