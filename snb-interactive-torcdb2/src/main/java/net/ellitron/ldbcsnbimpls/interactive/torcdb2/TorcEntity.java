/* 
 * Copyright (C) 2016-2019 Stanford University
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
package net.ellitron.ldbcsnbimpls.interactive.torcdb2;

import net.ellitron.ldbcsnbimpls.interactive.core.SnbEntity;

/**
 * Associates some meta-data with SNB entities, including the label to use in
 * TorcDB2 and the ID space which it's a part of.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public enum TorcEntity {

  COMMENT(SnbEntity.COMMENT, "Comment", 1l),
  FORUM(SnbEntity.FORUM, "Forum", 2l),
  ORGANISATION(SnbEntity.ORGANISATION, "Organisation", 3l),
  PERSON(SnbEntity.PERSON, "Person", 4l),
  PLACE(SnbEntity.PLACE, "Place", 5l),
  POST(SnbEntity.POST, "Post", 1l),
  TAG(SnbEntity.TAG, "Tag", 6l),
  TAGCLASS(SnbEntity.TAGCLASS, "TagClass", 7l);

  /*
   * Core SNB entity which these properties are associated with.
   */
  public final SnbEntity entity;

  /*
   * The label to use for this entity in TorcDB2.
   */
  public final String label;

  /*
   * The ID space this entity lives in.
   */
  public final long idSpace;

  private TorcEntity(SnbEntity entity, String label, long idSpace) {
    this.entity = entity;
    this.label = label;
    this.idSpace = idSpace;
  }
  
  /**
   * Returns the TorcEntity that is associated with the given SnbEntity.
   * 
   * @param entity SnbEntity to find the TorcEntity equivalent of.
   * @return TorcEntity equivalent of the given SnbEntity.
   */
  public static TorcEntity valueOf(SnbEntity entity) {
    return TorcEntity.valueOf(entity.name());
  }
}
