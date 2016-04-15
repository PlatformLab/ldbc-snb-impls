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
package net.ellitron.ldbcsnbimpls.interactive.torc;

import net.ellitron.ldbcsnbimpls.snb.Entity;

import net.ellitron.torc.util.UInt128;

/** 
 * This is a helper class for making working with LDBC entity Ids at the
 * gremlin console a little easier. Instead of needing to type out the
 * construction of a new UInt128, by statically importing IdGenerator methods,
 * a user can simply type 'g.V(genPersonId(5))'
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class IdGenerator {

    public static UInt128 gen(Entity ent, long id) {
        return new UInt128(ent.getNumber(), id);
    }

    public static UInt128 genCommentId(long id) {
        return gen(Entity.COMMENT, id);
    }

    public static UInt128 genForumId(long id) {
        return gen(Entity.FORUM, id);
    }

    public static UInt128 genMessageId(long id) {
        return gen(Entity.MESSAGE, id);
    }

    public static UInt128 genOrganisationId(long id) {
        return gen(Entity.ORGANISATION, id);
    }

    public static UInt128 genPersonId(long id) {
        return gen(Entity.PERSON, id);
    }

    public static UInt128 genPlaceId(long id) {
        return gen(Entity.PLACE, id);
    }

    public static UInt128 genPostId(long id) {
        return gen(Entity.POST, id);
    }

    public static UInt128 genTagId(long id) {
        return gen(Entity.TAG, id);
    }

    public static UInt128 genTagClassId(long id) {
        return gen(Entity.TAGCLASS, id);
    }


}
