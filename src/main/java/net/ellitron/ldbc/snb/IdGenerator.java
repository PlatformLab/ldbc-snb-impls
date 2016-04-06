/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.ellitron.ldbc.snb;

import net.ellitron.tinkerpop.gremlin.torc.structure.util.UInt128;

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
