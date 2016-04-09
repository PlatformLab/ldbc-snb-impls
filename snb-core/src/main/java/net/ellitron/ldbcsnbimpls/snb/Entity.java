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
package net.ellitron.ldbcsnbimpls.snb;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public enum Entity {
    COMMENT         (1l, "comment"),
    FORUM           (2l, "forum"),
    MESSAGE         (1l, "message"),
    ORGANISATION    (3l, "organisation"),
    PERSON          (4l, "person"),
    PLACE           (5l, "place"),
    POST            (1l, "post"),
    TAG             (7l, "tag"),
    TAGCLASS        (8l, "tagclass");
    
    private final long number;
    private final String name;
    
    private Entity(final long number, final String name) {
        this.number = number;
        this.name = name;
    }
    
    public long getNumber() {
        return number;
    }
    
    public String getName() {
        return name;
    }
    
    public static Entity fromName(final String name) {
        for (Entity e : values())
            if (name.equals(e.getName()))
                return e;
        
        throw new IllegalArgumentException();
    }
}
