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
package net.ellitron.ldbcsnbimpls.interactive.core;

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
