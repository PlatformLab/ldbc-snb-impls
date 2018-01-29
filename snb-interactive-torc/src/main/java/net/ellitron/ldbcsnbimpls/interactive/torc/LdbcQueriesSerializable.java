/* 
 * Copyright (C) 2015-2018 Stanford University
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

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import java.io.*;
import java.net.*;
import java.util.*;

public class LdbcQueriesSerializable {

  public static class LdbcQuery1Serializable implements Serializable {
      public final long personId;
      public final String firstName;
      public final int limit;

      public LdbcQuery1Serializable(LdbcQuery1 query) {
        this.personId = query.personId();
        this.firstName = query.firstName();
        this.limit = query.limit();
      }  

      public LdbcQuery1 getQuery() {
        return new LdbcQuery1(personId,
                              firstName,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery1{" + 
                  "personId=" + personId + ", " +
                  "firstName=" + firstName + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery2Serializable implements Serializable {
      public final long personId;
      public final Date maxDate;
      public final int limit;

      public LdbcQuery2Serializable(LdbcQuery2 query) {
        this.personId = query.personId();
        this.maxDate = query.maxDate();
        this.limit = query.limit();
      }  

      public LdbcQuery2 getQuery() {
        return new LdbcQuery2(personId,
                              maxDate,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery2{" + 
                  "personId=" + personId + ", " +
                  "maxDate=" + maxDate + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery3Serializable implements Serializable {
      public final long personId;
      public final String countryXName;
      public final String countryYName;
      public final Date startDate;
      public final int durationDays;
      public final int limit;

      public LdbcQuery3Serializable(LdbcQuery3 query) {
        this.personId = query.personId();
        this.countryXName = query.countryXName();
        this.countryYName = query.countryYName();
        this.startDate = query.startDate();
        this.durationDays = query.durationDays();
        this.limit = query.limit();
      }  

      public LdbcQuery3 getQuery() {
        return new LdbcQuery3(personId,
                              countryXName,
                              countryYName,
                              startDate,
                              durationDays,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery3{" + 
                  "personId=" + personId + ", " +
                  "countryXName=" + countryXName + ", " +
                  "countryYName=" + countryYName + ", " +
                  "startDate=" + startDate + ", " +
                  "durationDays=" + durationDays + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery4Serializable implements Serializable {
      public final long personId;
      public final Date startDate;
      public final int durationDays;
      public final int limit;

      public LdbcQuery4Serializable(LdbcQuery4 query) {
        this.personId = query.personId();
        this.startDate = query.startDate();
        this.durationDays = query.durationDays();
        this.limit = query.limit();
      }  

      public LdbcQuery4 getQuery() {
        return new LdbcQuery4(personId,
                              startDate,
                              durationDays,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery4{" + 
                  "personId=" + personId + ", " +
                  "startDate=" + startDate + ", " +
                  "durationDays=" + durationDays + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery5Serializable implements Serializable {
      public final long personId;
      public final Date minDate;
      public final int limit;

      public LdbcQuery5Serializable(LdbcQuery5 query) {
        this.personId = query.personId();
        this.minDate = query.minDate();
        this.limit = query.limit();
      }  

      public LdbcQuery5 getQuery() {
        return new LdbcQuery5(personId,
                              minDate,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery5{" + 
                  "personId=" + personId + ", " +
                  "minDate=" + minDate + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery6Serializable implements Serializable {
      public final long personId;
      public final String tagName;
      public final int limit;

      public LdbcQuery6Serializable(LdbcQuery6 query) {
        this.personId = query.personId();
        this.tagName = query.tagName();
        this.limit = query.limit();
      }  

      public LdbcQuery6 getQuery() {
        return new LdbcQuery6(personId,
                              tagName,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery6{" + 
                  "personId=" + personId + ", " +
                  "tagName=" + tagName + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery7Serializable implements Serializable {
      public final long personId;
      public final int limit;

      public LdbcQuery7Serializable(LdbcQuery7 query) {
        this.personId = query.personId();
        this.limit = query.limit();
      }  

      public LdbcQuery7 getQuery() {
        return new LdbcQuery7(personId,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery7{" + 
                  "personId=" + personId + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery8Serializable implements Serializable {
      public final long personId;
      public final int limit;

      public LdbcQuery8Serializable(LdbcQuery8 query) {
        this.personId = query.personId();
        this.limit = query.limit();
      }  

      public LdbcQuery8 getQuery() {
        return new LdbcQuery8(personId,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery8{" + 
                  "personId=" + personId + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery9Serializable implements Serializable {
      public final long personId;
      public final Date maxDate;
      public final int limit;

      public LdbcQuery9Serializable(LdbcQuery9 query) {
        this.personId = query.personId();
        this.maxDate = query.maxDate();
        this.limit = query.limit();
      }  

      public LdbcQuery9 getQuery() {
        return new LdbcQuery9(personId,
                              maxDate,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery9{" + 
                  "personId=" + personId + ", " +
                  "maxDate=" + maxDate + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery10Serializable implements Serializable {
      public final long personId;
      public final int month;
      public final int limit;

      public LdbcQuery10Serializable(LdbcQuery10 query) {
        this.personId = query.personId();
        this.month = query.month();
        this.limit = query.limit();
      }  

      public LdbcQuery10 getQuery() {
        return new LdbcQuery10(personId,
                              month,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery10{" + 
                  "personId=" + personId + ", " +
                  "month=" + month + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery11Serializable implements Serializable {
      public final long personId;
      public final String countryName;
      public final int workFromYear;
      public final int limit;

      public LdbcQuery11Serializable(LdbcQuery11 query) {
        this.personId = query.personId();
        this.countryName = query.countryName();
        this.workFromYear = query.workFromYear();
        this.limit = query.limit();
      }  

      public LdbcQuery11 getQuery() {
        return new LdbcQuery11(personId,
                              countryName,
                              workFromYear,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery11{" + 
                  "personId=" + personId + ", " +
                  "countryName=" + countryName + ", " +
                  "workFromYear=" + workFromYear + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery12Serializable implements Serializable {
      public final long personId;
      public final String tagClassName;
      public final int limit;

      public LdbcQuery12Serializable(LdbcQuery12 query) {
        this.personId = query.personId();
        this.tagClassName = query.tagClassName();
        this.limit = query.limit();
      }  

      public LdbcQuery12 getQuery() {
        return new LdbcQuery12(personId,
                              tagClassName,
                              limit);
      } 

      @Override
      public String toString() {
        return "LdbcQuery12{" + 
                  "personId=" + personId + ", " +
                  "tagClassName=" + tagClassName + ", " +
                  "limit=" + limit + "}";

      }
  }

  public static class LdbcQuery13Serializable implements Serializable {
      public final long person1Id;
      public final long person2Id;

      public LdbcQuery13Serializable(LdbcQuery13 query) {
        this.person1Id = query.person1Id();
        this.person2Id = query.person2Id();
      }  

      public LdbcQuery13 getQuery() {
        return new LdbcQuery13(person1Id,
                              person2Id);
      } 

      @Override
      public String toString() {
        return "LdbcQuery13{" + 
                  "person1Id=" + person1Id + ", " +
                  "person2Id=" + person2Id + "}";

      }
  }

  public static class LdbcQuery14Serializable implements Serializable {
      public final long person1Id;
      public final long person2Id;

      public LdbcQuery14Serializable(LdbcQuery14 query) {
        this.person1Id = query.person1Id();
        this.person2Id = query.person2Id();
      }  

      public LdbcQuery14 getQuery() {
        return new LdbcQuery14(person1Id,
                              person2Id);
      } 

      @Override
      public String toString() {
        return "LdbcQuery14{" + 
                  "person1Id=" + person1Id + ", " +
                  "person2Id=" + person2Id + "}";

      }
  }

}
