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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson.*;
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

public class LdbcSerializableQueriesAndResults {

  public static class LdbcQuery1Serializable implements Serializable {
      public final long personId;
      public final String firstName;
      public final int limit;

      public LdbcQuery1Serializable(LdbcQuery1 query) {
        this.personId = query.personId();
        this.firstName = query.firstName();
        this.limit = query.limit();
      }  

      public LdbcQuery1 unpack() {
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

  public static class LdbcQuery1ResultSerializable implements Serializable {
      public final long friendId;
      public final String friendLastName;
      public final int distanceFromPerson;
      public final long friendBirthday;
      public final long friendCreationDate;
      public final String friendGender;
      public final String friendBrowserUsed;
      public final String friendLocationIp;
      public final Iterable<String> friendEmails;
      public final Iterable<String> friendLanguages;
      public final String friendCityName;
      public final Iterable<List<Object>> friendUniversities;
      public final Iterable<List<Object>> friendCompanies;

      public LdbcQuery1ResultSerializable(LdbcQuery1Result query) {
          this.friendId = query.friendId();
          this.friendLastName = query.friendLastName();
          this.distanceFromPerson = query.distanceFromPerson();
          this.friendBirthday = query.friendBirthday();
          this.friendCreationDate = query.friendCreationDate();
          this.friendGender = query.friendGender();
          this.friendBrowserUsed = query.friendBrowserUsed();
          this.friendLocationIp = query.friendLocationIp();
          this.friendEmails = query.friendEmails();
          this.friendLanguages = query.friendLanguages();
          this.friendCityName = query.friendCityName();
          this.friendUniversities = query.friendUniversities();
          this.friendCompanies = query.friendCompanies();
      }

      public LdbcQuery1ResultSerializable(
              long friendId,
              String friendLastName,
              int distanceFromPerson,
              long friendBirthday,
              long friendCreationDate,
              String friendGender,
              String friendBrowserUsed,
              String friendLocationIp,
              Iterable<String> friendEmails,
              Iterable<String> friendLanguages,
              String friendCityName,
              Iterable<List<Object>> friendUniversities,
              Iterable<List<Object>> friendCompanies) {
          this.friendId = friendId;
          this.friendLastName = friendLastName;
          this.distanceFromPerson = distanceFromPerson;
          this.friendBirthday = friendBirthday;
          this.friendCreationDate = friendCreationDate;
          this.friendGender = friendGender;
          this.friendBrowserUsed = friendBrowserUsed;
          this.friendLocationIp = friendLocationIp;
          this.friendEmails = friendEmails;
          this.friendLanguages = friendLanguages;
          this.friendCityName = friendCityName;
          this.friendUniversities = friendUniversities;
          this.friendCompanies = friendCompanies;
      }

      public LdbcQuery1Result unpack() {
          return new LdbcQuery1Result(friendId,
                                      friendLastName,
                                      distanceFromPerson,
                                      friendBirthday,
                                      friendCreationDate,
                                      friendGender,
                                      friendBrowserUsed,
                                      friendLocationIp,
                                      friendEmails,
                                      friendLanguages,
                                      friendCityName,
                                      friendUniversities,
                                      friendCompanies);
      }

      @Override
      public String toString() {
          return "LdbcQuery1ResultSerializable{" +
                  "friendId=" + friendId +
                  ", friendLastName='" + friendLastName + '\'' +
                  ", distanceFromPerson=" + distanceFromPerson +
                  ", friendBirthday=" + friendBirthday +
                  ", friendCreationDate=" + friendCreationDate +
                  ", friendGender='" + friendGender + '\'' +
                  ", friendBrowserUsed='" + friendBrowserUsed + '\'' +
                  ", friendLocationIp='" + friendLocationIp + '\'' +
                  ", friendEmails=" + friendEmails +
                  ", friendLanguages=" + friendLanguages +
                  ", friendCityName='" + friendCityName + '\'' +
                  ", friendUniversities=" + friendUniversities +
                  ", friendCompanies=" + friendCompanies +
                  '}';
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

      public LdbcQuery2 unpack() {
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

  public static class LdbcQuery2ResultSerializable implements Serializable {
      public final long personId;
      public final String personFirstName;
      public final String personLastName;
      public final long postOrCommentId;
      public final String postOrCommentContent;
      public final long postOrCommentCreationDate;

      public LdbcQuery2ResultSerializable(LdbcQuery2Result query) {
          this.personId = query.personId();
          this.personFirstName = query.personFirstName();
          this.personLastName = query.personLastName();
          this.postOrCommentId = query.postOrCommentId();
          this.postOrCommentContent = query.postOrCommentContent();
          this.postOrCommentCreationDate = query.postOrCommentCreationDate();
      }

      public LdbcQuery2ResultSerializable(long personId, String personFirstName, String personLastName, long postOrCommentId, String postOrCommentContent, long postOrCommentCreationDate) {
          this.personId = personId;
          this.personFirstName = personFirstName;
          this.personLastName = personLastName;
          this.postOrCommentId = postOrCommentId;
          this.postOrCommentContent = postOrCommentContent;
          this.postOrCommentCreationDate = postOrCommentCreationDate;
      }

      public LdbcQuery2Result unpack() {
          return new LdbcQuery2Result(personId,
                                      personFirstName,
                                      personLastName,
                                      postOrCommentId,
                                      postOrCommentContent,
                                      postOrCommentCreationDate);
      }

      @Override
      public String toString() {
          return "LdbcQuery2ResultSerializable{" +
                  "personId=" + personId +
                  ", personFirstName='" + personFirstName + '\'' +
                  ", personLastName='" + personLastName + '\'' +
                  ", postOrCommentId=" + postOrCommentId +
                  ", postOrCommentContent='" + postOrCommentContent + '\'' +
                  ", postOrCommentCreationDate=" + postOrCommentCreationDate +
                  '}';
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

      public LdbcQuery3 unpack() {
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

  public static class LdbcQuery3ResultSerializable implements Serializable {
      public final long personId;
      public final String personFirstName;
      public final String personLastName;
      public final long xCount;
      public final long yCount;
      public final long count;

      public LdbcQuery3ResultSerializable(LdbcQuery3Result query) {
          this.personId = query.personId();
          this.personFirstName = query.personFirstName();
          this.personLastName = query.personLastName();
          this.xCount = query.xCount();
          this.yCount = query.yCount();
          this.count = query.count();
      }

      public LdbcQuery3ResultSerializable(long personId, String personFirstName, String personLastName, long xCount, long yCount, long count) {
          this.personId = personId;
          this.personFirstName = personFirstName;
          this.personLastName = personLastName;
          this.xCount = xCount;
          this.yCount = yCount;
          this.count = count;
      }

      public LdbcQuery3Result unpack() {
          return new LdbcQuery3Result(personId,
                                      personFirstName,
                                      personLastName,
                                      xCount,
                                      yCount,
                                      count);
      }

      @Override
      public String toString() {
          return "LdbcQuery3ResultSerializable{" +
                  "personId=" + personId +
                  ", personFirstName='" + personFirstName + '\'' +
                  ", personLastName='" + personLastName + '\'' +
                  ", xCount=" + xCount +
                  ", yCount=" + yCount +
                  ", count=" + count +
                  '}';
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

      public LdbcQuery4 unpack() {
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

  public static class LdbcQuery4ResultSerializable implements Serializable {
      public final String tagName;
      public final int postCount;

      public LdbcQuery4ResultSerializable(LdbcQuery4Result query) {
          this.tagName = query.tagName();
          this.postCount = query.postCount();
      }

      public LdbcQuery4ResultSerializable(String tagName, int postCount) {
          this.tagName = tagName;
          this.postCount = postCount;
      }

      public LdbcQuery4Result unpack() {
          return new LdbcQuery4Result(tagName,
                                      postCount);
      }

      @Override
      public String toString() {
          return "LdbcQuery4ResultSerializable{" +
                  "tagName='" + tagName + '\'' +
                  ", postCount=" + postCount +
                  '}';
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

      public LdbcQuery5 unpack() {
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

  public static class LdbcQuery5ResultSerializable implements Serializable {
      public final String forumTitle;
      public final int postCount;

      public LdbcQuery5ResultSerializable(LdbcQuery5Result query) {
          this.forumTitle = query.forumTitle();
          this.postCount = query.postCount();
      }

      public LdbcQuery5ResultSerializable(String forumTitle, int postCount) {
          this.forumTitle = forumTitle;
          this.postCount = postCount;
      }

      public LdbcQuery5Result unpack() {
          return new LdbcQuery5Result(forumTitle,
                                      postCount);
      }

      @Override
      public String toString() {
          return "LdbcQuery5ResultSerializable{" +
                  "forumTitle='" + forumTitle + '\'' +
                  ", postCount=" + postCount +
                  '}';
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

      public LdbcQuery6 unpack() {
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

  public static class LdbcQuery6ResultSerializable implements Serializable {
      public final String tagName;
      public final int postCount;

      public LdbcQuery6ResultSerializable(LdbcQuery6Result query) {
          this.tagName = query.tagName();
          this.postCount = query.postCount();
      }

      public LdbcQuery6ResultSerializable(String tagName, int postCount) {
          this.tagName = tagName;
          this.postCount = postCount;
      }

      public LdbcQuery6Result unpack() {
          return new LdbcQuery6Result(tagName,
                                      postCount);
      }

      @Override
      public String toString() {
          return "LdbcQuery6ResultSerializable{" +
                  "tagName='" + tagName + '\'' +
                  ", postCount=" + postCount +
                  '}';
      }
  }

  public static class LdbcQuery7Serializable implements Serializable {
      public final long personId;
      public final int limit;

      public LdbcQuery7Serializable(LdbcQuery7 query) {
        this.personId = query.personId();
        this.limit = query.limit();
      }  

      public LdbcQuery7 unpack() {
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

  public static class LdbcQuery7ResultSerializable implements Serializable {
      public final long personId;
      public final String personFirstName;
      public final String personLastName;
      public final long likeCreationDate;
      public final long commentOrPostId;
      public final String commentOrPostContent;
      public final int minutesLatency;
      public final boolean isNew;

      public LdbcQuery7ResultSerializable(LdbcQuery7Result query) {
          this.personId = query.personId();
          this.personFirstName = query.personFirstName();
          this.personLastName = query.personLastName();
          this.likeCreationDate = query.likeCreationDate();
          this.commentOrPostId = query.commentOrPostId();
          this.commentOrPostContent = query.commentOrPostContent();
          this.minutesLatency = query.minutesLatency();
          this.isNew = query.isNew();
      }

      public LdbcQuery7ResultSerializable(long personId, String personFirstName, String personLastName, long likeCreationDate, long commentOrPostId, String commentOrPostContent, int minutesLatency, boolean isNew) {
          this.personId = personId;
          this.personFirstName = personFirstName;
          this.personLastName = personLastName;
          this.likeCreationDate = likeCreationDate;
          this.commentOrPostId = commentOrPostId;
          this.commentOrPostContent = commentOrPostContent;
          this.minutesLatency = minutesLatency;
          this.isNew = isNew;
      }

      public LdbcQuery7Result unpack() {
          return new LdbcQuery7Result(personId,
                                      personFirstName,
                                      personLastName,
                                      likeCreationDate,
                                      commentOrPostId,
                                      commentOrPostContent,
                                      minutesLatency,
                                      isNew);
      }

      @Override
      public String toString() {
          return "LdbcQuery7ResultSerializable{" +
                  "personId=" + personId +
                  ", personFirstName='" + personFirstName + '\'' +
                  ", personLastName='" + personLastName + '\'' +
                  ", likeCreationDate=" + likeCreationDate +
                  ", commentOrPostId=" + commentOrPostId +
                  ", commentOrPostContent='" + commentOrPostContent + '\'' +
                  ", minutesLatency=" + minutesLatency +
                  ", isNew=" + isNew +
                  '}';
      }
  }

  public static class LdbcQuery8Serializable implements Serializable {
      public final long personId;
      public final int limit;

      public LdbcQuery8Serializable(LdbcQuery8 query) {
        this.personId = query.personId();
        this.limit = query.limit();
      }  

      public LdbcQuery8 unpack() {
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

  public static class LdbcQuery8ResultSerializable implements Serializable {
      public final long personId;
      public final String personFirstName;
      public final String personLastName;
      public final long commentCreationDate;
      public final long commentId;
      public final String commentContent;

      public LdbcQuery8ResultSerializable(LdbcQuery8Result query) {
          this.personId = query.personId();
          this.personFirstName = query.personFirstName();
          this.personLastName = query.personLastName();
          this.commentCreationDate = query.commentCreationDate();
          this.commentId = query.commentId();
          this.commentContent = query.commentContent();
      }

      public LdbcQuery8ResultSerializable(long personId, String personFirstName, String personLastName, long commentCreationDate, long commentId, String commentContent) {
          this.personId = personId;
          this.personFirstName = personFirstName;
          this.personLastName = personLastName;
          this.commentCreationDate = commentCreationDate;
          this.commentId = commentId;
          this.commentContent = commentContent;
      }

      public LdbcQuery8Result unpack() {
          return new LdbcQuery8Result(personId,
                                      personFirstName,
                                      personLastName,
                                      commentCreationDate,
                                      commentId,
                                      commentContent);
      }

      @Override
      public String toString() {
          return "LdbcQuery8ResultSerializable{" +
                  "personId=" + personId +
                  ", personFirstName='" + personFirstName + '\'' +
                  ", personLastName='" + personLastName + '\'' +
                  ", commentCreationDate=" + commentCreationDate +
                  ", commentId=" + commentId +
                  ", commentContent='" + commentContent + '\'' +
                  '}';
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

      public LdbcQuery9 unpack() {
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

  public static class LdbcQuery9ResultSerializable implements Serializable {
      public final long personId;
      public final String personFirstName;
      public final String personLastName;
      public final long commentOrPostId;
      public final String commentOrPostContent;
      public final long commentOrPostCreationDate;

      public LdbcQuery9ResultSerializable(LdbcQuery9Result query) {
          this.personId = query.personId();
          this.personFirstName = query.personFirstName();
          this.personLastName = query.personLastName();
          this.commentOrPostId = query.commentOrPostId();
          this.commentOrPostContent = query.commentOrPostContent();
          this.commentOrPostCreationDate = query.commentOrPostCreationDate();
      }

      public LdbcQuery9ResultSerializable(long personId, String personFirstName, String personLastName, long commentOrPostId, String commentOrPostContent, long commentOrPostCreationDate) {
          this.personId = personId;
          this.personFirstName = personFirstName;
          this.personLastName = personLastName;
          this.commentOrPostId = commentOrPostId;
          this.commentOrPostContent = commentOrPostContent;
          this.commentOrPostCreationDate = commentOrPostCreationDate;
      }

      public LdbcQuery9Result unpack() {
          return new LdbcQuery9Result(personId,
                                      personFirstName,
                                      personLastName,
                                      commentOrPostId,
                                      commentOrPostContent,
                                      commentOrPostCreationDate);
      }

      @Override
      public String toString() {
          return "LdbcQuery9ResultSerializable{" +
                  "personId=" + personId +
                  ", personFirstName='" + personFirstName + '\'' +
                  ", personLastName='" + personLastName + '\'' +
                  ", commentOrPostId=" + commentOrPostId +
                  ", commentOrPostContent='" + commentOrPostContent + '\'' +
                  ", commentOrPostCreationDate=" + commentOrPostCreationDate +
                  '}';
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

      public LdbcQuery10 unpack() {
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

  public static class LdbcQuery10ResultSerializable implements Serializable {
      public final long personId;
      public final String personFirstName;
      public final String personLastName;
      public final int commonInterestScore;
      public final String personGender;
      public final String personCityName;

      public LdbcQuery10ResultSerializable(LdbcQuery10Result query) {
          this.personId = query.personId();
          this.personFirstName = query.personFirstName();
          this.personLastName = query.personLastName();
          this.commonInterestScore = query.commonInterestScore();
          this.personGender = query.personGender();
          this.personCityName = query.personCityName();
      }

      public LdbcQuery10ResultSerializable(long personId, String personFirstName, String personLastName, int commonInterestScore, String personGender, String personCityName) {
          this.personId = personId;
          this.personFirstName = personFirstName;
          this.personLastName = personLastName;
          this.commonInterestScore = commonInterestScore;
          this.personGender = personGender;
          this.personCityName = personCityName;
      }

      public LdbcQuery10Result unpack() {
          return new LdbcQuery10Result(personId,
                                      personFirstName,
                                      personLastName,
                                      commonInterestScore,
                                      personGender,
                                      personCityName);
      }

      @Override
      public String toString() {
          return "LdbcQuery10ResultSerializable{" +
                  "personId=" + personId +
                  ", personFirstName='" + personFirstName + '\'' +
                  ", personLastName='" + personLastName + '\'' +
                  ", personGender='" + personGender + '\'' +
                  ", personCityName='" + personCityName + '\'' +
                  ", commonInterestScore=" + commonInterestScore +
                  '}';
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

      public LdbcQuery11 unpack() {
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

  public static class LdbcQuery11ResultSerializable implements Serializable {
      public final long personId;
      public final String personFirstName;
      public final String personLastName;
      public final String organizationName;
      public final int organizationWorkFromYear;

      public LdbcQuery11ResultSerializable(LdbcQuery11Result query) {
          this.personId = query.personId();
          this.personFirstName = query.personFirstName();
          this.personLastName = query.personLastName();
          this.organizationName = query.organizationName();
          this.organizationWorkFromYear = query.organizationWorkFromYear();
      }

      public LdbcQuery11ResultSerializable(long personId, String personFirstName, String personLastName, String organizationName, int organizationWorkFromYear) {
          this.personId = personId;
          this.personFirstName = personFirstName;
          this.personLastName = personLastName;
          this.organizationName = organizationName;
          this.organizationWorkFromYear = organizationWorkFromYear;
      }

      public LdbcQuery11Result unpack() {
          return new LdbcQuery11Result(personId,
                                      personFirstName,
                                      personLastName,
                                      organizationName,
                                      organizationWorkFromYear);
      }

      @Override
      public String toString() {
          return "LdbcQuery11ResultSerializable{" +
                  "personId=" + personId +
                  ", personFirstName='" + personFirstName + '\'' +
                  ", personLastName='" + personLastName + '\'' +
                  ", organizationName='" + organizationName + '\'' +
                  ", organizationWorkFromYear=" + organizationWorkFromYear +
                  '}';
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

      public LdbcQuery12 unpack() {
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

  public static class LdbcQuery12ResultSerializable implements Serializable {
      public final long personId;
      public final String personFirstName;
      public final String personLastName;
      public final Iterable<String> tagNames;
      public final int replyCount;

      public LdbcQuery12ResultSerializable(LdbcQuery12Result query) {
          this.personId = query.personId();
          this.personFirstName = query.personFirstName();
          this.personLastName = query.personLastName();
          this.tagNames = query.tagNames();
          this.replyCount = query.replyCount();
      }

      public LdbcQuery12ResultSerializable(long personId, String personFirstName, String personLastName, Iterable<String> tagNames, int replyCount) {
          this.personId = personId;
          this.personFirstName = personFirstName;
          this.personLastName = personLastName;
          this.tagNames = tagNames;
          this.replyCount = replyCount;
      }

      public LdbcQuery12Result unpack() {
          return new LdbcQuery12Result(personId,
                                      personFirstName,
                                      personLastName,
                                      tagNames,
                                      replyCount);
      }

      @Override
      public String toString() {
          return "LdbcQuery12ResultSerializable{" +
                  "personId=" + personId +
                  ", personFirstName='" + personFirstName + '\'' +
                  ", personLastName='" + personLastName + '\'' +
                  ", tagNames=" + tagNames +
                  ", replyCount=" + replyCount +
                  '}';
      }
  }

  public static class LdbcQuery13Serializable implements Serializable {
      public final long person1Id;
      public final long person2Id;

      public LdbcQuery13Serializable(LdbcQuery13 query) {
        this.person1Id = query.person1Id();
        this.person2Id = query.person2Id();
      }  

      public LdbcQuery13 unpack() {
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

  public static class LdbcQuery13ResultSerializable implements Serializable {
      public final int shortestPathLength;

      public LdbcQuery13ResultSerializable(LdbcQuery13Result query) {
          this.shortestPathLength = query.shortestPathLength();
      }

      public LdbcQuery13ResultSerializable(int shortestPathLength) {
          this.shortestPathLength = shortestPathLength;
      }

      public LdbcQuery13Result unpack() {
          return new LdbcQuery13Result(shortestPathLength);
      }

      @Override
      public String toString() {
          return "LdbcQuery13ResultSerializable{" +
                  "shortestPathLength=" + shortestPathLength +
                  '}';
      }
  }

  public static class LdbcQuery14Serializable implements Serializable {
      public final long person1Id;
      public final long person2Id;

      public LdbcQuery14Serializable(LdbcQuery14 query) {
        this.person1Id = query.person1Id();
        this.person2Id = query.person2Id();
      }  

      public LdbcQuery14 unpack() {
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

  public static class LdbcQuery14ResultSerializable implements Serializable {
      public final Iterable<? extends Number> personIdsInPath;
      public final double pathWeight;

      public LdbcQuery14ResultSerializable(LdbcQuery14Result query) {
          this.personIdsInPath = query.personsIdsInPath();
          this.pathWeight = query.pathWeight();
      }

      public LdbcQuery14ResultSerializable( Iterable<? extends Number> personIdsInPath, double pathWeight )
      {
          this.personIdsInPath = personIdsInPath;
          this.pathWeight = pathWeight;
      }

      public LdbcQuery14Result unpack() {
          return new LdbcQuery14Result(personIdsInPath,
                                      pathWeight);
      }

      @Override
      public String toString()
      {
          return "LdbcQuery14ResultSerializable{" +
                "personIdsInPath=" + personIdsInPath +
                ", pathWeight=" + pathWeight +
                '}';
      }
  }

  public static class LdbcShortQuery1PersonProfileSerializable implements Serializable
  {
      public final long personId;

      public LdbcShortQuery1PersonProfileSerializable(LdbcShortQuery1PersonProfile query) {
          this.personId = query.personId();
      }

      public LdbcShortQuery1PersonProfileSerializable( long personId )
      {
          this.personId = personId;
      }

      public LdbcShortQuery1PersonProfile unpack() {
          return new LdbcShortQuery1PersonProfile( personId);
      }

      @Override
      public String toString()
      {
          return "LdbcShortQuery1PersonProfile{" +
                "personId=" + personId +
                '}';
      }
  }

  public static class LdbcShortQuery1PersonProfileResultSerializable implements Serializable 
  {
      public final String firstName;
      public final String lastName;
      public final long birthday;
      public final String locationIp;
      public final String browserUsed;
      public final long cityId;
      public final String gender;
      public final long creationDate;

      public LdbcShortQuery1PersonProfileResultSerializable(LdbcShortQuery1PersonProfileResult query) {
          this.firstName = query.firstName();
          this.lastName = query.lastName();
          this.birthday = query.birthday();
          this.locationIp = query.locationIp();
          this.browserUsed = query.browserUsed();
          this.cityId = query.cityId();
          this.gender = query.gender();
          this.creationDate = query.creationDate();
      }

      public LdbcShortQuery1PersonProfileResultSerializable(String firstName,
                                                String lastName,
                                                long birthday,
                                                String locationIp,
                                                String browserUsed,
                                                long cityId,
                                                String gender,
                                                long creationDate) {
          this.firstName = firstName;
          this.lastName = lastName;
          this.birthday = birthday;
          this.locationIp = locationIp;
          this.browserUsed = browserUsed;
          this.cityId = cityId;
          this.gender = gender;
          this.creationDate = creationDate;
      }

      public LdbcShortQuery1PersonProfileResult unpack() {
          return new LdbcShortQuery1PersonProfileResult( firstName,
                                                  lastName,
                                                  birthday,
                                                  locationIp,
                                                  browserUsed,
                                                  cityId,
                                                  gender,
                                                  creationDate);
      }

      @Override
      public String toString() {
          return "LdbcShortQuery1PersonProfileResult{" +
                  "firstName='" + firstName + '\'' +
                  ", lastName='" + lastName + '\'' +
                  ", birthday=" + birthday +
                  ", locationIp='" + locationIp + '\'' +
                  ", browserUsed='" + browserUsed + '\'' +
                  ", cityId=" + cityId +
                  ", gender='" + gender + '\'' +
                  ", creationDate=" + creationDate +
                  '}';
      }
  }

  public static class LdbcShortQuery2PersonPostsSerializable implements Serializable
  {
      public final long personId;
      public final int limit;

      public LdbcShortQuery2PersonPostsSerializable(LdbcShortQuery2PersonPosts query) {
          this.personId = query.personId();
          this.limit = query.limit();
      }

      public LdbcShortQuery2PersonPostsSerializable( long personId, int limit )
      {
          this.personId = personId;
          this.limit = limit;
      }

      public LdbcShortQuery2PersonPosts unpack() {
          return new LdbcShortQuery2PersonPosts( personId,
                                                  limit);
      }

      @Override
      public String toString()
      {
          return "LdbcShortQuery2PersonPosts{" +
                "personId=" + personId +
                ", limit=" + limit +
                '}';
      }
  }

  public static class LdbcShortQuery2PersonPostsResultSerializable implements Serializable 
  {
      public final long messageId;
      public final String messageContent;
      public final long messageCreationDate;
      public final long originalPostId;
      public final long originalPostAuthorId;
      public final String originalPostAuthorFirstName;
      public final String originalPostAuthorLastName;

      public LdbcShortQuery2PersonPostsResultSerializable(LdbcShortQuery2PersonPostsResult query) {
          this.messageId = query.messageId();
          this.messageContent = query.messageContent();
          this.messageCreationDate = query.messageCreationDate();
          this.originalPostId = query.originalPostId();
          this.originalPostAuthorId = query.originalPostAuthorId();
          this.originalPostAuthorFirstName = query.originalPostAuthorFirstName();
          this.originalPostAuthorLastName = query.originalPostAuthorLastName();
      }

      public LdbcShortQuery2PersonPostsResultSerializable(long messageId, String messageContent, long messageCreationDate, long originalPostId, long originalPostAuthorId, String originalPostAuthorFirstName, String originalPostAuthorLastName) {
          this.messageId = messageId;
          this.messageContent = messageContent;
          this.messageCreationDate = messageCreationDate;
          this.originalPostId = originalPostId;
          this.originalPostAuthorId = originalPostAuthorId;
          this.originalPostAuthorFirstName = originalPostAuthorFirstName;
          this.originalPostAuthorLastName = originalPostAuthorLastName;
      }

      public LdbcShortQuery2PersonPostsResult unpack() {
          return new LdbcShortQuery2PersonPostsResult( messageId,
                                                  messageContent,
                                                  messageCreationDate,
                                                  originalPostId,
                                                  originalPostAuthorId,
                                                  originalPostAuthorFirstName,
                                                  originalPostAuthorLastName);
      }

      @Override
      public String toString() {
          return "LdbcShortQuery2PersonPostsResult{" +
                  "messageId=" + messageId +
                  ", messageContent='" + messageContent + '\'' +
                  ", messageCreationDate=" + messageCreationDate +
                  ", originalPostId=" + originalPostId +
                  ", originalPostAuthorId=" + originalPostAuthorId +
                  ", originalPostAuthorFirstName='" + originalPostAuthorFirstName + '\'' +
                  ", originalPostAuthorLastName='" + originalPostAuthorLastName + '\'' +
                  '}';
      }
  }

  public static class LdbcShortQuery3PersonFriendsSerializable implements Serializable
  {
      public final long personId;

      public LdbcShortQuery3PersonFriendsSerializable(LdbcShortQuery3PersonFriends query) {
          this.personId = query.personId();
      }

      public LdbcShortQuery3PersonFriendsSerializable( long personId )
      {
          this.personId = personId;
      }

      public LdbcShortQuery3PersonFriends unpack() {
          return new LdbcShortQuery3PersonFriends( personId);
      }

      @Override
      public String toString()
      {
          return "LdbcShortQuery3PersonFriends{" +
                "personId=" + personId +
                '}';
      }
  }

  public static class LdbcShortQuery3PersonFriendsResultSerializable implements Serializable 
  {
      public final long personId;
      public final String firstName;
      public final String lastName;
      public final long friendshipCreationDate;

      public LdbcShortQuery3PersonFriendsResultSerializable(LdbcShortQuery3PersonFriendsResult query) {
          this.personId = query.personId();
          this.firstName = query.firstName();
          this.lastName = query.lastName();
          this.friendshipCreationDate = query.friendshipCreationDate();
      }

      public LdbcShortQuery3PersonFriendsResultSerializable(long personId, String firstName, String lastName, long friendshipCreationDate) {
          this.personId = personId;
          this.firstName = firstName;
          this.lastName = lastName;
          this.friendshipCreationDate = friendshipCreationDate;
      }

      public LdbcShortQuery3PersonFriendsResult unpack() {
          return new LdbcShortQuery3PersonFriendsResult( personId,
                                                  firstName,
                                                  lastName,
                                                  friendshipCreationDate);
      }

      @Override
      public String toString() {
          return "LdbcShortQuery3PersonFriendsResult{" +
                  "personId=" + personId +
                  ", firstName='" + firstName + '\'' +
                  ", lastName='" + lastName + '\'' +
                  ", friendshipCreationDate=" + friendshipCreationDate +
                  '}';
      }
  }




  public static class LdbcShortQuery4MessageContentSerializable implements Serializable
  {
      public final long messageId;

      public LdbcShortQuery4MessageContentSerializable(LdbcShortQuery4MessageContent query) {
          this.messageId = query.messageId();
      }

      public LdbcShortQuery4MessageContentSerializable( long messageId )
      {
          this.messageId = messageId;
      }

      public LdbcShortQuery4MessageContent unpack() {
          return new LdbcShortQuery4MessageContent( messageId);
      }

      @Override
      public String toString()
      {
          return "LdbcShortQuery4MessageContent{" +
                "messageId=" + messageId +
                '}';
      }
  }

  public static class LdbcShortQuery4MessageContentResultSerializable implements Serializable 
  {
      public final String messageContent;
      public final long messageCreationDate;

      public LdbcShortQuery4MessageContentResultSerializable(LdbcShortQuery4MessageContentResult query) {
          this.messageContent = query.messageContent();
          this.messageCreationDate = query.messageCreationDate();
      }

      public LdbcShortQuery4MessageContentResultSerializable(String messageContent, long messageCreationDate) {
          this.messageContent = messageContent;
          this.messageCreationDate = messageCreationDate;
      }

      public LdbcShortQuery4MessageContentResult unpack() {
          return new LdbcShortQuery4MessageContentResult( messageContent,
                                                  messageCreationDate);
      }

      @Override
      public String toString() {
          return "LdbcShortQuery4MessageContentResult{" +
                  "messageContent='" + messageContent + '\'' +
                  ", messageCreationDate=" + messageCreationDate +
                  '}';
      }
  }




  public static class LdbcShortQuery5MessageCreatorSerializable implements Serializable
  {
      public final long messageId;

      public LdbcShortQuery5MessageCreatorSerializable(LdbcShortQuery5MessageCreator query) {
          this.messageId = query.messageId();
      }

      public LdbcShortQuery5MessageCreatorSerializable( long messageId )
      {
          this.messageId = messageId;
      }

      public LdbcShortQuery5MessageCreator unpack() {
          return new LdbcShortQuery5MessageCreator( messageId);
      }

      @Override
      public String toString()
      {
          return "LdbcShortQuery5MessageCreator{" +
                "messageId=" + messageId +
                '}';
      }
  }

  public static class LdbcShortQuery5MessageCreatorResultSerializable implements Serializable 
  {
      public final long personId;
      public final String firstName;
      public final String lastName;

      public LdbcShortQuery5MessageCreatorResultSerializable(LdbcShortQuery5MessageCreatorResult query) {
          this.personId = query.personId();
          this.firstName = query.firstName();
          this.lastName = query.lastName();
      }

      public LdbcShortQuery5MessageCreatorResultSerializable(long personId, String firstName, String lastName) {
          this.personId = personId;
          this.firstName = firstName;
          this.lastName = lastName;
      }

      public LdbcShortQuery5MessageCreatorResult unpack() {
          return new LdbcShortQuery5MessageCreatorResult( personId,
                                                  firstName,
                                                  lastName);
      }

      @Override
      public String toString() {
          return "LdbcShortQuery5MessageCreatorResult{" +
                  "personId=" + personId +
                  ", firstName='" + firstName + '\'' +
                  ", lastName='" + lastName + '\'' +
                  '}';
      }
  }




  public static class LdbcShortQuery6MessageForumSerializable implements Serializable
  {
      public final long messageId;

      public LdbcShortQuery6MessageForumSerializable(LdbcShortQuery6MessageForum query) {
          this.messageId = query.messageId();
      }

      public LdbcShortQuery6MessageForumSerializable( long messageId )
      {
          this.messageId = messageId;
      }

      public LdbcShortQuery6MessageForum unpack() {
          return new LdbcShortQuery6MessageForum( messageId);
      }

      @Override
      public String toString()
      {
          return "LdbcShortQuery6MessageForum{" +
                "messageId=" + messageId +
                '}';
      }
  }

  public static class LdbcShortQuery6MessageForumResultSerializable implements Serializable 
  {
      public final long forumId;
      public final String forumTitle;
      public final long moderatorId;
      public final String moderatorFirstName;
      public final String moderatorLastName;

      public LdbcShortQuery6MessageForumResultSerializable(LdbcShortQuery6MessageForumResult query) {
          this.forumId = query.forumId();
          this.forumTitle = query.forumTitle();
          this.moderatorId = query.moderatorId();
          this.moderatorFirstName = query.moderatorFirstName();
          this.moderatorLastName = query.moderatorLastName();
      }

      public LdbcShortQuery6MessageForumResultSerializable(long forumId, String forumTitle, long moderatorId, String moderatorFirstName, String moderatorLastName) {
          this.forumId = forumId;
          this.forumTitle = forumTitle;
          this.moderatorId = moderatorId;
          this.moderatorFirstName = moderatorFirstName;
          this.moderatorLastName = moderatorLastName;
      }

      public LdbcShortQuery6MessageForumResult unpack() {
          return new LdbcShortQuery6MessageForumResult( forumId,
                                                  forumTitle,
                                                  moderatorId,
                                                  moderatorFirstName,
                                                  moderatorLastName);
      }

      @Override
      public String toString() {
          return "LdbcShortQuery6MessageForumResult{" +
                  "forumId=" + forumId +
                  ", forumTitle='" + forumTitle + '\'' +
                  ", moderatorId=" + moderatorId +
                  ", moderatorFirstName='" + moderatorFirstName + '\'' +
                  ", moderatorLastName='" + moderatorLastName + '\'' +
                  '}';
      }
  }

  public static class LdbcShortQuery7MessageRepliesSerializable implements Serializable
  {
      public final long messageId;

      public LdbcShortQuery7MessageRepliesSerializable(LdbcShortQuery7MessageReplies query) {
          this.messageId = query.messageId();
      }

      public LdbcShortQuery7MessageRepliesSerializable( long messageId )
      {
          this.messageId = messageId;
      }

      public LdbcShortQuery7MessageReplies unpack() {
          return new LdbcShortQuery7MessageReplies( messageId);
      }

      @Override
      public String toString()
      {
          return "LdbcShortQuery7MessageReplies{" +
                "messageId=" + messageId +
                '}';
      }
  }

  public static class LdbcShortQuery7MessageRepliesResultSerializable implements Serializable 
  {
      public final long commentId;
      public final String commentContent;
      public final long commentCreationDate;
      public final long replyAuthorId;
      public final String replyAuthorFirstName;
      public final String replyAuthorLastName;
      public final boolean replyAuthorKnowsOriginalMessageAuthor;

      public LdbcShortQuery7MessageRepliesResultSerializable(LdbcShortQuery7MessageRepliesResult query) {
          this.commentId = query.commentId();
          this.commentContent = query.commentContent();
          this.commentCreationDate = query.commentCreationDate();
          this.replyAuthorId = query.replyAuthorId();
          this.replyAuthorFirstName = query.replyAuthorFirstName();
          this.replyAuthorLastName = query.replyAuthorLastName();
          this.replyAuthorKnowsOriginalMessageAuthor = query.isReplyAuthorKnowsOriginalMessageAuthor();
      }

      public LdbcShortQuery7MessageRepliesResultSerializable(long commentId, String commentContent, long commentCreationDate, long replyAuthorId, String replyAuthorFirstName, String replyAuthorLastName, boolean replyAuthorKnowsOriginalMessageAuthor) {
          this.commentId = commentId;
          this.commentContent = commentContent;
          this.commentCreationDate = commentCreationDate;
          this.replyAuthorId = replyAuthorId;
          this.replyAuthorFirstName = replyAuthorFirstName;
          this.replyAuthorLastName = replyAuthorLastName;
          this.replyAuthorKnowsOriginalMessageAuthor = replyAuthorKnowsOriginalMessageAuthor;
      }

      public LdbcShortQuery7MessageRepliesResult unpack() {
          return new LdbcShortQuery7MessageRepliesResult( commentId,
                                                  commentContent,
                                                  commentCreationDate,
                                                  replyAuthorId,
                                                  replyAuthorFirstName,
                                                  replyAuthorLastName,
                                                  replyAuthorKnowsOriginalMessageAuthor);
      }

      @Override
      public String toString() {
          return "LdbcShortQuery7MessageRepliesResult{" +
                  "commentId=" + commentId +
                  ", commentContent='" + commentContent + '\'' +
                  ", commentCreationDate=" + commentCreationDate +
                  ", replyAuthorId=" + replyAuthorId +
                  ", replyAuthorFirstName='" + replyAuthorFirstName + '\'' +
                  ", replyAuthorLastName='" + replyAuthorLastName + '\'' +
                  ", replyAuthorKnowsOriginalMessageAuthor=" + replyAuthorKnowsOriginalMessageAuthor +
                  '}';
      }
  }

  public static class LdbcNoResultSerializable implements Serializable {
      public static final LdbcNoResultSerializable INSTANCE = 
        new LdbcNoResultSerializable();

      public LdbcNoResultSerializable() {
      }
  }

  public static class LdbcUpdate1AddPersonSerializable implements Serializable
  {
      public final long personId;
      public final String personFirstName;
      public final String personLastName;
      public final String gender;
      public final Date birthday; // input format "1984-03-22"
      public final Date creationDate; // input format "2004-03-22"
      public final String locationIp;
      public final String browserUsed;
      public final long cityId;
      public final List<String> languages;
      public final List<String> emails;
      public final List<Long> tagIds;
      public final List<OrganizationSerializable> studyAt = new ArrayList<>();
      public final List<OrganizationSerializable> workAt = new ArrayList<>();

      public LdbcUpdate1AddPersonSerializable(LdbcUpdate1AddPerson query) {
          this.personId = query.personId();
          this.personFirstName = query.personFirstName();
          this.personLastName = query.personLastName();
          this.gender = query.gender();
          this.birthday = query.birthday();
          this.creationDate = query.creationDate();
          this.locationIp = query.locationIp();
          this.browserUsed = query.browserUsed();
          this.cityId = query.cityId();
          this.languages = query.languages();
          this.emails = query.emails();
          this.tagIds = query.tagIds();
          query.studyAt().forEach((v) -> {
              this.studyAt.add(new OrganizationSerializable(v));
            });
          query.workAt().forEach((v) -> {
              this.workAt.add(new OrganizationSerializable(v));
            });
      }

      public LdbcUpdate1AddPersonSerializable( long personId,
              String personFirstName,
              String personLastName,
              String gender,
              Date birthday,
              Date creationDate,
              String locationIp,
              String browserUsed,
              long cityId,
              List<String> languages,
              List<String> emails,
              List<Long> tagIds,
              List<Organization> studyAt,
              List<Organization> workAt )
      {
          this.personId = personId;
          this.personFirstName = personFirstName;
          this.personLastName = personLastName;
          this.gender = gender;
          this.birthday = birthday;
          this.creationDate = creationDate;
          this.locationIp = locationIp;
          this.browserUsed = browserUsed;
          this.cityId = cityId;
          this.languages = languages;
          this.emails = emails;
          this.tagIds = tagIds;
          studyAt.forEach((v) -> {
              this.studyAt.add(new OrganizationSerializable(v));
            });
          workAt.forEach((v) -> {
              this.workAt.add(new OrganizationSerializable(v));
            });
      }

      public LdbcUpdate1AddPerson unpack() {
          List<Organization> studyAtUnpacked = new ArrayList<>();
          studyAt.forEach((v) -> {
              studyAtUnpacked.add(v.unpack());
            });
          List<Organization> workAtUnpacked = new ArrayList<>();
          workAt.forEach((v) -> {
              workAtUnpacked.add(v.unpack());
            });
          return new LdbcUpdate1AddPerson( personId,
                                                  personFirstName,
                                                  personLastName,
                                                  gender,
                                                  birthday,
                                                  creationDate,
                                                  locationIp,
                                                  browserUsed,
                                                  cityId,
                                                  languages,
                                                  emails,
                                                  tagIds,
                                                  studyAtUnpacked,
                                                  workAtUnpacked);
      }

      public static class OrganizationSerializable implements Serializable
      {
          public final long organizationId;
          public final int year;

          public OrganizationSerializable(Organization org) {
              this.organizationId = org.organizationId();
              this.year = org.year();
          }

          public OrganizationSerializable( long organizationId, int year )
          {
              this.organizationId = organizationId;
              this.year = year;
          }

          public Organization unpack() {
              return new Organization(organizationId, year);
          }

          @Override
          public String toString()
          {
              return "Organization{" +
                    "organizationId=" + organizationId +
                    ", year=" + year +
                    '}';
          }
      }

      @Override
      public String toString()
      {
          return "LdbcUpdate1AddPerson{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", gender='" + gender + '\'' +
                ", birthday=" + birthday +
                ", creationDate=" + creationDate +
                ", locationIp='" + locationIp + '\'' +
                ", browserUsed='" + browserUsed + '\'' +
                ", cityId=" + cityId +
                ", languages=" + languages +
                ", emails=" + emails +
                ", tagIds=" + tagIds +
                ", studyAt=" + studyAt +
                ", workAt=" + workAt +
                '}';
      }
  }

  public static class LdbcUpdate2AddPostLikeSerializable implements Serializable
  {
      public final long personId;
      public final long postId;
      public final Date creationDate;

      public LdbcUpdate2AddPostLikeSerializable(LdbcUpdate2AddPostLike query) {
          this.personId = query.personId();
          this.postId = query.postId();
          this.creationDate = query.creationDate();
      }

      public LdbcUpdate2AddPostLikeSerializable( long personId, long postId, Date creationDate )
      {
          this.personId = personId;
          this.postId = postId;
          this.creationDate = creationDate;
      }

      public LdbcUpdate2AddPostLike unpack() {
          return new LdbcUpdate2AddPostLike( personId,
                                                  postId,
                                                  creationDate);
      }

      @Override
      public String toString()
      {
          return "LdbcUpdate2AddPostLike{" +
                "personId=" + personId +
                ", postId=" + postId +
                ", creationDate=" + creationDate +
                '}';
      }
  }

  public static class LdbcUpdate3AddCommentLikeSerializable implements Serializable
  {
      public final long personId;
      public final long commentId;
      public final Date creationDate;

      public LdbcUpdate3AddCommentLikeSerializable(LdbcUpdate3AddCommentLike query) {
          this.personId = query.personId();
          this.commentId = query.commentId();
          this.creationDate = query.creationDate();
      }

      public LdbcUpdate3AddCommentLikeSerializable( long personId, long commentId, Date creationDate )
      {
          this.personId = personId;
          this.commentId = commentId;
          this.creationDate = creationDate;
      }

      public LdbcUpdate3AddCommentLike unpack() {
          return new LdbcUpdate3AddCommentLike( personId,
                                                  commentId,
                                                  creationDate);
      }

      @Override
      public String toString()
      {
          return "LdbcUpdate3AddCommentLike{" +
                "personId=" + personId +
                ", commentId=" + commentId +
                ", creationDate=" + creationDate +
                '}';
      }
  }

  public static class LdbcUpdate4AddForumSerializable implements Serializable
  {
      public final long forumId;
      public final String forumTitle;
      public final Date creationDate;
      public final long moderatorPersonId;
      public final List<Long> tagIds;

      public LdbcUpdate4AddForumSerializable(LdbcUpdate4AddForum query) {
          this.forumId = query.forumId();
          this.forumTitle = query.forumTitle();
          this.creationDate = query.creationDate();
          this.moderatorPersonId = query.moderatorPersonId();
          this.tagIds = query.tagIds();
      }

      public LdbcUpdate4AddForumSerializable( long forumId, String forumTitle, Date creationDate, long moderatorPersonId,
              List<Long> tagIds )
      {
          this.forumId = forumId;
          this.forumTitle = forumTitle;
          this.creationDate = creationDate;
          this.moderatorPersonId = moderatorPersonId;
          this.tagIds = tagIds;
      }

      public LdbcUpdate4AddForum unpack() {
          return new LdbcUpdate4AddForum( forumId,
                                                  forumTitle,
                                                  creationDate,
                                                  moderatorPersonId,
                                                  tagIds);
      }

      @Override
      public String toString()
      {
          return "LdbcUpdate4AddForum{" +
                "forumId=" + forumId +
                ", forumTitle='" + forumTitle + '\'' +
                ", creationDate=" + creationDate +
                ", moderatorPersonId=" + moderatorPersonId +
                ", tagIds=" + tagIds +
                '}';
      }
  }

  public static class LdbcUpdate5AddForumMembershipSerializable implements Serializable
  {
      public final long forumId;
      public final long personId;
      public final Date joinDate;

      public LdbcUpdate5AddForumMembershipSerializable(LdbcUpdate5AddForumMembership query) {
          this.forumId = query.forumId();
          this.personId = query.personId();
          this.joinDate = query.joinDate();
      }

      public LdbcUpdate5AddForumMembershipSerializable( long forumId, long personId, Date joinDate )
      {
          this.forumId = forumId;
          this.personId = personId;
          this.joinDate = joinDate;
      }

      public LdbcUpdate5AddForumMembership unpack() {
          return new LdbcUpdate5AddForumMembership( forumId,
                                                  personId,
                                                  joinDate);
      }

      @Override
      public String toString()
      {
          return "LdbcUpdate5AddForumMembership{" +
                "forumId=" + forumId +
                ", personId=" + personId +
                ", joinDate=" + joinDate +
                '}';
      }
  }

  public static class LdbcUpdate6AddPostSerializable implements Serializable
  {
      public final long postId;
      public final String imageFile;
      public final Date creationDate;
      public final String locationIp;
      public final String browserUsed;
      public final String language;
      public final String content;
      public final int length;
      public final long authorPersonId;
      public final long forumId;
      public final long countryId;
      public final List<Long> tagIds;

      public LdbcUpdate6AddPostSerializable(LdbcUpdate6AddPost query) {
          this.postId = query.postId();
          this.imageFile = query.imageFile();
          this.creationDate = query.creationDate();
          this.locationIp = query.locationIp();
          this.browserUsed = query.browserUsed();
          this.language = query.language();
          this.content = query.content();
          this.length = query.length();
          this.authorPersonId = query.authorPersonId();
          this.forumId = query.forumId();
          this.countryId = query.countryId();
          this.tagIds = query.tagIds();
      }

      public LdbcUpdate6AddPostSerializable( long postId,
              String imageFile,
              Date creationDate,
              String locationIp,
              String browserUsed,
              String language,
              String content,
              int length,
              long authorPersonId,
              long forumId,
              long countryId,
              List<Long> tagIds )
      {
          this.postId = postId;
          this.imageFile = imageFile;
          this.creationDate = creationDate;
          this.locationIp = locationIp;
          this.browserUsed = browserUsed;
          this.language = language;
          this.content = content;
          this.length = length;
          this.authorPersonId = authorPersonId;
          this.forumId = forumId;
          this.countryId = countryId;
          this.tagIds = tagIds;
      }

      public LdbcUpdate6AddPost unpack() {
          return new LdbcUpdate6AddPost( postId,
                                                  imageFile,
                                                  creationDate,
                                                  locationIp,
                                                  browserUsed,
                                                  language,
                                                  content,
                                                  length,
                                                  authorPersonId,
                                                  forumId,
                                                  countryId,
                                                  tagIds);
      }

      @Override
      public String toString()
      {
          return "LdbcUpdate6AddPost{" +
                "postId=" + postId +
                ", imageFile='" + imageFile + '\'' +
                ", creationDate=" + creationDate +
                ", locationIp='" + locationIp + '\'' +
                ", browserUsed='" + browserUsed + '\'' +
                ", language='" + language + '\'' +
                ", content='" + content + '\'' +
                ", length=" + length +
                ", authorPersonId=" + authorPersonId +
                ", forumId=" + forumId +
                ", countryId=" + countryId +
                ", tagIds=" + tagIds +
                '}';
      }
  }

  public static class LdbcUpdate7AddCommentSerializable implements Serializable
  {
      public final long commentId;
      public final Date creationDate;
      public final String locationIp;
      public final String browserUsed;
      public final String content;
      public final int length;
      public final long authorPersonId;
      public final long countryId;
      public final long replyToPostId;
      public final long replyToCommentId;
      public final List<Long> tagIds;

      public LdbcUpdate7AddCommentSerializable(LdbcUpdate7AddComment query) {
          this.commentId = query.commentId();
          this.creationDate = query.creationDate();
          this.locationIp = query.locationIp();
          this.browserUsed = query.browserUsed();
          this.content = query.content();
          this.length = query.length();
          this.authorPersonId = query.authorPersonId();
          this.countryId = query.countryId();
          this.replyToPostId = query.replyToPostId();
          this.replyToCommentId = query.replyToCommentId();
          this.tagIds = query.tagIds();
      }

      public LdbcUpdate7AddCommentSerializable( long commentId,
              Date creationDate,
              String locationIp,
              String browserUsed,
              String content,
              int length,
              long authorPersonId,
              long countryId,
              long replyToPostId,
              long replyToCommentId,
              List<Long> tagIds )
      {
          this.commentId = commentId;
          this.creationDate = creationDate;
          this.locationIp = locationIp;
          this.browserUsed = browserUsed;
          this.content = content;
          this.length = length;
          this.authorPersonId = authorPersonId;
          this.countryId = countryId;
          this.replyToPostId = replyToPostId;
          this.replyToCommentId = replyToCommentId;
          this.tagIds = tagIds;
      }

      public LdbcUpdate7AddComment unpack() {
          return new LdbcUpdate7AddComment( commentId,
                                                  creationDate,
                                                  locationIp,
                                                  browserUsed,
                                                  content,
                                                  length,
                                                  authorPersonId,
                                                  countryId,
                                                  replyToPostId,
                                                  replyToCommentId,
                                                  tagIds);
      }

      @Override
      public String toString()
      {
          return "LdbcUpdate7AddComment{" +
                "commentId=" + commentId +
                ", creationDate=" + creationDate +
                ", locationIp='" + locationIp + '\'' +
                ", browserUsed='" + browserUsed + '\'' +
                ", content='" + content + '\'' +
                ", length=" + length +
                ", authorPersonId=" + authorPersonId +
                ", countryId=" + countryId +
                ", replyToPostId=" + replyToPostId +
                ", replyToCommentId=" + replyToCommentId +
                ", tagIds=" + tagIds +
                '}';
      }
  }

  public static class LdbcUpdate8AddFriendshipSerializable implements Serializable
  {
      public final long person1Id;
      public final long person2Id;
      public final Date creationDate;

      public LdbcUpdate8AddFriendshipSerializable(LdbcUpdate8AddFriendship query) {
          this.person1Id = query.person1Id();
          this.person2Id = query.person2Id();
          this.creationDate = query.creationDate();
      }

      public LdbcUpdate8AddFriendshipSerializable( long person1Id, long person2Id, Date creationDate )
      {
          this.person1Id = person1Id;
          this.person2Id = person2Id;
          this.creationDate = creationDate;
      }

      public LdbcUpdate8AddFriendship unpack() {
          return new LdbcUpdate8AddFriendship( person1Id,
                                                  person2Id,
                                                  creationDate);
      }

      @Override
      public String toString()
      {
          return "LdbcUpdate8AddFriendship{" +
                "person1Id=" + person1Id +
                ", person2Id=" + person2Id +
                ", creationDate=" + creationDate +
                '}';
      }
  }
}
