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

public class LdbcQueryResultsSerializable {

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

        public LdbcQuery1Result getResult() {
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

    public static class LdbcQuery2ResultSerializable implements Serializable {
        public final long personId;
        public final String personFirstName;
        public final String personLastName;
        public final long messageId;
        public final String messageContent;
        public final long messageCreationDate;

        public LdbcQuery2ResultSerializable(LdbcQuery2Result query) {
            this.personId = query.personId();
            this.personFirstName = query.personFirstName();
            this.personLastName = query.personLastName();
            this.messageId = query.messageId();
            this.messageContent = query.messageContent();
            this.messageCreationDate = query.messageCreationDate();
        }

        public LdbcQuery2ResultSerializable(long personId, String personFirstName, String personLastName, long messageId, String messageContent, long messageCreationDate) {
            this.personId = personId;
            this.personFirstName = personFirstName;
            this.personLastName = personLastName;
            this.messageId = messageId;
            this.messageContent = messageContent;
            this.messageCreationDate = messageCreationDate;
        }

        public LdbcQuery2Result getResult() {
            return new LdbcQuery2Result(personId,
                                        personFirstName,
                                        personLastName,
                                        messageId,
                                        messageContent,
                                        messageCreationDate);
        }

        @Override
        public String toString() {
            return "LdbcQuery2ResultSerializable{" +
                    "personId=" + personId +
                    ", personFirstName='" + personFirstName + '\'' +
                    ", personLastName='" + personLastName + '\'' +
                    ", messageId=" + messageId +
                    ", messageContent='" + messageContent + '\'' +
                    ", messageCreationDate=" + messageCreationDate +
                    '}';
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

        public LdbcQuery3Result getResult() {
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

        public LdbcQuery4Result getResult() {
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

        public LdbcQuery5Result getResult() {
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

        public LdbcQuery6Result getResult() {
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

    public static class LdbcQuery7ResultSerializable implements Serializable {
        public final long personId;
        public final String personFirstName;
        public final String personLastName;
        public final long likeCreationDate;
        public final long messageId;
        public final String messageContent;
        public final int minutesLatency;
        public final boolean isNew;

        public LdbcQuery7ResultSerializable(LdbcQuery7Result query) {
            this.personId = query.personId();
            this.personFirstName = query.personFirstName();
            this.personLastName = query.personLastName();
            this.likeCreationDate = query.likeCreationDate();
            this.messageId = query.messageId();
            this.messageContent = query.messageContent();
            this.minutesLatency = query.minutesLatency();
            this.isNew = query.isNew();
        }

        public LdbcQuery7ResultSerializable(long personId, String personFirstName, String personLastName, long likeCreationDate, long messageId, String messageContent, int minutesLatency, boolean isNew) {
            this.personId = personId;
            this.personFirstName = personFirstName;
            this.personLastName = personLastName;
            this.likeCreationDate = likeCreationDate;
            this.messageId = messageId;
            this.messageContent = messageContent;
            this.minutesLatency = minutesLatency;
            this.isNew = isNew;
        }

        public LdbcQuery7Result getResult() {
            return new LdbcQuery7Result(personId,
                                        personFirstName,
                                        personLastName,
                                        likeCreationDate,
                                        messageId,
                                        messageContent,
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
                    ", messageId=" + messageId +
                    ", messageContent='" + messageContent + '\'' +
                    ", minutesLatency=" + minutesLatency +
                    ", isNew=" + isNew +
                    '}';
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

        public LdbcQuery8Result getResult() {
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

    public static class LdbcQuery9ResultSerializable implements Serializable {
        public final long personId;
        public final String personFirstName;
        public final String personLastName;
        public final long messageId;
        public final String messageContent;
        public final long messageCreationDate;

        public LdbcQuery9ResultSerializable(LdbcQuery9Result query) {
            this.personId = query.personId();
            this.personFirstName = query.personFirstName();
            this.personLastName = query.personLastName();
            this.messageId = query.messageId();
            this.messageContent = query.messageContent();
            this.messageCreationDate = query.messageCreationDate();
        }

        public LdbcQuery9ResultSerializable(long personId, String personFirstName, String personLastName, long messageId, String messageContent, long messageCreationDate) {
            this.personId = personId;
            this.personFirstName = personFirstName;
            this.personLastName = personLastName;
            this.messageId = messageId;
            this.messageContent = messageContent;
            this.messageCreationDate = messageCreationDate;
        }

        public LdbcQuery9Result getResult() {
            return new LdbcQuery9Result(personId,
                                        personFirstName,
                                        personLastName,
                                        messageId,
                                        messageContent,
                                        messageCreationDate);
        }

        @Override
        public String toString() {
            return "LdbcQuery9ResultSerializable{" +
                    "personId=" + personId +
                    ", personFirstName='" + personFirstName + '\'' +
                    ", personLastName='" + personLastName + '\'' +
                    ", messageId=" + messageId +
                    ", messageContent='" + messageContent + '\'' +
                    ", messageCreationDate=" + messageCreationDate +
                    '}';
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

        public LdbcQuery10Result getResult() {
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

        public LdbcQuery11Result getResult() {
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

        public LdbcQuery12Result getResult() {
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

    public static class LdbcQuery13ResultSerializable implements Serializable {
        public final int shortestPathLength;

        public LdbcQuery13ResultSerializable(LdbcQuery13Result query) {
            this.shortestPathLength = query.shortestPathLength();
        }

        public LdbcQuery13ResultSerializable(int shortestPathLength) {
            this.shortestPathLength = shortestPathLength;
        }

        public LdbcQuery13Result getResult() {
            return new LdbcQuery13Result(shortestPathLength);
        }

        @Override
        public String toString() {
            return "LdbcQuery13ResultSerializable{" +
                    "shortestPathLength=" + shortestPathLength +
                    '}';
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

        public LdbcQuery14Result getResult() {
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

}
