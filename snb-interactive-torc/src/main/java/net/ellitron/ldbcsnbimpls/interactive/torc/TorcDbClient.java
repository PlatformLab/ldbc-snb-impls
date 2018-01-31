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

import net.ellitron.ldbcsnbimpls.interactive.torc.TorcDbServer.*;
import net.ellitron.ldbcsnbimpls.interactive.torc.LdbcSerializableQueriesAndResults.*;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.Operation;
import com.ldbc.driver.ResultReporter;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * An LDBC SNB driver database implementation for TorcDB that spreads query load
 * across multiple TorcDbServers. 
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcDbClient extends Db {

  // Holds the state of our connections to TorcDbServers
  private TorcDbClientConnectionState connectionState = null;

  @Override
  protected void onInit(Map<String, String> properties,
      LoggingService loggingService) throws DbException {

    connectionState = new TorcDbClientConnectionState(properties);

    /*
     * Register operation handlers with the benchmark.
     */
    registerOperationHandler(LdbcQuery1.class,
        LdbcQuery1Handler.class);
    registerOperationHandler(LdbcQuery2.class,
        LdbcQuery2Handler.class);
    registerOperationHandler(LdbcQuery7.class,
        LdbcQuery7Handler.class);
    registerOperationHandler(LdbcQuery8.class,
        LdbcQuery8Handler.class);
    registerOperationHandler(LdbcQuery10.class,
        LdbcQuery10Handler.class);
    registerOperationHandler(LdbcQuery11.class,
        LdbcQuery11Handler.class);
    registerOperationHandler(LdbcQuery13.class,
        LdbcQuery13Handler.class);

    registerOperationHandler(LdbcShortQuery1PersonProfile.class,
        LdbcShortQuery1PersonProfileHandler.class);
    registerOperationHandler(LdbcShortQuery2PersonPosts.class,
        LdbcShortQuery2PersonPostsHandler.class);
    registerOperationHandler(LdbcShortQuery3PersonFriends.class,
        LdbcShortQuery3PersonFriendsHandler.class);
    registerOperationHandler(LdbcShortQuery4MessageContent.class,
        LdbcShortQuery4MessageContentHandler.class);
    registerOperationHandler(LdbcShortQuery5MessageCreator.class,
        LdbcShortQuery5MessageCreatorHandler.class);
    registerOperationHandler(LdbcShortQuery6MessageForum.class,
        LdbcShortQuery6MessageForumHandler.class);
    registerOperationHandler(LdbcShortQuery7MessageReplies.class,
        LdbcShortQuery7MessageRepliesHandler.class);

    registerOperationHandler(LdbcUpdate1AddPerson.class,
        LdbcUpdate1AddPersonHandler.class);
    registerOperationHandler(LdbcUpdate2AddPostLike.class,
        LdbcUpdate2AddPostLikeHandler.class);
    registerOperationHandler(LdbcUpdate3AddCommentLike.class,
        LdbcUpdate3AddCommentLikeHandler.class);
    registerOperationHandler(LdbcUpdate4AddForum.class,
        LdbcUpdate4AddForumHandler.class);
    registerOperationHandler(LdbcUpdate5AddForumMembership.class,
        LdbcUpdate5AddForumMembershipHandler.class);
    registerOperationHandler(LdbcUpdate6AddPost.class,
        LdbcUpdate6AddPostHandler.class);
    registerOperationHandler(LdbcUpdate7AddComment.class,
        LdbcUpdate7AddCommentHandler.class);
    registerOperationHandler(LdbcUpdate8AddFriendship.class,
        LdbcUpdate8AddFriendshipHandler.class);
  }

  @Override
  protected void onClose() throws IOException {
    connectionState.close();
  }

  @Override
  protected DbConnectionState getConnectionState() throws DbException {
    return connectionState;
  }

  public static void executeQuery(Operation operation, 
      TorcDbClientConnectionState connState, ResultReporter resultReporter) 
      throws DbException {
    try {
      List<ObjectOutputStream> oStreams = connState.getObjectOutputStreams();
      List<ObjectInputStream> iStreams = connState.getObjectInputStreams();

      // Pick server uniformly at random.
      int n = (int) (Math.random() * oStreams.size());
      ObjectOutputStream out = oStreams.get(n);
      ObjectInputStream in = iStreams.get(n);

      // Package operation into serializable form and send to server.
      if (operation instanceof LdbcQuery1) {
        out.writeObject(new LdbcQuery1Serializable((LdbcQuery1) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery1ResultSerializable> resp = 
          (List<LdbcQuery1ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery1Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery2) {
        out.writeObject(new LdbcQuery2Serializable((LdbcQuery2) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery2ResultSerializable> resp = 
          (List<LdbcQuery2ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery2Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery3) {
        out.writeObject(new LdbcQuery3Serializable((LdbcQuery3) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery3ResultSerializable> resp = 
          (List<LdbcQuery3ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery3Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery4) {
        out.writeObject(new LdbcQuery4Serializable((LdbcQuery4) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery4ResultSerializable> resp = 
          (List<LdbcQuery4ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery4Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery5) {
        out.writeObject(new LdbcQuery5Serializable((LdbcQuery5) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery5ResultSerializable> resp = 
          (List<LdbcQuery5ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery5Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery6) {
        out.writeObject(new LdbcQuery6Serializable((LdbcQuery6) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery6ResultSerializable> resp = 
          (List<LdbcQuery6ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery6Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery7) {
        out.writeObject(new LdbcQuery7Serializable((LdbcQuery7) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery7ResultSerializable> resp = 
          (List<LdbcQuery7ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery7Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery8) {
        out.writeObject(new LdbcQuery8Serializable((LdbcQuery8) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery8ResultSerializable> resp = 
          (List<LdbcQuery8ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery8Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery9) {
        out.writeObject(new LdbcQuery9Serializable((LdbcQuery9) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery9ResultSerializable> resp = 
          (List<LdbcQuery9ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery9Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery10) {
        out.writeObject(new LdbcQuery10Serializable((LdbcQuery10) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery10ResultSerializable> resp = 
          (List<LdbcQuery10ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery10Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery11) {
        out.writeObject(new LdbcQuery11Serializable((LdbcQuery11) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery11ResultSerializable> resp = 
          (List<LdbcQuery11ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery11Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery12) {
        out.writeObject(new LdbcQuery12Serializable((LdbcQuery12) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery12ResultSerializable> resp = 
          (List<LdbcQuery12ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery12Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery13) {
        out.writeObject(new LdbcQuery13Serializable((LdbcQuery13) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery13ResultSerializable> resp = 
          (List<LdbcQuery13ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery13Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcQuery14) {
        out.writeObject(new LdbcQuery14Serializable((LdbcQuery14) operation));
        out.flush();

        // Receive the response.
        List<LdbcQuery14ResultSerializable> resp = 
          (List<LdbcQuery14ResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcQuery14Result> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcShortQuery1PersonProfile) {
        out.writeObject(new LdbcShortQuery1PersonProfileSerializable((LdbcShortQuery1PersonProfile) operation));
        out.flush();

        // Receive the response.
        LdbcShortQuery1PersonProfileResultSerializable resp = 
          (LdbcShortQuery1PersonProfileResultSerializable) in.readObject();

        // Convert the response to type expected by driver.  
        LdbcShortQuery1PersonProfileResult result = resp.unpack();

        resultReporter.report(1, result, operation);
      } else if (operation instanceof LdbcShortQuery2PersonPosts) {
        out.writeObject(new LdbcShortQuery2PersonPostsSerializable((LdbcShortQuery2PersonPosts) operation));
        out.flush();

        // Receive the response.
        List<LdbcShortQuery2PersonPostsResultSerializable> resp = 
          (List<LdbcShortQuery2PersonPostsResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcShortQuery3PersonFriends) {
        out.writeObject(new LdbcShortQuery3PersonFriendsSerializable((LdbcShortQuery3PersonFriends) operation));
        out.flush();

        // Receive the response.
        List<LdbcShortQuery3PersonFriendsResultSerializable> resp = 
          (List<LdbcShortQuery3PersonFriendsResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcShortQuery4MessageContent) {
        out.writeObject(new LdbcShortQuery4MessageContentSerializable((LdbcShortQuery4MessageContent) operation));
        out.flush();

        // Receive the response.
        LdbcShortQuery4MessageContentResultSerializable resp = 
          (LdbcShortQuery4MessageContentResultSerializable) in.readObject();

        // Convert the response to type expected by driver.  
        LdbcShortQuery4MessageContentResult result = resp.unpack();

        resultReporter.report(1, result, operation);
      } else if (operation instanceof LdbcShortQuery5MessageCreator) {
        out.writeObject(new LdbcShortQuery5MessageCreatorSerializable((LdbcShortQuery5MessageCreator) operation));
        out.flush();

        // Receive the response.
        LdbcShortQuery5MessageCreatorResultSerializable resp = 
          (LdbcShortQuery5MessageCreatorResultSerializable) in.readObject();

        // Convert the response to type expected by driver.  
        LdbcShortQuery5MessageCreatorResult result = resp.unpack();

        resultReporter.report(1, result, operation);
      } else if (operation instanceof LdbcShortQuery6MessageForum) {
        out.writeObject(new LdbcShortQuery6MessageForumSerializable((LdbcShortQuery6MessageForum) operation));
        out.flush();

        // Receive the response.
        LdbcShortQuery6MessageForumResultSerializable resp = 
          (LdbcShortQuery6MessageForumResultSerializable) in.readObject();

        // Convert the response to type expected by driver.  
        LdbcShortQuery6MessageForumResult result = resp.unpack();

        resultReporter.report(1, result, operation);
      } else if (operation instanceof LdbcShortQuery7MessageReplies) {
        out.writeObject(new LdbcShortQuery7MessageRepliesSerializable((LdbcShortQuery7MessageReplies) operation));
        out.flush();

        // Receive the response.
        List<LdbcShortQuery7MessageRepliesResultSerializable> resp = 
          (List<LdbcShortQuery7MessageRepliesResultSerializable>) in.readObject();

        // Convert the response to type expected by driver.  
        List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();
        resp.forEach((v) -> {
          result.add(v.unpack());
        });

        resultReporter.report(result.size(), result, operation);
      } else if (operation instanceof LdbcUpdate1AddPerson) {
        out.writeObject(new LdbcUpdate1AddPersonSerializable((LdbcUpdate1AddPerson) operation));
        out.flush();

        // Receive the response.
        LdbcNoResultSerializable resp = (LdbcNoResultSerializable) in.readObject();

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
      } else if (operation instanceof LdbcUpdate2AddPostLike) {
        out.writeObject(new LdbcUpdate2AddPostLikeSerializable((LdbcUpdate2AddPostLike) operation));
        out.flush();

        // Receive the response.
        LdbcNoResultSerializable resp = (LdbcNoResultSerializable) in.readObject();

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
      } else if (operation instanceof LdbcUpdate3AddCommentLike) {
        out.writeObject(new LdbcUpdate3AddCommentLikeSerializable((LdbcUpdate3AddCommentLike) operation));
        out.flush();

        // Receive the response.
        LdbcNoResultSerializable resp = (LdbcNoResultSerializable) in.readObject();

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
      } else if (operation instanceof LdbcUpdate4AddForum) {
        out.writeObject(new LdbcUpdate4AddForumSerializable((LdbcUpdate4AddForum) operation));
        out.flush();

        // Receive the response.
        LdbcNoResultSerializable resp = (LdbcNoResultSerializable) in.readObject();

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
      } else if (operation instanceof LdbcUpdate5AddForumMembership) {
        out.writeObject(new LdbcUpdate5AddForumMembershipSerializable((LdbcUpdate5AddForumMembership) operation));
        out.flush();

        // Receive the response.
        LdbcNoResultSerializable resp = (LdbcNoResultSerializable) in.readObject();

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
      } else if (operation instanceof LdbcUpdate6AddPost) {
        out.writeObject(new LdbcUpdate6AddPostSerializable((LdbcUpdate6AddPost) operation));
        out.flush();

        // Receive the response.
        LdbcNoResultSerializable resp = (LdbcNoResultSerializable) in.readObject();

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
      } else if (operation instanceof LdbcUpdate7AddComment) {
        out.writeObject(new LdbcUpdate7AddCommentSerializable((LdbcUpdate7AddComment) operation));
        out.flush();

        // Receive the response.
        LdbcNoResultSerializable resp = (LdbcNoResultSerializable) in.readObject();

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
      } else if (operation instanceof LdbcUpdate8AddFriendship) {
        out.writeObject(new LdbcUpdate8AddFriendshipSerializable((LdbcUpdate8AddFriendship) operation));
        out.flush();

        // Receive the response.
        LdbcNoResultSerializable resp = (LdbcNoResultSerializable) in.readObject();

        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
      } else {
        throw new RuntimeException("Unrecognized query");
      }

    } catch (Exception e) {
        throw new RuntimeException(e);
    }
  }

  /**
   * ------------------------------------------------------------------------
   * Complex Queries
   * ------------------------------------------------------------------------
   */
  /**
   * Given a start Person, find up to 20 Persons with a given first name that
   * the start Person is connected to (excluding start Person) by at most 3
   * steps via Knows relationships. Return Persons, including summaries of the
   * Persons workplaces and places of study. Sort results ascending by their
   * distance from the start Person, for Persons within the same distance sort
   * ascending by their last name, and for Persons with same last name
   * ascending by their identifier.[1]
   */
  public static class LdbcQuery1Handler
      implements OperationHandler<LdbcQuery1, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery1Handler.class);

    @Override
    public void executeOperation(final LdbcQuery1 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find (most recent) Posts and Comments from all of
   * that Person’s friends, that were created before (and including) a given
   * date. Return the top 20 Posts/Comments, and the Person that created each
   * of them. Sort results descending by creation date, and then ascending by
   * Post identifier.[1]
   */
  public static class LdbcQuery2Handler
      implements OperationHandler<LdbcQuery2, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery2Handler.class);

    @Override
    public void executeOperation(final LdbcQuery2 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find Persons that are their friends and friends of
   * friends (excluding start Person) that have made Posts/Comments in both of
   * the given Countries, X and Y, within a given period. Only Persons that are
   * foreign to Countries X and Y are considered, that is Persons whose
   * Location is not Country X or Country Y. Return top 20 Persons, and their
   * Post/Comment counts, in the given countries and period. Sort results
   * descending by total number of Posts/Comments, and then ascending by Person
   * identifier.[1]
   */
  public static class LdbcQuery3Handler
      implements OperationHandler<LdbcQuery3, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery3Handler.class);

    @Override
    public void executeOperation(final LdbcQuery3 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find Tags that are attached to Posts that were
   * created by that Person’s friends. Only include Tags that were attached to
   * friends’ Posts created within a given time interval, and that were never
   * attached to friends’ Posts created before this interval. Return top 10
   * Tags, and the count of Posts, which were created within the given time
   * interval, that this Tag was attached to. Sort results descending by Post
   * count, and then ascending by Tag name.[1]
   */
  public static class LdbcQuery4Handler
      implements OperationHandler<LdbcQuery4, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery4Handler.class);

    @Override
    public void executeOperation(final LdbcQuery4 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find the Forums which that Person’s friends and
   * friends of friends (excluding start Person) became Members of after a
   * given date. Return top 20 Forums, and the number of Posts in each Forum
   * that was Created by any of these Persons. For each Forum consider only
   * those Persons which joined that particular Forum after the given date.
   * Sort results descending by the count of Posts, and then ascending by Forum
   * identifier.[1]
   */
  public static class LdbcQuery5Handler
      implements OperationHandler<LdbcQuery5, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery5Handler.class);

    @Override
    public void executeOperation(final LdbcQuery5 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person and some Tag, find the other Tags that occur together
   * with this Tag on Posts that were created by start Person’s friends and
   * friends of friends (excluding start Person). Return top 10 Tags, and the
   * count of Posts that were created by these Persons, which contain both this
   * Tag and the given Tag. Sort results descending by count, and then
   * ascending by Tag name.[1]
   */
  public static class LdbcQuery6Handler
      implements OperationHandler<LdbcQuery6, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery6Handler.class);

    @Override
    public void executeOperation(final LdbcQuery6 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find (most recent) Likes on any of start Person’s
   * Posts/Comments. Return top 20 Persons that Liked any of start Person’s
   * Posts/Comments, the Post/Comment they liked most recently, creation date
   * of that Like, and the latency (in minutes) between creation of
   * Post/Comment and Like. Additionally, return a flag indicating whether the
   * liker is a friend of start Person. In the case that a Person Liked
   * multiple Posts/Comments at the same time, return the Post/Comment with
   * lowest identifier. Sort results descending by creation time of Like, then
   * ascending by Person identifier of liker.[1]
   */
  public static class LdbcQuery7Handler
      implements OperationHandler<LdbcQuery7, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery7Handler.class);

    @Override
    public void executeOperation(final LdbcQuery7 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find (most recent) Comments that are replies to
   * Posts/Comments of the start Person. Only consider immediate (1-hop)
   * replies, not the transitive (multi-hop) case. Return the top 20 reply
   * Comments, and the Person that created each reply Comment. Sort results
   * descending by creation date of reply Comment, and then ascending by
   * identifier of reply Comment.[1]
   */
  public static class LdbcQuery8Handler
      implements OperationHandler<LdbcQuery8, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery8Handler.class);

    @Override
    public void executeOperation(final LdbcQuery8 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find the (most recent) Posts/Comments created by
   * that Person’s friends or friends of friends (excluding start Person). Only
   * consider the Posts/Comments created before a given date (excluding that
   * date). Return the top 20 Posts/Comments, and the Person that created each
   * of those Posts/Comments. Sort results descending by creation date of
   * Post/Comment, and then ascending by Post/Comment identifier.[1]
   */
  public static class LdbcQuery9Handler
      implements OperationHandler<LdbcQuery9, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery9Handler.class);

    @Override
    public void executeOperation(final LdbcQuery9 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find that Person’s friends of friends (excluding
   * start Person, and immediate friends), who were born on or after the 21st
   * of a given month (in any year) and before the 22nd of the following month.
   * Calculate the similarity between each of these Persons and start Person,
   * where similarity for any Person is defined as follows:
   * <ul>
   * <li>common = number of Posts created by that Person, such that the Post
   * has a Tag that start Person is Interested in</li>
   * <li>uncommon = number of Posts created by that Person, such that the Post
   * has no Tag that start Person is Interested in</li>
   * <li>similarity = common - uncommon</li>
   * </ul>
   * Return top 10 Persons, their Place, and their similarity score. Sort
   * results descending by similarity score, and then ascending by Person
   * identifier.[1]
   */
  public static class LdbcQuery10Handler
      implements OperationHandler<LdbcQuery10, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery10Handler.class);

    @Override
    public void executeOperation(final LdbcQuery10 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find that Person’s friends and friends of friends
   * (excluding start Person) who started Working in some Company in a given
   * Country, before a given date (year). Return top 10 Persons, the Company
   * they worked at, and the year they started working at that Company. Sort
   * results ascending by the start date, then ascending by Person identifier,
   * and lastly by Organization name descending.[1]
   */
  public static class LdbcQuery11Handler
      implements OperationHandler<LdbcQuery11, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery11Handler.class);

    @Override
    public void executeOperation(final LdbcQuery11 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, find the Comments that this Person’s friends made in
   * reply to Posts, considering only those Comments that are immediate (1-hop)
   * replies to Posts, not the transitive (multi-hop) case. Only consider Posts
   * with a Tag in a given TagClass or in a descendent of that TagClass. Count
   * the number of these reply Comments, and collect the Tags (with valid tag
   * class) that were attached to the Posts they replied to. Return top 20
   * Persons with at least one reply, the reply count, and the collection of
   * Tags. Sort results descending by Comment count, and then ascending by
   * Person identifier.[1]
   */
  public static class LdbcQuery12Handler
      implements OperationHandler<LdbcQuery12, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery12Handler.class);

    @Override
    public void executeOperation(final LdbcQuery12 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given two Persons, find the shortest path between these two Persons in the
   * subgraph induced by the Knows relationships. Return the length of this
   * path. -1 should be returned if no path is found, and 0 should be returned
   * if the start person is the same as the end person.[1]
   */
  public static class LdbcQuery13Handler
      implements OperationHandler<LdbcQuery13, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery13Handler.class);

    @Override
    public void executeOperation(final LdbcQuery13 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given two Persons, find all (unweighted) shortest paths between these two
   * Persons, in the subgraph induced by the Knows relationship. Then, for each
   * path calculate a weight. The nodes in the path are Persons, and the weight
   * of a path is the sum of weights between every pair of consecutive Person
   * nodes in the path. The weight for a pair of Persons is calculated such
   * that every reply (by one of the Persons) to a Post (by the other Person)
   * contributes 1.0, and every reply (by ones of the Persons) to a Comment (by
   * the other Person) contributes 0.5. Return all the paths with shortest
   * length, and their weights. Sort results descending by path weight. The
   * order of paths with the same weight is unspecified.[1]
   */
  public static class LdbcQuery14Handler
      implements OperationHandler<LdbcQuery14, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery14Handler.class);

    @Override
    public void executeOperation(final LdbcQuery14 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * ------------------------------------------------------------------------
   * Short Queries
   * ------------------------------------------------------------------------
   */
  /**
   * Given a start Person, retrieve their first name, last name, birthday, IP
   * address, browser, and city of residence.[1]
   */
  public static class LdbcShortQuery1PersonProfileHandler implements
      OperationHandler<LdbcShortQuery1PersonProfile, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery1PersonProfileHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery1PersonProfile operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, retrieve the last 10 Messages (Posts or Comments)
   * created by that user. For each message, return that message, the original
   * post in its conversation, and the author of that post. If any of the
   * Messages is a Post, then the original Post will be the same Message, i.e.,
   * that Message will appear twice in that result. Order results descending by
   * message creation date, then descending by message identifier.[1]
   */
  public static class LdbcShortQuery2PersonPostsHandler implements
      OperationHandler<LdbcShortQuery2PersonPosts, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery2PersonPostsHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery2PersonPosts operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a start Person, retrieve all of their friends, and the date at which
   * they became friends. Order results descending by friendship creation date,
   * then ascending by friend identifier.[1]
   */
  public static class LdbcShortQuery3PersonFriendsHandler implements
      OperationHandler<LdbcShortQuery3PersonFriends, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery3PersonFriendsHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery3PersonFriends operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve its content and creation
   * date.[1]
   */
  public static class LdbcShortQuery4MessageContentHandler implements
      OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery4MessageContentHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery4MessageContent operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve its author.[1]
   */
  public static class LdbcShortQuery5MessageCreatorHandler implements
      OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery5MessageCreatorHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery5MessageCreator operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve the Forum that contains it and
   * the Person that moderates that forum. Since comments are not directly
   * contained in forums, for comments, return the forum containing the
   * original post in the thread which the comment is replying to.[1]
   */
  public static class LdbcShortQuery6MessageForumHandler implements
      OperationHandler<LdbcShortQuery6MessageForum, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery6MessageForumHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery6MessageForum operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve the (1-hop) Comments that
   * reply to it. In addition, return a boolean flag indicating if the author
   * of the reply knows the author of the original message. If author is same
   * as original author, return false for "knows" flag. Order results
   * descending by creation date, then ascending by author identifier.[1]
   */
  public static class LdbcShortQuery7MessageRepliesHandler implements
      OperationHandler<LdbcShortQuery7MessageReplies, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery7MessageRepliesHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery7MessageReplies operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * ------------------------------------------------------------------------
   * Update Queries
   * ------------------------------------------------------------------------
   */
  /**
   * Add a Person to the social network. [1]
   */
  public static class LdbcUpdate1AddPersonHandler implements
      OperationHandler<LdbcUpdate1AddPerson, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcUpdate1AddPersonHandler.class);

    @Override
    public void executeOperation(LdbcUpdate1AddPerson operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Add a Like to a Post of the social network.[1]
   */
  public static class LdbcUpdate2AddPostLikeHandler implements
      OperationHandler<LdbcUpdate2AddPostLike, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcUpdate2AddPostLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate2AddPostLike operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Add a Like to a Comment of the social network.[1]
   */
  public static class LdbcUpdate3AddCommentLikeHandler implements
      OperationHandler<LdbcUpdate3AddCommentLike, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcUpdate3AddCommentLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate3AddCommentLike operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Add a Forum to the social network.[1]
   */
  public static class LdbcUpdate4AddForumHandler implements
      OperationHandler<LdbcUpdate4AddForum, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate4AddForum operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Add a Forum membership to the social network.[1]
   */
  public static class LdbcUpdate5AddForumMembershipHandler implements
      OperationHandler<LdbcUpdate5AddForumMembership, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate5AddForumMembership operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Add a Post to the social network.[1]
   */
  public static class LdbcUpdate6AddPostHandler implements
      OperationHandler<LdbcUpdate6AddPost, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate6AddPost operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Add a Comment replying to a Post/Comment to the social network.[1]
   */
  public static class LdbcUpdate7AddCommentHandler implements
      OperationHandler<LdbcUpdate7AddComment, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate7AddComment operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }

  /**
   * Add a friendship relation to the social network.[1]
   */
  public static class LdbcUpdate8AddFriendshipHandler implements
      OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {

    final static Logger logger =
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate8AddFriendship operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      TorcDbClient.executeQuery(operation, 
          (TorcDbClientConnectionState) dbConnectionState, 
          resultReporter);
    }
  }
}
