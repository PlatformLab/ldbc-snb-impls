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
package net.ellitron.ldbcsnbimpls.interactive.neo4j;

import net.ellitron.ldbcsnbimpls.snb.Entity;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
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

import org.apache.commons.configuration.BaseConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.Map;

/**
 * An implementation of the LDBC SNB interactive workload for Neo4j. Queries
 * are executed against a running Neo4j server. Configuration parameters for
 * this implementation (that are supplied via the LDBC driver) are listed
 * below.
 *
 * Configuration Parameters:
 *
 * @param host IP address of the Neo4j web server (default: 127.0.0.1).
 * @param port port of the Neo4j web server (default: 7473).
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class Neo4jDb extends Db {

  private Neo4jDbConnectionState connectionState = null;

  @Override
  protected DbConnectionState getConnectionState() throws DbException {
    return connectionState;
  }

  @Override
  protected void onClose() throws IOException {
    connectionState.close();
  }

  @Override
  protected void onInit(Map<String, String> properties,
      LoggingService loggingService) throws DbException {

    connectionState = new Neo4jDbConnectionState(properties);
  }

  /**
   * ------------------------------------------------------------------------
   * Complex Queries
   * ------------------------------------------------------------------------
   */
  public static class LdbcQuery1Handler
      implements OperationHandler<LdbcQuery1, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery1Handler.class);

    @Override
    public void executeOperation(LdbcQuery1 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery2Handler
      implements OperationHandler<LdbcQuery2, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery2Handler.class);

    @Override
    public void executeOperation(LdbcQuery2 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery3Handler
      implements OperationHandler<LdbcQuery3, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery3Handler.class);

    @Override
    public void executeOperation(LdbcQuery3 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery4Handler
      implements OperationHandler<LdbcQuery4, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery4Handler.class);

    @Override
    public void executeOperation(LdbcQuery4 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery5Handler
      implements OperationHandler<LdbcQuery5, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery5Handler.class);

    @Override
    public void executeOperation(LdbcQuery5 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery6Handler
      implements OperationHandler<LdbcQuery6, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery6Handler.class);

    @Override
    public void executeOperation(LdbcQuery6 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery7Handler
      implements OperationHandler<LdbcQuery7, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery7Handler.class);

    @Override
    public void executeOperation(LdbcQuery7 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery8Handler
      implements OperationHandler<LdbcQuery8, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery8Handler.class);

    @Override
    public void executeOperation(LdbcQuery8 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery9Handler
      implements OperationHandler<LdbcQuery9, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery9Handler.class);

    @Override
    public void executeOperation(LdbcQuery9 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery10Handler
      implements OperationHandler<LdbcQuery10, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery10Handler.class);

    @Override
    public void executeOperation(LdbcQuery10 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery11Handler
      implements OperationHandler<LdbcQuery11, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery11Handler.class);

    @Override
    public void executeOperation(LdbcQuery11 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery12Handler
      implements OperationHandler<LdbcQuery12, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery12Handler.class);

    @Override
    public void executeOperation(LdbcQuery12 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery13Handler
      implements OperationHandler<LdbcQuery13, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery13Handler.class);

    @Override
    public void executeOperation(LdbcQuery13 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcQuery14Handler
      implements OperationHandler<LdbcQuery14, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery14Handler.class);

    @Override
    public void executeOperation(LdbcQuery14 operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }
  
  /**
   * ------------------------------------------------------------------------
   * Short Queries
   * ------------------------------------------------------------------------
   */
  public static class LdbcShortQuery1PersonProfileHandler implements 
      OperationHandler<LdbcShortQuery1PersonProfile, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery1PersonProfileHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery1PersonProfile operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcShortQuery2PersonPostsHandler implements
      OperationHandler<LdbcShortQuery2PersonPosts, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery2PersonPostsHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery2PersonPosts operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcShortQuery3PersonFriendsHandler implements
      OperationHandler<LdbcShortQuery3PersonFriends, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery3PersonFriendsHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery3PersonFriends operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcShortQuery4MessageContentHandler implements
      OperationHandler<LdbcShortQuery4MessageContent, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery4MessageContentHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery4MessageContent operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcShortQuery5MessageCreatorHandler implements
      OperationHandler<LdbcShortQuery5MessageCreator, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery5MessageCreatorHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery5MessageCreator operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcShortQuery6MessageForumHandler implements
      OperationHandler<LdbcShortQuery6MessageForum, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery6MessageForumHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery6MessageForum operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  public static class LdbcShortQuery7MessageRepliesHandler implements
      OperationHandler<LdbcShortQuery7MessageReplies, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery7MessageRepliesHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery7MessageReplies operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

    }
  }

  /**
   * ------------------------------------------------------------------------
   * Update Queries
   * ------------------------------------------------------------------------
   */
  public static class LdbcUpdate1AddPersonHandler implements
      OperationHandler<LdbcUpdate1AddPerson, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate1AddPersonHandler.class);
    private final Calendar calendar;

    public LdbcUpdate1AddPersonHandler() {
      this.calendar = new GregorianCalendar();
    }

    @Override
    public void executeOperation(LdbcUpdate1AddPerson operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

    }
  }

  public static class LdbcUpdate2AddPostLikeHandler implements
      OperationHandler<LdbcUpdate2AddPostLike, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate2AddPostLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate2AddPostLike operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

    }

  }

  public static class LdbcUpdate3AddCommentLikeHandler implements
      OperationHandler<LdbcUpdate3AddCommentLike, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate3AddCommentLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate3AddCommentLike operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

    }
  }

  public static class LdbcUpdate4AddForumHandler implements
      OperationHandler<LdbcUpdate4AddForum, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate4AddForumHandler.class);

    @Override
    public void executeOperation(LdbcUpdate4AddForum operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

    }
  }

  public static class LdbcUpdate5AddForumMembershipHandler implements
      OperationHandler<LdbcUpdate5AddForumMembership, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate5AddForumMembershipHandler.class);

    @Override
    public void executeOperation(LdbcUpdate5AddForumMembership operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

    }
  }

  public static class LdbcUpdate6AddPostHandler implements
      OperationHandler<LdbcUpdate6AddPost, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate6AddPostHandler.class);

    @Override
    public void executeOperation(LdbcUpdate6AddPost operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

    }
  }

  public static class LdbcUpdate7AddCommentHandler implements
      OperationHandler<LdbcUpdate7AddComment, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate7AddCommentHandler.class);

    @Override
    public void executeOperation(LdbcUpdate7AddComment operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

    }
  }

  public static class LdbcUpdate8AddFriendshipHandler implements
      OperationHandler<LdbcUpdate8AddFriendship, Neo4jDbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate8AddFriendshipHandler.class);

    @Override
    public void executeOperation(LdbcUpdate8AddFriendship operation,
        Neo4jDbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

    }
  }

  private static class Neo4jDbConnectionState extends DbConnectionState {

    private Neo4jDbConnectionState(Map<String, String> properties) {
    }

    @Override
    public void close() throws IOException {

    }
  }
}
