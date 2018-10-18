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

import static org.neo4j.driver.v1.Values.parameters;

import net.ellitron.ldbcsnbimpls.interactive.neo4j.util.DbHelper;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson.Organization;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import javax.json.JsonArray;
import javax.json.JsonString;
import javax.json.JsonValue;

/**
 * An implementation of the LDBC SNB interactive workload[1] for Neo4j. Queries
 * are executed against a running Neo4j server. Configuration parameters for
 * this implementation (that are supplied via the LDBC driver) are listed
 * below.
 * <p>
 * Configuration Parameters:
 * <ul>
 * <li>host - IP address of the Neo4j web server (default: 127.0.0.1).</li>
 * <li>port - port of the Neo4j web server (default: 7474).</li>
 * </ul>
 * <p>
 * References:<br>
 * [1]: Prat, Arnau (UPC) and Boncz, Peter (VUA) and Larriba, Josep Lluís (UPC)
 * and Angles, Renzo (TALCA) and Averbuch, Alex (NEO) and Erling, Orri (OGL)
 * and Gubichev, Andrey (TUM) and Spasić, Mirko (OGL) and Pham, Minh-Duc (VUA)
 * and Martínez, Norbert (SPARSITY). "LDBC Social Network Benchmark (SNB) -
 * v0.2.2 First Public Draft Release". http://www.ldbcouncil.org/.
 * <p>
 * TODO:<br>
 * <ul>
 * </ul>
 * <p>
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class Neo4jDb extends Db {

  private DbConnectionState connectionState = null;

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

    /*
     * Register operation handlers with the benchmark.
     */
    registerOperationHandler(LdbcQuery1.class,
        LdbcQuery1Handler.class);
    registerOperationHandler(LdbcQuery2.class,
        LdbcQuery2Handler.class);
    registerOperationHandler(LdbcQuery3.class,
        LdbcQuery3Handler.class);
    registerOperationHandler(LdbcQuery4.class,
        LdbcQuery4Handler.class);
    registerOperationHandler(LdbcQuery5.class,
        LdbcQuery5Handler.class);
    registerOperationHandler(LdbcQuery6.class,
        LdbcQuery6Handler.class);
    registerOperationHandler(LdbcQuery7.class,
        LdbcQuery7Handler.class);
    registerOperationHandler(LdbcQuery8.class,
        LdbcQuery8Handler.class);
    registerOperationHandler(LdbcQuery9.class,
        LdbcQuery9Handler.class);
    registerOperationHandler(LdbcQuery10.class,
        LdbcQuery10Handler.class);
    registerOperationHandler(LdbcQuery11.class,
        LdbcQuery11Handler.class);
    registerOperationHandler(LdbcQuery12.class,
        LdbcQuery12Handler.class);
    registerOperationHandler(LdbcQuery13.class,
        LdbcQuery13Handler.class);
    registerOperationHandler(LdbcQuery14.class,
        LdbcQuery14Handler.class);

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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery1Handler.class);

    @Override
    public void executeOperation(LdbcQuery1 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (:Person {id:{1}})-[path:KNOWS*1..3]-(friend:Person)"
          + " WHERE friend.firstName = {2}"
          + " WITH friend, min(length(path)) AS distance"
          + " ORDER BY distance ASC, friend.lastName ASC, toInt(friend.id) ASC"
          + " LIMIT {3}"
          + " MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:Place)"
          + " OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:Organisation)-[:IS_LOCATED_IN]->(uniCity:Place)"
          + " WITH"
          + "   friend,"
          + "   collect("
          + "     CASE uni.name"
          + "       WHEN null THEN null"
          + "       ELSE [uni.name, studyAt.classYear, uniCity.name]"
          + "     END"
          + "   ) AS unis,"
          + "   friendCity,"
          + "   distance"
          + " OPTIONAL MATCH (friend)-[worksAt:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(companyCountry:Place)"
          + " WITH"
          + "   friend,"
          + "   collect("
          + "     CASE company.name"
          + "       WHEN null THEN null"
          + "       ELSE [company.name, worksAt.workFrom, companyCountry.name]"
          + "     END"
          + "   ) AS companies,"
          + "   unis,"
          + "   friendCity,"
          + "   distance"
          + " RETURN"
          + "   friend.id AS id,"
          + "   friend.lastName AS lastName,"
          + "   distance,"
          + "   friend.birthday AS birthday,"
          + "   friend.creationDate AS creationDate,"
          + "   friend.gender AS gender,"
          + "   friend.browserUsed AS browser,"
          + "   friend.locationIP AS locationIp,"
          + "   friend.email AS emails,"
          + "   friend.speaks AS languages,"
          + "   friendCity.name AS cityName,"
          + "   unis,"
          + "   companies"
          + " ORDER BY distance ASC, friend.lastName ASC, toInt(friend.id) ASC"
          + " LIMIT {3}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.firstName(), 
            "3", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery1Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            List<String> emails;
            if (!record.get("emails").isNull()) {
              emails = record.get("emails").asList((e) -> e.asString());
            } else {
              emails = new ArrayList<>();
            }

            List<String> languages;
            if (!record.get("languages").isNull()) {
              languages = record.get("languages").asList((e) -> e.asString());
            } else {
              languages = new ArrayList<>();
            }

            List<List<Object>> universities;
            if (!record.get("unis").isNull()) {
              universities = record.get("unis").asList((e) -> 
                  e.asList());
            } else {
              universities = new ArrayList<>();
            }

            List<List<Object>> companies;
            if (!record.get("companies").isNull()) {
              companies = record.get("companies").asList((e) -> 
                  e.asList());
            } else {
              companies = new ArrayList<>();
            }

            resultList.add(
                new LdbcQuery1Result(
                    Long.valueOf(record.get("id").asString()), //Long.decode(row.getString(0)),
                    record.get("lastName").asString(), //row.getString(1),
                    record.get("distance").asInt(), //row.getInt(2),
                    record.get("birthday").asLong(), //row.getJsonNumber(3).longValue(),
                    record.get("creationDate").asLong(), //row.getJsonNumber(4).longValue(),
                    record.get("gender").asString(), //row.getString(5),
                    record.get("browser").asString(), //row.getString(6),
                    record.get("locationIp").asString(), //row.getString(7),
                    emails,
                    languages,
                    record.get("cityName").asString(), //row.getString(10),
                    universities,
                    companies));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery2Handler.class);

    @Override
    public void executeOperation(LdbcQuery2 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (:Person {id:{1}})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)"
          + " WHERE message.creationDate <= {2}"
          + " RETURN"
          + "   friend.id AS personId,"
          + "   friend.firstName AS personFirstName,"
          + "   friend.lastName AS personLastName,"
          + "   message.id AS messageId,"
          + "   CASE exists(message.content)"
          + "     WHEN true THEN message.content"
          + "     ELSE message.imageFile"
          + "   END AS messageContent,"
          + "   message.creationDate AS messageDate"
          + " ORDER BY messageDate DESC, toInt(messageId) ASC"
          + " LIMIT {3}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.maxDate().getTime(), 
            "3", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery2Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery2Result(
                    Long.valueOf(record.get("personId").asString()),
                    record.get("personFirstName").asString(),
                    record.get("personLastName").asString(),
                    Long.valueOf(record.get("messageId").asString()),
                    record.get("messageContent").asString(),
                    record.get("messageDate").asLong()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery3Handler.class);

    @Override
    public void executeOperation(LdbcQuery3 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      long periodStart = operation.startDate().getTime();
      long periodEnd = periodStart
          + ((long) operation.durationDays()) * 24l * 60l * 60l * 1000l;

      String statement =
          "   MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(messageX),"
          + " (messageX)-[:IS_LOCATED_IN]->(countryX:Place)"
          + " WHERE"
          + "   not(person=friend)"
          + "   AND not((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryX))"
          + "   AND countryX.name={2} AND messageX.creationDate>={4}"
          + "   AND messageX.creationDate<{5}"
          + " WITH friend, count(DISTINCT messageX) AS xCount"
          + " MATCH (friend)<-[:HAS_CREATOR]-(messageY)-[:IS_LOCATED_IN]->(countryY:Place)"
          + " WHERE"
          + "   countryY.name={3}"
          + "   AND not((friend)-[:IS_LOCATED_IN]->()-[:IS_PART_OF]->(countryY))"
          + "   AND messageY.creationDate>={4}"
          + "   AND messageY.creationDate<{5}"
          + " WITH"
          + "   friend.id AS friendId,"
          + "   friend.firstName AS friendFirstName,"
          + "   friend.lastName AS friendLastName,"
          + "   xCount,"
          + "   count(DISTINCT messageY) AS yCount"
          + " RETURN"
          + "   friendId,"
          + "   friendFirstName,"
          + "   friendLastName,"
          + "   xCount,"
          + "   yCount,"
          + "   xCount + yCount AS xyCount"
          + " ORDER BY xyCount DESC, toInt(friendId) ASC"
          + " LIMIT {6}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.countryXName(), 
            "3", operation.countryYName(),
            "4", periodStart,
            "5", periodEnd,
            "6", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery3Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery3Result(
                    Long.valueOf(record.get("friendId").asString()),
                    record.get("friendFirstName").asString(),
                    record.get("friendLastName").asString(),
                    record.get("xCount").asInt(),
                    record.get("yCount").asInt(),
                    record.get("xyCount").asInt()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery4Handler.class);

    @Override
    public void executeOperation(LdbcQuery4 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      long periodStart = operation.startDate().getTime();
      long periodEnd = periodStart
          + ((long) operation.durationDays()) * 24l * 60l * 60l * 1000l;

      String statement =
          "   MATCH (person:Person {id:{1}})-[:KNOWS]-(:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag:Tag)"
          + " WHERE post.creationDate >= {2} AND post.creationDate < {3}"
          + " OPTIONAL MATCH (tag)<-[:HAS_TAG]-(oldPost:Post)-[:HAS_CREATOR]->(:Person)-[:KNOWS]-(person)"
          + " WHERE oldPost.creationDate < {2}"
          + " WITH tag, post, length(collect(oldPost)) AS oldPostCount"
          + " WHERE oldPostCount=0"
          + " RETURN"
          + "   tag.name AS tagName,"
          + "   length(collect(post)) AS postCount"
          + " ORDER BY postCount DESC, tagName ASC"
          + " LIMIT {4}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", periodStart,
            "3", periodEnd, 
            "4", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery4Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery4Result(
                    record.get("tagName").asString(),
                    record.get("postCount").asInt()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery5Handler.class);

    @Override
    public void executeOperation(LdbcQuery5 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)<-[membership:HAS_MEMBER]-(forum:Forum)"
          + " WHERE membership.joinDate>{2} AND not(person=friend)"
          + " WITH DISTINCT friend, forum"
          + " OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)"
          + " WITH forum, count(post) AS postCount"
          + " RETURN"
          + "   forum.title AS forumName,"
          + "   postCount"
          + " ORDER BY postCount DESC, toInt(forum.id) ASC"
          + " LIMIT {3}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.minDate().getTime(), 
            "3", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery5Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery5Result(
                    record.get("forumName").asString(),
                    record.get("postCount").asInt()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery6Handler.class);

    @Override
    public void executeOperation(LdbcQuery6 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH"
          + "   (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person),"
          + "   (friend)<-[:HAS_CREATOR]-(friendPost:Post)-[:HAS_TAG]->(knownTag:Tag {name:{2}})"
          + " WHERE not(person=friend)"
          + " MATCH (friendPost)-[:HAS_TAG]->(commonTag:Tag)"
          + " WHERE not(commonTag=knownTag)"
          + " WITH DISTINCT commonTag, knownTag, friend"
          + " MATCH (commonTag)<-[:HAS_TAG]-(commonPost:Post)-[:HAS_TAG]->(knownTag)"
          + " WHERE (commonPost)-[:HAS_CREATOR]->(friend)"
          + " RETURN"
          + "   commonTag.name AS tagName,"
          + "   count(commonPost) AS postCount"
          + " ORDER BY postCount DESC, tagName ASC"
          + " LIMIT {3}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.tagName(), 
            "3", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery6Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery6Result(
                    record.get("tagName").asString(),
                    record.get("postCount").asInt()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery7Handler.class);

    @Override
    public void executeOperation(LdbcQuery7 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (person:Person {id:{1}})<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)"
          + " WITH liker, message, like.creationDate AS likeTime, person"
          + " ORDER BY likeTime DESC, toInt(message.id) ASC"
          + " WITH"
          + "   liker,"
          + "   head(collect({msg: message, likeTime: likeTime})) AS latestLike,"
          + "   person"
          + " RETURN"
          + "   liker.id AS personId,"
          + "   liker.firstName AS personFirstName,"
          + "   liker.lastName AS personLastName,"
          + "   latestLike.likeTime AS likeTime,"
          + "   latestLike.msg.id AS messageId,"
          + "   CASE exists(latestLike.msg.content)"
          + "     WHEN true THEN latestLike.msg.content"
          + "     ELSE latestLike.msg.imageFile"
          + "   END AS messageContent,"
          + "   latestLike.likeTime - latestLike.msg.creationDate AS latencyAsMilli,"
          + "   not((liker)-[:KNOWS]-(person)) AS isNew"
          + " ORDER BY likeTime DESC, toInt(personId) ASC"
          + " LIMIT {2}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery7Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery7Result(
                    Long.valueOf(record.get("personId").asString()),
                    record.get("personFirstName").asString(),
                    record.get("personLastName").asString(),
                    record.get("likeTime").asLong(),
                    Long.valueOf(record.get("messageId").asString()),
                    record.get("messageContent").asString(),
                    (int) (record.get("latencyAsMilli").asLong() / (1000l * 60l)),
                    record.get("isNew").asBoolean()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery8Handler.class);

    @Override
    public void executeOperation(LdbcQuery8 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH"
          + "   (start:Person {id:{1}})<-[:HAS_CREATOR]-()<-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)"
          + " RETURN"
          + "   person.id AS personId,"
          + "   person.firstName AS personFirstName,"
          + "   person.lastName AS personLastName,"
          + "   comment.creationDate AS commentCreationDate,"
          + "   comment.id AS commentId,"
          + "   comment.content AS commentContent"
          + " ORDER BY commentCreationDate DESC, toInt(commentId) ASC"
          + " LIMIT {2}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery8Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery8Result(
                    Long.valueOf(record.get("personId").asString()),
                    record.get("personFirstName").asString(),
                    record.get("personLastName").asString(),
                    record.get("commentCreationDate").asLong(),
                    Long.valueOf(record.get("commentId").asString()),
                    record.get("commentContent").asString()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery9Handler.class);

    @Override
    public void executeOperation(LdbcQuery9 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)<-[:HAS_CREATOR]-(message)"
          + " WHERE message.creationDate < {2}"
          + " RETURN DISTINCT"
          + "   friend.id AS personId,"
          + "   friend.firstName AS personFirstName,"
          + "   friend.lastName AS personLastName,"
          + "   message.id AS messageId,"
          + "   CASE exists(message.content)"
          + "     WHEN true THEN message.content"
          + "     ELSE message.imageFile"
          + "   END AS messageContent,"
          + "   message.creationDate AS messageCreationDate"
          + " ORDER BY message.creationDate DESC, toInt(message.id) ASC"
          + " LIMIT {3}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.maxDate().getTime(), 
            "3", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery9Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery9Result(
                    Long.valueOf(record.get("personId").asString()),
                    record.get("personFirstName").asString(),
                    record.get("personLastName").asString(),
                    Long.valueOf(record.get("messageId").asString()),
                    record.get("messageContent").asString(),
                    record.get("messageCreationDate").asLong()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery10Handler.class);

    @Override
    public void executeOperation(LdbcQuery10 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (person:Person {id:{1}})-[:KNOWS*2..2]-(friend:Person)-[:IS_LOCATED_IN]->(city:Place)"
          + " WHERE "
          + "   ((friend.birthday_month = {2} AND friend.birthday_day >= 21) OR"
          + "   (friend.birthday_month = ({2}%12)+1 AND friend.birthday_day < 22))"
          + "   AND not(friend=person)"
          + "   AND not((friend)-[:KNOWS]-(person))"
          + " WITH DISTINCT friend, city, person"
          + " OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)"
          + " WITH friend, city, collect(post) AS posts, person"
          + " WITH "
          + "   friend,"
          + "   city,"
          + "   length(posts) AS postCount,"
          + "   length([p IN posts WHERE (p)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(person)]) AS commonPostCount"
          + " RETURN"
          + "   friend.id AS personId,"
          + "   friend.firstName AS personFirstName,"
          + "   friend.lastName AS personLastName,"
          + "   friend.gender AS personGender,"
          + "   city.name AS personCityName,"
          + "   commonPostCount - (postCount - commonPostCount) AS commonInterestScore"
          + " ORDER BY commonInterestScore DESC, toInt(personId) ASC"
          + " LIMIT {3}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.month(), 
            "3", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery10Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery10Result(
                    Long.valueOf(record.get("personId").asString()),
                    record.get("personFirstName").asString(),
                    record.get("personLastName").asString(),
                    record.get("commonInterestScore").asInt(),
                    record.get("personGender").asString(),
                    record.get("personCityName").asString()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery11Handler.class);

    @Override
    public void executeOperation(LdbcQuery11 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)"
          + " WHERE not(person=friend)"
          + " WITH DISTINCT friend"
          + " MATCH (friend)-[worksAt:WORK_AT]->(company:Organisation)-[:IS_LOCATED_IN]->(:Place {name:{3}})"
          + " WHERE worksAt.workFrom < {2}"
          + " RETURN"
          + "   friend.id AS friendId,"
          + "   friend.firstName AS friendFirstName,"
          + "   friend.lastName AS friendLastName,"
          + "   company.name AS companyName,"
          + "   worksAt.workFrom AS workFromYear"
          + " ORDER BY workFromYear ASC, toInt(friendId) ASC, companyName DESC"
          + " LIMIT {4}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.workFromYear(), 
            "3", operation.countryName(),
            "4", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery11Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(
                new LdbcQuery11Result(
                    Long.valueOf(record.get("friendId").asString()),
                    record.get("friendFirstName").asString(),
                    record.get("friendLastName").asString(),
                    record.get("companyName").asString(),
                    record.get("workFromYear").asInt()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery12Handler.class);

    @Override
    public void executeOperation(LdbcQuery12 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (:Person {id:{1}})-[:KNOWS]-(friend:Person)"
          + " OPTIONAL MATCH"
          + "   (friend)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag),"
          + "   (tag)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)"
          + " WHERE tagClass.name = {2} OR baseTagClass.name = {2}"
          + " RETURN"
          + "   friend.id AS friendId,"
          + "   friend.firstName AS friendFirstName,"
          + "   friend.lastName AS friendLastName,"
          + "   collect(DISTINCT tag.name) AS tagNames,"
          + "   count(DISTINCT comment) AS count"
          + " ORDER BY count DESC, toInt(friendId) ASC"
          + " LIMIT {3}";
      Value parameters = parameters(
            "1", String.valueOf(operation.personId()), 
            "2", operation.tagClassName(), 
            "3", operation.limit());

      // Execute the query and get the results.
      List<LdbcQuery12Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            if (record.get("count").asInt() > 0) {
              List<String> tagNames = 
                  record.get("tagNames").asList((e) -> e.asString());

              resultList.add(
                  new LdbcQuery12Result(
                      Long.valueOf(record.get("friendId").asString()),
                      record.get("friendFirstName").asString(),
                      record.get("friendLastName").asString(),
                      tagNames,
                      record.get("count").asInt()));
            }
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery13Handler.class);

    @Override
    public void executeOperation(LdbcQuery13 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (person1:Person {id:{1}}), (person2:Person {id:{2}})"
          + " OPTIONAL MATCH path = shortestPath((person1)-[:KNOWS*..15]-(person2))"
          + " RETURN"
          + " CASE path IS NULL"
          + "   WHEN true THEN -1"
          + "   ELSE length(path)"
          + " END AS pathLength";
      Value parameters = parameters(
            "1", String.valueOf(operation.person1Id()), 
            "2", String.valueOf(operation.person2Id()));

      // Execute the query and get the results.
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          if (result.hasNext()) {
            Record record = result.next();

            resultReporter.report(
                0, 
                new LdbcQuery13Result(record.get("pathLength").asInt()), 
                operation);
          } else {
            resultReporter.report(0, null, operation);
          }
        }
      }
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcQuery14Handler.class);

    @Override
    public void executeOperation(LdbcQuery14 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH path = allShortestPaths((person1:Person {id:{1}})-[:KNOWS*..15]-(person2:Person {id:{2}}))"
          + " WITH nodes(path) AS pathNodes"
          + " RETURN"
          + "   extract(n IN pathNodes | n.id) AS pathNodeIds,"
          + "   reduce(weight=0.0, idx IN range(1,size(pathNodes)-1) | extract(prev IN [pathNodes[idx-1]] | extract(curr IN [pathNodes[idx]] | weight + length((curr)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(prev))*1.0 + length((prev)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(curr))*1.0 + length((prev)-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]-(curr))*0.5) )[0][0]) AS weight"
          + " ORDER BY weight DESC";
      Value parameters = parameters(
            "1", String.valueOf(operation.person1Id()), 
            "2", String.valueOf(operation.person2Id()));

      // Execute the query and get the results.
      List<LdbcQuery14Result> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            List<Long> personIdsInPath = new ArrayList<>();
            if (!record.get("pathNodeIds").isNull()) {
              personIdsInPath = 
                  record.get("pathNodeIds").asList((e) -> 
                      Long.valueOf(e.asString()));
            }

            resultList.add(
                new LdbcQuery14Result(
                    personIdsInPath,
                    record.get("weight").asDouble()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery1PersonProfileHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery1PersonProfile operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver(); 
      String statement =
          "   MATCH (n:Person {id:{id}})-[:IS_LOCATED_IN]-(p:Place)"
          + " RETURN"
          + "   n.firstName AS firstName,"
          + "   n.lastName AS lastName,"
          + "   n.birthday AS birthday,"
          + "   n.locationIP AS locationIp,"
          + "   n.browserUsed AS browserUsed,"
          + "   n.gender AS gender,"
          + "   n.creationDate AS creationDate,"
          + "   p.id AS cityId";
      Value parameters = parameters("id", String.valueOf(operation.personId()));

      // Execute the query and get the results.
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          if (result.hasNext()) {
            Record record = result.next();

            resultReporter.report(0, 
                new LdbcShortQuery1PersonProfileResult(
                    record.get("firstName").asString(),
                    record.get("lastName").asString(),
                    record.get("birthday").asLong(),
                    record.get("locationIp").asString(),
                    record.get("browserUsed").asString(),
                    Long.valueOf(record.get("cityId").asString()),
                    record.get("gender").asString(),
                    record.get("creationDate").asLong()),
                operation);
          } else {
            resultReporter.report(0, null, operation);
          }
        }
      }
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery2PersonPostsHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery2PersonPosts operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (:Person {id:{id}})<-[:HAS_CREATOR]-(m)-[:REPLY_OF*0..]->(p:Post)"
          + " MATCH (p)-[:HAS_CREATOR]->(c)"
          + " RETURN"
          + "   m.id as messageId,"
          + "   CASE exists(m.content)"
          + "     WHEN true THEN m.content"
          + "     ELSE m.imageFile"
          + "   END AS messageContent,"
          + "   m.creationDate AS messageCreationDate,"
          + "   p.id AS originalPostId,"
          + "   c.id AS originalPostAuthorId,"
          + "   c.firstName as originalPostAuthorFirstName,"
          + "   c.lastName as originalPostAuthorLastName"
          + " ORDER BY messageCreationDate DESC"
          + " LIMIT {limit}";
      Value parameters = parameters(
            "id", String.valueOf(operation.personId()), 
            "limit", operation.limit());

      // Execute the query and get the results.
      List<LdbcShortQuery2PersonPostsResult> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(new LdbcShortQuery2PersonPostsResult(
                Long.valueOf(record.get("messageId").asString()),
                record.get("messageContent").asString(),
                record.get("messageCreationDate").asLong(),
                Long.valueOf(record.get("originalPostId").asString()),
                Long.valueOf(record.get("originalPostAuthorId").asString()),
                record.get("originalPostAuthorFirstName").asString(),
                record.get("originalPostAuthorLastName").asString()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
    }
  }

  /**
   * Given a start Person, retrieve all of their friends, and the date at which
   * they became friends. Order results descending by friendship creation date,
   * then ascending by friend identifier.[1]
   */
  public static class LdbcShortQuery3PersonFriendsHandler implements
      OperationHandler<LdbcShortQuery3PersonFriends, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery3PersonFriendsHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery3PersonFriends operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (n:Person {id:{id}})-[r:KNOWS]-(friend)"
          + " RETURN"
          + "   friend.id AS personId,"
          + "   friend.firstName AS firstName,"
          + "   friend.lastName AS lastName,"
          + "   r.creationDate AS friendshipCreationDate"
          + " ORDER BY friendshipCreationDate DESC, toInt(personId) ASC";
      Value parameters = parameters(
            "id", String.valueOf(operation.personId()));

      // Execute the query and get the results.
      List<LdbcShortQuery3PersonFriendsResult> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(new LdbcShortQuery3PersonFriendsResult(
                Long.valueOf(record.get("personId").asString()),
                record.get("firstName").asString(),
                record.get("lastName").asString(),
                Long.valueOf(record.get("friendshipCreationDate").asLong())));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve its content and creation
   * date.[1]
   */
  public static class LdbcShortQuery4MessageContentHandler implements
      OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery4MessageContentHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery4MessageContent operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (m:Message {id:{id}})"
          + " RETURN"
          + "   CASE exists(m.content)"
          + "     WHEN true THEN m.content"
          + "     ELSE m.imageFile"
          + "   END AS messageContent,"
          + "   m.creationDate as messageCreationDate";
      Value parameters = parameters(
            "id", String.valueOf(operation.messageId()));

      // Execute the query and get the results.
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          if (result.hasNext()) {
            Record record = result.next();

            resultReporter.report(
                0, 
                new LdbcShortQuery4MessageContentResult(
                    record.get("messageContent").asString(),
                    record.get("messageCreationDate").asLong()), 
                operation);
          } else {
            resultReporter.report(0, null, operation);
          }
        }
      }
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve its author.[1]
   */
  public static class LdbcShortQuery5MessageCreatorHandler implements
      OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery5MessageCreatorHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery5MessageCreator operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (m:Message {id:{id}})-[:HAS_CREATOR]->(p:Person)"
          + " RETURN"
          + "   p.id AS personId,"
          + "   p.firstName AS firstName,"
          + "   p.lastName AS lastName";
      Value parameters = parameters(
            "id", String.valueOf(operation.messageId()));

      // Execute the query and get the results.
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          if (result.hasNext()) {
            Record record = result.next();

            resultReporter.report(
                0, 
                new LdbcShortQuery5MessageCreatorResult(
                    Long.valueOf(record.get("personId").asString()),
                    record.get("firstName").asString(),
                    record.get("lastName").asString()), 
                operation);
          } else {
            resultReporter.report(0, null, operation);
          }
        }
      }
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery6MessageForumHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery6MessageForum operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (m:Message {id:{id}})-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)"
          + " RETURN"
          + "   f.id AS forumId,"
          + "   f.title AS forumTitle,"
          + "   mod.id AS moderatorId,"
          + "   mod.firstName AS moderatorFirstName,"
          + "   mod.lastName AS moderatorLastName";
      Value parameters = parameters(
            "id", String.valueOf(operation.messageId()));

      // Execute the query and get the results.
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          if (result.hasNext()) {
            Record record = result.next();

            resultReporter.report(
                0, 
                new LdbcShortQuery6MessageForumResult(
                    Long.valueOf(record.get("forumId").asString()),
                    record.get("forumTitle").asString(),
                    Long.valueOf(record.get("moderatorId").asString()),
                    record.get("moderatorFirstName").asString(),
                    record.get("moderatorLastName").asString()),
                operation);
          } else {
            resultReporter.report(0, null, operation);
          }
        }
      }
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

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery7MessageRepliesHandler.class);

    @Override
    public void executeOperation(LdbcShortQuery7MessageReplies operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      String statement =
          "   MATCH (m:Message {id:{id}})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)"
          + " OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)"
          + " RETURN"
          + "   c.id AS commentId,"
          + "   c.content AS commentContent,"
          + "   c.creationDate AS commentCreationDate,"
          + "   p.id AS replyAuthorId,"
          + "   p.firstName AS replyAuthorFirstName,"
          + "   p.lastName AS replyAuthorLastName,"
          + "   CASE r"
          + "     WHEN null THEN false"
          + "     ELSE true"
          + "   END AS replyAuthorKnowsOriginalMessageAuthor"
          + " ORDER BY commentCreationDate DESC, toInt(replyAuthorId) ASC";
      Value parameters = parameters(
            "id", String.valueOf(operation.messageId()));

      // Execute the query and get the results.
      List<LdbcShortQuery7MessageRepliesResult> resultList = new ArrayList<>();
      try (Session session = driver.session(AccessMode.READ)) {
        try (Transaction tx = session.beginTransaction()) {
          StatementResult result = tx.run(statement, parameters);
          tx.success();
          tx.close();

          while (result.hasNext()) {
            Record record = result.next();

            resultList.add(new LdbcShortQuery7MessageRepliesResult(
                Long.valueOf(record.get("commentId").asString()),
                record.get("commentContent").asString(),
                record.get("commentCreationDate").asLong(),
                Long.valueOf(record.get("replyAuthorId").asString()),
                record.get("replyAuthorFirstName").asString(),
                record.get("replyAuthorLastName").asString(),
                record.get("replyAuthorKnowsOriginalMessageAuthor").asBoolean()));
          }
        }
      }

      resultReporter.report(0, resultList, operation);
    }
  }

  /**
   * ------------------------------------------------------------------------
   * Update Queries
   * ------------------------------------------------------------------------
   */
  /**
   * Add a Person to the social network. [1]
   * <p>
   * TODO:
   * <ul>
   * <li>This query involves creating many relationships of different types.
   * This is currently done using multiple cypher queries, but it may be
   * possible to combine them in some way to amortize per query overhead and
   * thus increase performance.</li>
   * </ul>
   */
  public static class LdbcUpdate1AddPersonHandler implements
      OperationHandler<LdbcUpdate1AddPerson, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate1AddPersonHandler.class);
    private final Calendar calendar;

    public LdbcUpdate1AddPersonHandler() {
      this.calendar = new GregorianCalendar();
    }

    @Override
    public void executeOperation(LdbcUpdate1AddPerson operation,
        DbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      try (Session session = driver.session(AccessMode.WRITE)) {
        try (Transaction tx = session.beginTransaction()) {
          // Create the person node.
          String statement =
              "   CREATE (p:Person {id: {id}, firstName: {firstName}, lastName: {lastName}, gender: {gender}, birthday: {birthday}, creationDate: {creationDate}, locationIP: {locationIP}, browserUsed: {browserUsed}, speaks: {speaks}, emails: {emails}})";
          Value parameters = parameters(
              "id", String.valueOf(operation.personId()),
              "firstName", operation.personFirstName(),
              "lastName", operation.personLastName(),
              "gender", operation.gender(),
              "birthday", operation.birthday().getTime(),
              "creationDate", operation.creationDate().getTime(),
              "locationIP", operation.locationIp(),
              "browserUsed", operation.browserUsed(),
              "speaks", operation.languages(),
              "emails", operation.emails());

          tx.run(statement, parameters);

          // Add isLocatedIn and hasInterest relationships.
          statement =
              "   MATCH (p:Person {id:{personId}}),"
              + "       (c:Place {id:{cityId}})"
              + " OPTIONAL MATCH (t:Tag)"
              + " WHERE t.id IN {tagIds}"
              + " WITH p, c, collect(t) AS tagSet"
              + " CREATE (p)-[:IS_LOCATED_IN]->(c)"
              + " FOREACH(t IN tagSet| CREATE (p)-[:HAS_INTEREST]->(t))";
          parameters = parameters(
              "personId", String.valueOf(operation.personId()),
              "cityId", String.valueOf(operation.cityId()),
              "tagIds", DbHelper.listLongToListString(operation.tagIds()));

          tx.run(statement, parameters);

          // Add studyAt relationships.
          if (operation.studyAt().size() > 0) {
            StringBuilder matchBldr = new StringBuilder();
            StringBuilder createBldr = new StringBuilder();
            List<Object> params = new ArrayList<>();

            matchBldr.append("MATCH (p:Person {id:{personId}}), ");
            createBldr.append("CREATE ");
            params.add("personId");
            params.add(String.valueOf(operation.personId()));

            for (int i = 0; i < operation.studyAt().size(); i++) {
              Organization org = operation.studyAt().get(i);
              if (i > 0) {
                matchBldr.append(", ");
                createBldr.append(", ");
              }
              matchBldr.append(
                  String.format("(u%d:Organisation {id:{uId%d}})", i, i));
              createBldr.append(
                  String.format("(p)-[:STUDY_AT {classYear:{cY%d}}]->(u%d)", i, i));
              params.add(String.format("uId%d", i));
              params.add(String.format("%d", org.organizationId()));
              params.add(String.format("cY%d", i));
              params.add(String.format("%d", org.year()));
            }


            statement = matchBldr.toString() + " " + createBldr.toString();
            parameters = parameters(params.toArray());

            tx.run(statement, parameters);
          }

          // Add workAt relationships.
          if (operation.workAt().size() > 0) {
            StringBuilder matchBldr = new StringBuilder();
            StringBuilder createBldr = new StringBuilder();
            List<Object> params = new ArrayList<>();

            matchBldr.append("MATCH (p:Person {id:{personId}}), ");
            createBldr.append("CREATE ");
            params.add("personId");
            params.add(String.valueOf(operation.personId()));

            for (int i = 0; i < operation.workAt().size(); i++) {
              Organization org = operation.workAt().get(i);
              if (i > 0) {
                matchBldr.append(", ");
                createBldr.append(", ");
              }
              matchBldr.append(
                  String.format("(c%d:Organisation {id:{cId%d}})", i, i));
              createBldr.append(
                  String.format("(p)-[:WORK_AT {workFrom:{wF%d}}]->(c%d)", i, i));
              params.add(String.format("cId%d", i));
              params.add(String.format("%d", org.organizationId()));
              params.add(String.format("wF%d", i));
              params.add(String.format("%d", org.year()));
            }

            statement = matchBldr.toString() + " " + createBldr.toString();
            parameters = parameters(params.toArray());

            tx.run(statement, parameters);
          }

          tx.success();
          tx.close();
        }
      }

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Like to a Post of the social network.[1]
   */
  public static class LdbcUpdate2AddPostLikeHandler implements
      OperationHandler<LdbcUpdate2AddPostLike, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate2AddPostLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate2AddPostLike operation,
        DbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      try (Session session = driver.session(AccessMode.WRITE)) {
        try (Transaction tx = session.beginTransaction()) {
          String statement =
              "   MATCH (p:Person {id:{personId}}),"
              + "       (m:Post {id:{postId}})"
              + " CREATE (p)-[:LIKES {creationDate:{creationDate}}]->(m)";
          Value parameters = parameters(
              "personId", String.valueOf(operation.personId()),
              "postId", String.valueOf(operation.postId()),
              "creationDate", operation.creationDate().getTime());

          tx.run(statement, parameters);
          tx.success();
          tx.close();
        }
      }

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Like to a Comment of the social network.[1]
   */
  public static class LdbcUpdate3AddCommentLikeHandler implements
      OperationHandler<LdbcUpdate3AddCommentLike, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate3AddCommentLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate3AddCommentLike operation,
        DbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      try (Session session = driver.session(AccessMode.WRITE)) {
        try (Transaction tx = session.beginTransaction()) {
          String statement =
              "   MATCH (p:Person {id:{personId}}),"
              + "       (m:Comment {id:{commentId}})"
              + " CREATE (p)-[:LIKES {creationDate:{creationDate}}]->(m)";
          Value parameters = parameters(
              "personId", String.valueOf(operation.personId()),
              "commentId", String.valueOf(operation.commentId()),
              "creationDate", operation.creationDate().getTime());

          tx.run(statement, parameters);
          tx.success();
          tx.close();
        }
      }

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Forum to the social network.[1]
   */
  public static class LdbcUpdate4AddForumHandler implements
      OperationHandler<LdbcUpdate4AddForum, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate4AddForumHandler.class);

    @Override
    public void executeOperation(LdbcUpdate4AddForum operation,
        DbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      try (Session session = driver.session(AccessMode.WRITE)) {
        try (Transaction tx = session.beginTransaction()) {
          // Create the forum node.
          String statement =
              "   CREATE (f:Forum {id: {id}, title: {title}, creationDate: {creationDate}})";
          Value parameters = parameters(
              "id", String.valueOf(operation.forumId()),
              "title", operation.forumTitle(),
              "creationDate", operation.creationDate().getTime());

          tx.run(statement, parameters);

          // Add hasModerator and hasTag relationships.
          statement =
              "   MATCH (f:Forum {id:{forumId}}),"
              + "       (p:Person {id:{moderatorId}})"
              + " OPTIONAL MATCH (t:Tag)"
              + " WHERE t.id IN {tagIds}"
              + " WITH f, p, collect(t) as tagSet"
              + " CREATE (f)-[:HAS_MODERATOR]->(p)"
              + " FOREACH (t IN tagSet| CREATE (f)-[:HAS_TAG]->(t))";
          parameters = parameters(
              "forumId", String.valueOf(operation.forumId()),
              "moderatorId", String.valueOf(operation.moderatorPersonId()),
              "tagIds", DbHelper.listLongToListString(operation.tagIds()));

          tx.run(statement, parameters);

          tx.success();
          tx.close();
        }
      }

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Forum membership to the social network.[1]
   */
  public static class LdbcUpdate5AddForumMembershipHandler implements
      OperationHandler<LdbcUpdate5AddForumMembership, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate5AddForumMembershipHandler.class);

    @Override
    public void executeOperation(LdbcUpdate5AddForumMembership operation,
        DbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      try (Session session = driver.session(AccessMode.WRITE)) {
        try (Transaction tx = session.beginTransaction()) {
          String statement =
              "   MATCH (f:Forum {id:{forumId}}),"
              + "       (p:Person {id:{personId}})"
              + " CREATE (f)-[:HAS_MEMBER {joinDate:{joinDate}}]->(p)";
          Value parameters = parameters(
              "forumId", String.valueOf(operation.forumId()),
              "personId", String.valueOf(operation.personId()),
              "joinDate", operation.joinDate().getTime());

          tx.run(statement, parameters);
          tx.success();
          tx.close();
        }
      }

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Post to the social network.[1]
   */
  public static class LdbcUpdate6AddPostHandler implements
      OperationHandler<LdbcUpdate6AddPost, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate6AddPostHandler.class);

    @Override
    public void executeOperation(LdbcUpdate6AddPost operation,
        DbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      try (Session session = driver.session(AccessMode.WRITE)) {
        try (Transaction tx = session.beginTransaction()) {
          // Create the post node.
          String statement;
          Value parameters;
          if (operation.imageFile().length() > 0) {
            statement =
                "   CREATE (m:Post:Message {id: {id}, imageFile: {imageFile}, creationDate: {creationDate}, locationIP: {locationIP}, browserUsed: {browserUsed}, language: {language}, length: {length}})";
            parameters = parameters(
                "id", String.valueOf(operation.postId()),
                "imageFile", operation.imageFile(),
                "creationDate", operation.creationDate().getTime(),
                "locationIP", operation.locationIp(),
                "browserUsed", operation.browserUsed(),
                "language", operation.language(),
                "length", operation.length());
          } else {
            statement =
                "   CREATE (m:Post:Message {id: {id}, creationDate: {creationDate}, locationIP: {locationIP}, browserUsed: {browserUsed}, language: {language}, content: {content}, length: {length}})";
            parameters = parameters(
                "id", String.valueOf(operation.postId()),
                "creationDate", operation.creationDate().getTime(),
                "locationIP", operation.locationIp(),
                "browserUsed", operation.browserUsed(),
                "language", operation.language(),
                "content", operation.content(),
                "length", operation.length());
          }

          tx.run(statement, parameters);

          // Add hasCreator, containerOf, isLocatedIn, and hasTag relationships.
          statement =
              "   MATCH (m:Post {id:{postId}}),"
              + "       (p:Person {id:{authorId}}),"
              + "       (f:Forum {id:{forumId}}),"
              + "       (c:Place {id:{countryId}})"
              + " OPTIONAL MATCH (t:Tag)"
              + " WHERE t.id IN {tagIds}"
              + " WITH m, p, f, c, collect(t) as tagSet"
              + " CREATE (m)-[:HAS_CREATOR]->(p),"
              + "        (m)<-[:CONTAINER_OF]-(f),"
              + "        (m)-[:IS_LOCATED_IN]->(c)"
              + " FOREACH (t IN tagSet| CREATE (m)-[:HAS_TAG]->(t))";
          parameters = parameters(
              "postId", String.valueOf(operation.postId()),
              "authorId", String.valueOf(operation.authorPersonId()),
              "forumId", String.valueOf(operation.forumId()),
              "countryId", String.valueOf(operation.countryId()),
              "tagIds", DbHelper.listLongToListString(operation.tagIds()));

          tx.run(statement, parameters);

          tx.success();
          tx.close();
        }
      }

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a Comment replying to a Post/Comment to the social network.[1]
   */
  public static class LdbcUpdate7AddCommentHandler implements
      OperationHandler<LdbcUpdate7AddComment, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate7AddCommentHandler.class);

    @Override
    public void executeOperation(LdbcUpdate7AddComment operation,
        DbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      try (Session session = driver.session(AccessMode.WRITE)) {
        try (Transaction tx = session.beginTransaction()) {
          // Create the comment node.
          String statement =
              "   CREATE (c:Comment:Message {id: {id}, creationDate: {creationDate}, locationIP: {locationIP}, browserUsed: {browserUsed}, content: {content}, length: {length}})";
          Value parameters = parameters(
              "id", String.valueOf(operation.commentId()),
              "creationDate", operation.creationDate().getTime(),
              "locationIP", operation.locationIp(),
              "browserUsed", operation.browserUsed(),
              "content", operation.content(),
              "length", operation.length());

          tx.run(statement, parameters);

          Long replyOfId;
          if (operation.replyToCommentId() != -1) {
            replyOfId = operation.replyToCommentId();
          } else {
            replyOfId = operation.replyToPostId();
          }

          // Add hasCreator, containerOf, isLocatedIn, and hasTag relationships.
          statement =
              "   MATCH (m:Comment {id:{commentId}}),"
              + "       (p:Person {id:{authorId}}),"
              + "       (r:Message {id:{replyOfId}}),"
              + "       (c:Place {id:{countryId}})"
              + " OPTIONAL MATCH (t:Tag)"
              + " WHERE t.id IN {tagIds}"
              + " WITH m, p, r, c, collect(t) as tagSet"
              + " CREATE (m)-[:HAS_CREATOR]->(p),"
              + "        (m)-[:REPLY_OF]->(r),"
              + "        (m)-[:IS_LOCATED_IN]->(c)"
              + " FOREACH (t IN tagSet| CREATE (m)-[:HAS_TAG]->(t))";
          parameters = parameters(
              "commentId", String.valueOf(operation.commentId()),
              "authorId", String.valueOf(operation.authorPersonId()),
              "replyOfId", String.valueOf(replyOfId),
              "countryId", String.valueOf(operation.countryId()),
              "tagIds", DbHelper.listLongToListString(operation.tagIds()));

          tx.run(statement, parameters);

          tx.success();
          tx.close();
        }
      }

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  /**
   * Add a friendship relation to the social network.[1]
   */
  public static class LdbcUpdate8AddFriendshipHandler implements
      OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {

    private static final Logger logger =
        LoggerFactory.getLogger(LdbcUpdate8AddFriendshipHandler.class);

    @Override
    public void executeOperation(LdbcUpdate8AddFriendship operation,
        DbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {

      Driver driver = ((Neo4jDbConnectionState) dbConnectionState).getDriver();

      try (Session session = driver.session(AccessMode.WRITE)) {
        try (Transaction tx = session.beginTransaction()) {
          String statement =
              "   MATCH (p1:Person {id:{person1Id}}),"
              + "       (p2:Person {id:{person2Id}})"
              + " CREATE (p1)-[:KNOWS {creationDate:{creationDate}}]->(p2)";
          Value parameters = parameters(
              "person1Id", String.valueOf(operation.person1Id()),
              "person2Id", String.valueOf(operation.person2Id()),
              "creationDate", operation.creationDate().getTime());

          tx.run(statement, parameters);

          tx.success();
          tx.close();
        }
      }

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }
}
