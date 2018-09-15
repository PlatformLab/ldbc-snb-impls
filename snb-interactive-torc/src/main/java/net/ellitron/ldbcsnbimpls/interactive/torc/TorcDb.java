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

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.incr;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.decr;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.assign;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.mult;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.minus;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.Pop.*;
import static org.apache.tinkerpop.gremlin.structure.Column.*;

import net.ellitron.torc.*;
import net.ellitron.torc.util.UInt128;
import net.ellitron.torc.TorcGraphProviderOptimizationStrategy;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * An implementation of the LDBC SNB interactive workload[1] for TorcDB.
 * Queries are executed against a running RAMCloud cluster. Configuration
 * parameters for this implementation (that are supplied via the LDBC driver)
 * are listed below.
 * <p>
 * Configuration Parameters:
 * <ul>
 * <li>coordinatorLocator - locator string for the RAMCloud cluster coordinator
 * (default: tcp:host=127.0.0.1,port=12246).</li>
 * <li>graphName - name of the graph stored in RAMCloud against which to
 * execute queries (default: default).</li>
 * <li>txReads - the presence of this switch turns on performing transactions
 * for read queries (Note: at time of writing complex read queries touch too
 * much data and trying to do these transactionally will result in a timeout.
 * This is currently being fixed in RAMCloud).</li>
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
public class TorcDb extends Db {

  private TorcDbConnectionState connectionState = null;
  private static boolean doTransactionalReads = false;
  private static boolean fakeComplexReads = false;
  private static String personIDsFilename;
  private static String messageIDsFilename;
  private static List<Long> personIDs;
  private static List<Long> messageIDs;

  // Maximum number of times to try a transaction before giving up.
  private static int MAX_TX_ATTEMPTS = 100;

  @Override
  protected void onInit(Map<String, String> properties,
      LoggingService loggingService) throws DbException {

    connectionState = new TorcDbConnectionState(properties);

    if (properties.containsKey("txReads")) {
      doTransactionalReads = true;
    }

    if (properties.containsKey("personIDsFile") && 
        properties.containsKey("messageIDsFile")) {
      this.personIDsFilename = properties.get("personIDsFile");
      this.personIDs = new ArrayList<>();

      try (BufferedReader br = 
          new BufferedReader(new FileReader(personIDsFilename))) {
        String line;
        while ((line = br.readLine()) != null) {
          personIDs.add(Long.decode(line));
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } 

      this.messageIDsFilename = properties.get("messageIDsFile");
      this.messageIDs = new ArrayList<>();

      try (BufferedReader br = 
          new BufferedReader(new FileReader(messageIDsFilename))) {
        String line;
        while ((line = br.readLine()) != null) {
          messageIDs.add(Long.decode(line));
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } 

      this.fakeComplexReads = true;
    } else if (properties.containsKey("personIDsFile") ||
               properties.containsKey("messageIDsFile")) {
      throw new RuntimeException(
          "Error: Must specify BOTH personIDs and messageIDs file");
    }

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

  @Override
  protected void onClose() throws IOException {
    connectionState.close();
  }

  @Override
  protected DbConnectionState getConnectionState() throws DbException {
    return connectionState;
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
      if (fakeComplexReads) {
        List<LdbcQuery1Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
          Long pid = personIDs.get(n1);
          result.add(new LdbcQuery1Result(
              pid,
              null,
              0,
              0,
              0,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        long personId = operation.personId();
        String firstName = operation.firstName();
        int resultLimit = operation.limit();
        int maxLevels = 3;
        Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

        GraphTraversalSource g = graph.traversal();

        List<Long> distList = new ArrayList<>(resultLimit);
        List<Vertex> matchList = new ArrayList<>(resultLimit);

        Vertex root = g.V(new UInt128(TorcEntity.PERSON.idSpace, personId))
            .next();

        g.withSideEffect("x", matchList).withSideEffect("d", distList)
            .V(root).aggregate("done").out("knows")
            .where(without("done")).dedup().fold().sideEffect(
                unfold().has("firstName", firstName).order()
                .by("lastName", incr).by(id(), incr).limit(resultLimit)
                .as("person")
                .select("x").by(count(Scope.local)).is(lt(resultLimit))
                .store("x").by(select("person"))
            ).filter(select("x").count(Scope.local).is(lt(resultLimit))
                .store("d")).unfold().aggregate("done").out("knows")
            .where(without("done")).dedup().fold().sideEffect(
                unfold().has("firstName", firstName).order()
                .by("lastName", incr).by(id(), incr).limit(resultLimit)
                .as("person")
                .select("x").by(count(Scope.local)).is(lt(resultLimit))
                .store("x").by(select("person"))
            ).filter(select("x").count(Scope.local).is(lt(resultLimit))
                .store("d")).unfold().aggregate("done").out("knows")
            .where(without("done")).dedup().fold().sideEffect(
                unfold().has("firstName", firstName).order()
                .by("lastName", incr).by(id(), incr).limit(resultLimit)
                .as("person")
                .select("x").by(count(Scope.local)).is(lt(resultLimit))
                .store("x").by(select("person"))
            ).select("x").count(Scope.local)
            .store("d").iterate();

        Map<Vertex, Map<String, List<String>>> propertiesMap =
            new HashMap<>(matchList.size());
        g.V(matchList.toArray()).as("person")
            .<List<String>>valueMap().as("props")
            .select("person", "props")
            .forEachRemaining(map -> {
              propertiesMap.put((Vertex) map.get("person"),
                  (Map<String, List<String>>) map.get("props"));
            });

        Map<Vertex, String> placeNameMap = new HashMap<>(matchList.size());
        g.V(matchList.toArray()).as("person")
            .out("isLocatedIn")
            .<String>values("name")
            .as("placeName")
            .select("person", "placeName")
            .forEachRemaining(map -> {
              placeNameMap.put((Vertex) map.get("person"),
                  (String) map.get("placeName"));
            });

        Map<Vertex, List<List<Object>>> universityInfoMap =
            new HashMap<>(matchList.size());
        g.V(matchList.toArray()).as("person")
            .outE("studyAt").as("classYear")
            .inV().as("universityName")
            .out("isLocatedIn").as("cityName")
            .select("person", "universityName", "classYear", "cityName")
            .by().by("name").by("classYear").by("name")
            .forEachRemaining(map -> {
              Vertex v = (Vertex) map.get("person");
              List<Object> tuple = new ArrayList<>(3);
              tuple.add(map.get("universityName"));
              tuple.add(Integer.decode((String) map.get("classYear")));
              tuple.add(map.get("cityName"));
              if (universityInfoMap.containsKey(v)) {
                universityInfoMap.get(v).add(tuple);
              } else {
                List<List<Object>> tupleList = new ArrayList<>();
                tupleList.add(tuple);
                universityInfoMap.put(v, tupleList);
              }
            });

        Map<Vertex, List<List<Object>>> companyInfoMap =
            new HashMap<>(matchList.size());
        g.V(matchList.toArray()).as("person")
            .outE("workAt").as("workFrom")
            .inV().as("companyName")
            .out("isLocatedIn").as("cityName")
            .select("person", "companyName", "workFrom", "cityName")
            .by().by("name").by("workFrom").by("name")
            .forEachRemaining(map -> {
              Vertex v = (Vertex) map.get("person");
              List<Object> tuple = new ArrayList<>(3);
              tuple.add(map.get("companyName"));
              tuple.add(Integer.decode((String) map.get("workFrom")));
              tuple.add(map.get("cityName"));
              if (companyInfoMap.containsKey(v)) {
                companyInfoMap.get(v).add(tuple);
              } else {
                List<List<Object>> tupleList = new ArrayList<>();
                tupleList.add(tuple);
                companyInfoMap.put(v, tupleList);
              }
            });

        List<LdbcQuery1Result> result = new ArrayList<>();

        for (int i = 0; i < matchList.size(); i++) {
          Vertex match = matchList.get(i);
          int distance = (i < distList.get(0)) ? 1
              : (i < distList.get(1)) ? 2 : 3;
          Map<String, List<String>> properties = propertiesMap.get(match);
          List<String> emails = properties.get("email");
          if (emails == null) {
            emails = new ArrayList<>();
          }
          List<String> languages = properties.get("language");
          if (languages == null) {
            languages = new ArrayList<>();
          }
          String placeName = placeNameMap.get(match);
          List<List<Object>> universityInfo = universityInfoMap.get(match);
          if (universityInfo == null) {
            universityInfo = new ArrayList<>();
          }
          List<List<Object>> companyInfo = companyInfoMap.get(match);
          if (companyInfo == null) {
            companyInfo = new ArrayList<>();
          }
          result.add(new LdbcQuery1Result(
              ((UInt128) match.id()).getLowerLong(),
              properties.get("lastName").get(0),
              distance,
              Long.decode(properties.get("birthday").get(0)),
              Long.decode(properties.get("creationDate").get(0)),
              properties.get("gender").get(0),
              properties.get("browserUsed").get(0),
              properties.get("locationIP").get(0),
              emails,
              languages,
              placeName,
              universityInfo,
              companyInfo));
        }

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery2Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
          int n2 = ThreadLocalRandom.current().nextInt(0, messageIDs.size());
          Long pid = personIDs.get(n1);
          Long mid = messageIDs.get(n2);
          result.add(new LdbcQuery2Result(
              pid, 
              null,
              null,
              mid,
              null,
              0));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }
      
      // Parameters of this query
      final long personId = operation.personId();
      final long maxDate = operation.maxDate().getTime();
      final int limit = operation.limit();
      
      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery2Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result).V(torcPersonId)
            .out("knows").as("friend")
            .in("hasCreator").as("message")
            .order().by("creationDate", decr).by(id(), incr)
            .filter(t -> 
                Long.valueOf(t.get().value("creationDate")) <= maxDate)
            .limit(limit)
            .project("personId", "firstName", "lastName", "messageId", 
                "content", "creationDate")
                .by(select("friend").id())
                .by(select("friend").values("firstName"))
                .by(select("friend").values("lastName"))
                .by(select("message").id())
                .by(select("message")
                    .choose(values("content").is(neq("")),
                        values("content"),
                        values("imageFile")))
                .by(select("message").values("creationDate"))
            .map(t -> new LdbcQuery2Result(
                ((UInt128)t.get().get("personId")).getLowerLong(),
                (String)t.get().get("firstName"), 
                (String)t.get().get("lastName"),
                ((UInt128)t.get().get("messageId")).getLowerLong(), 
                (String)t.get().get("content"),
                Long.valueOf((String)t.get().get("creationDate"))))
            .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery3Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
          Long pid = personIDs.get(n1);
          result.add(new LdbcQuery3Result(
              pid,
              null,
              null,
              0,
              0,
              0));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }

      // Parameters of this query
      final long personId = operation.personId();
      final String countryXName = operation.countryXName();
      final String countryYName = operation.countryYName();
      final long startDate = operation.startDate().getTime();
      final long durationDays = operation.durationDays();
      final int limit = operation.limit();

      final long endDate = startDate + (durationDays * 24L * 60L * 60L * 1000L);

      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery3Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result).V(torcPersonId).as("person")
          .out("knows")
          .union(identity(), out("knows")).dedup().where(neq("person"))
          .where(
            out("isLocatedIn").out("isPartOf").is(without(countryXName, countryYName))
          )
          .as("friend")
          .in("hasCreator")
          .filter(t -> {
                    long date = Long.valueOf(t.get().value("creationDate"));
                    return date <= endDate && date >= startDate;
                  })
          .out("isLocatedIn").values("name")
          .where( is(within(countryXName, countryYName)) )
          .group().by(select("friend"))
          .flatMap(t -> {
                Map m = t.get();
                List removeList = new ArrayList<Object>();
                for (Object k : m.keySet()) {
                  List v = (List) m.get(k);
                  if ( !v.contains(countryXName) || !v.contains(countryYName) )
                    removeList.add(k);
                }

                for (Object k : removeList)
                  m.remove(k);

                return m.entrySet().iterator();
              })
          .order()
            .by(select(values).unfold().count(), decr)
            .by(select(keys).id(), incr)
          .limit(limit)
          .project("personId",
              "firstName",
              "lastName",
              "countryXCount",
              "countryYCount",
              "totalCount")
            .by(select(keys).id())
            .by(select(keys).values("firstName"))
            .by(select(keys).values("lastName"))
            .by(select(values).unfold().is(eq(countryXName)).count())
            .by(select(values).unfold().is(eq(countryYName)).count())
            .by(select(values).unfold().count())
          .map(t -> new LdbcQuery3Result(
              ((UInt128)((Traverser<Map>)t).get().get("personId")).getLowerLong(),
              (String)((Traverser<Map>)t).get().get("firstName"), 
              (String)((Traverser<Map>)t).get().get("lastName"),
              (Long)((Traverser<Map>)t).get().get("countryXCount"),
              (Long)((Traverser<Map>)t).get().get("countryYCount"),
              (Long)((Traverser<Map>)t).get().get("totalCount")))
          .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery4Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          result.add(new LdbcQuery4Result(
              null,
              0));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }

      // Parameters of this query
      final long personId = operation.personId();
      final long startDate = operation.startDate().getTime();
      final long durationDays = operation.durationDays();
      final int limit = operation.limit();

      final long endDate = startDate + (durationDays * 24L * 60L * 60L * 1000L);

      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery4Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result).V(torcPersonId).out("knows")
          .in("hasCreator")
          .hasLabel("Post")
          .as("post")
          .values("creationDate")
          .sideEffect(
              filter(t -> {
                    long date = Long.valueOf((String)t.get());
                    return date < startDate;
                    })
              .select("post").out("hasTag").dedup().aggregate("oldTags")
          )
          .barrier()
          .filter(t -> {
                    long date = Long.valueOf((String)t.get());
                    return date <= endDate && date >= startDate;
                  })
          .select("post")
          .out("hasTag")
          .where(without("oldTags")).values("name")
          .as("newTags")
          .select("post")
          .group().by(select("newTags")).by(count())
          .order(local)
            .by(select(values), decr)
            .by(select(keys), incr)
          .limit(local, limit)
          .unfold()
          .project("tagName",
              "postCount")
            .by(select(keys))
            .by(select(values))
          .map(t -> new LdbcQuery4Result(
              (String)(t.get().get("tagName")), 
              ((Long)(t.get().get("postCount"))).intValue()))
          .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery5Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          result.add(new LdbcQuery5Result(
              null,
              0));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }

      // Parameters of this query
      final long personId = operation.personId();
      final long minDate = operation.minDate().getTime();
      final int limit = operation.limit();

      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery5Result> result = new ArrayList<>(limit);
        List<Vertex> forums = new ArrayList<>();

        g.withSideEffect("result", result).withSideEffect("forums", forums)
          .V(torcPersonId).as("person")
          .out("knows")
          .union(identity(), out("knows")).dedup().where(neq("person"))
          .as("friend")
          .aggregate("friendAgg")
          .inE("hasMember")
          .as("memberEdge")
          .values("joinDate")
          .filter(t -> {
                    long date = Long.valueOf((String)t.get());
                    return date > minDate;
                })
          .select("memberEdge")
          .outV()
          .store("forums")
          .barrier()
          .group()
            .by(select("friend"))
          .as("friendForums")
          .select("friendAgg")
          .unfold()
          .as("friend")
          .in("hasCreator")
          .as("post")
          .in("containerOf")
          .as("forum")
          .filter(t -> {
                    Map<Vertex, List<Vertex>> m = t.path("friendForums");
                    Vertex v = t.path("friend");
                    List<Vertex> friendForums = m.get(v);
                    Vertex thisForum = t.get();
                    if (friendForums == null)
                      return false;
                    else
                      return friendForums.contains(thisForum);
                })
          .groupCount()
          .map(t -> {
                  Map<Object, Long> m = t.get();
                  for (Vertex v : forums) {
                    if (!m.containsKey((Object)v))
                      m.put(v, 0L);
                  }
                  return t.get();
              })
          .order(local)
            .by(select(values), decr)
            .by(select(keys).id(), incr)
          .limit(local, limit)
          .unfold()
          .project("forumTitle", "postCount")
            .by(select(keys).values("title"))
            .by(select(values))
          .map(t -> new LdbcQuery5Result(
              (String)(t.get().get("forumTitle")), 
              ((Long)(t.get().get("postCount"))).intValue()))
          .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery6Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          result.add(new LdbcQuery6Result(
              null,
              0));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }

      // Parameters of this query
      final long personId = operation.personId();
      final String tagName = operation.tagName();
      final int limit = operation.limit();

      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery6Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result).V(torcPersonId).as("person")
          .out("knows")
          .union(identity(), out("knows")).dedup().where(neq("person"))
          .as("friend")
          .in("hasCreator")
          .hasLabel("Post")
          .as("post")
          .out("hasTag")
          .values("name")
          .as("tag")
          .group()
            .by(select("post"))
          .as("postToTagMap")
          .flatMap(t -> {
                  Map m = t.get();
                  List removeList = new ArrayList<Object>();
                  for (Object k : m.keySet()) {
                    List v = (List) m.get(k);
                    if ( !v.contains(tagName) )
                      removeList.add(k);
                  }

                  for (Object k : removeList)
                    m.remove(k);

                  return m.entrySet().iterator();
                })
          .select(values).unfold()
          .where(is(neq(tagName)))
          .groupCount()
          .order(local)
            .by(select(values), decr)
            .by(select(keys), incr)
          .limit(local, limit)
          .unfold()
          .project("tagName", "postCount")
            .by(select(keys))
            .by(select(values))
          .map(t -> new LdbcQuery6Result(
              (String)(((Traverser<Map>)t).get().get("tagName")), 
              ((Long)(((Traverser<Map>)t).get().get("postCount"))).intValue()))
          .store("result").iterate(); 


        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery7Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
          int n2 = ThreadLocalRandom.current().nextInt(0, messageIDs.size());
          Long pid = personIDs.get(n1);
          Long mid = messageIDs.get(n2);
          result.add(new LdbcQuery7Result(
              pid,
              null,
              null,
              0,
              mid,
              null,
              0,
              false));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }
      
      // Parameters of this query
      final long personId = operation.personId();
      final int limit = operation.limit();
      
      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery7Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result).V(torcPersonId).as("person")
            .in("hasCreator").as("message")
            .inE("likes").as("like")
            .outV().as("liker")
            .order()
                .by(select("like").values("creationDate"), decr)
                .by(select("message").id(), incr)
            .dedup()
                .by(select("liker"))
            .limit(limit)
            .project("personId", 
                "personFirstName", 
                "personLastName", 
                "likeCreationDate", 
                "commentOrPostId",
                "commentOrPostContent",
                "commentOrPostCreationDate",
                "isNew") 
                .by(select("liker").id())
                .by(select("liker").values("firstName"))
                .by(select("liker").values("lastName"))
                .by(select("like").values("creationDate")
                    .map(t -> Long.valueOf((String)t.get())))
                .by(select("message").id())
                .by(select("message")
                    .choose(values("content").is(neq("")),
                        values("content"),
                        values("imageFile")))
                .by(select("message").values("creationDate")
                    .map(t -> Long.valueOf((String)t.get())))
                .by(choose(
                    where(select("person").out("knows").as("liker")),
                    constant(false),
                    constant(true)))
            .map(t -> new LdbcQuery7Result(
                ((UInt128)t.get().get("personId")).getLowerLong(),
                (String)t.get().get("personFirstName"), 
                (String)t.get().get("personLastName"),
                (Long)t.get().get("likeCreationDate"),
                ((UInt128)t.get().get("commentOrPostId")).getLowerLong(), 
                (String)t.get().get("commentOrPostContent"),
                (int)(((Long)t.get().get("likeCreationDate") 
                    - (Long)t.get().get("commentOrPostCreationDate")) 
                    / (1000l * 60l)),
                (Boolean)t.get().get("isNew")))
            .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery8Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
          int n2 = ThreadLocalRandom.current().nextInt(0, messageIDs.size());
          Long pid = personIDs.get(n1);
          Long mid = messageIDs.get(n2);
          result.add(new LdbcQuery8Result(
              pid,
              null,
              null,
              0,
              mid,
              null));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }
      
      // Parameters of this query
      final long personId = operation.personId();
      final int limit = operation.limit();
      
      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery8Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result).V(torcPersonId).as("person")
            .in("hasCreator").as("message")
            .in("replyOf").as("comment")
            .order()
                .by(select("comment").values("creationDate"), decr)
                .by(select("comment").id(), incr)
            .limit(limit)
            .out("hasCreator").as("commenter")
            .project("personId", 
                "personFirstName", 
                "personLastName", 
                "commentCreationDate", 
                "commentId",
                "commentContent")
                .by(select("commenter").id())
                .by(select("commenter").values("firstName"))
                .by(select("commenter").values("lastName"))
                .by(select("comment").values("creationDate"))
                .by(select("comment").id())
                .by(select("comment")
                    .choose(values("content").is(neq("")),
                        values("content"),
                        values("imageFile")))
            .map(t -> new LdbcQuery8Result(
                ((UInt128)t.get().get("personId")).getLowerLong(),
                (String)t.get().get("personFirstName"), 
                (String)t.get().get("personLastName"),
                Long.valueOf((String)t.get().get("commentCreationDate")),
                ((UInt128)t.get().get("commentId")).getLowerLong(), 
                (String)t.get().get("commentContent")))
            .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery9Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
          int n2 = ThreadLocalRandom.current().nextInt(0, messageIDs.size());
          Long pid = personIDs.get(n1);
          Long mid = messageIDs.get(n2);
          result.add(new LdbcQuery9Result(
              pid,
              null,
              null,
              mid,
              null,
              0));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }

      // Parameters of this query
      final long personId = operation.personId();
      final long maxDate = operation.maxDate().getTime();
      final int limit = operation.limit();
      
      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery9Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result).V(torcPersonId).as("person")
          .out("knows")
          .union(identity(), out("knows")).dedup().where(neq("person"))
          .as("friend")
          .in("hasCreator")
          .as("commentOrPost")
          .values("creationDate")
          .map(t -> Long.valueOf((String)t.get()))
          .as("creationDate")
          .where(is(lt(maxDate)))
          .order()
            .by(decr)
            .by(select("friend").id(), incr)
          .limit(limit)
          .project("personId", 
              "personFirstName", 
              "personLastName", 
              "commentOrPostId",
              "commentOrPostContent",
              "commentOrPostCreationDate")
              .by(select("friend").id())
              .by(select("friend").values("firstName"))
              .by(select("friend").values("lastName"))
              .by(select("commentOrPost").id())
              .by(select("commentOrPost")
                  .choose(values("content").is(neq("")),
                      values("content"),
                      values("imageFile")))
              .by(select("creationDate"))
          .map(t -> new LdbcQuery9Result(
              ((UInt128)t.get().get("personId")).getLowerLong(),
              (String)t.get().get("personFirstName"), 
              (String)t.get().get("personLastName"),
              ((UInt128)t.get().get("commentOrPostId")).getLowerLong(), 
              (String)t.get().get("commentOrPostContent"),
              (Long)t.get().get("commentOrPostCreationDate")))
          .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery10Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
          Long pid = personIDs.get(n1);
          result.add(new LdbcQuery10Result(
              pid,
              null,
              null,
              0,
              null,
              null));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }
      
      // Parameters of this query
      final long personId = operation.personId();
      final int month = operation.month() - 1; // make month zero based
      final int limit = operation.limit();

      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<Map<UInt128, Long>> postCountMap = new ArrayList<>();
        List<Map<UInt128, Long>> commonPostCountMap = new ArrayList<>();
        List<UInt128> friendIds = new ArrayList<>();

        g.withSideEffect("postCountMap", postCountMap)
            .withSideEffect("commonPostCountMap", commonPostCountMap)
            .withSideEffect("friendIds", friendIds)
            .withStrategies(TorcGraphProviderOptimizationStrategy.instance())
            .V(torcPersonId).as("person")
            .aggregate("done")
            .out("hasInterest").hasLabel("Tag")
            .aggregate("personInterests")
            .select("person").out("knows").hasLabel("Person")
            .aggregate("done")
            .out("knows").hasLabel("Person").where(without("done")).dedup()
            .filter(t -> {
                calendar.setTimeInMillis(
                    Long.valueOf(t.get().value("birthday")));
                int bmonth = calendar.get(Calendar.MONTH); // zero based 
                int bday = calendar.get(Calendar.DAY_OF_MONTH); // starts with 1
                if ((bmonth == month && bday >= 21) || 
                  (bmonth == ((month + 1) % 12) && bday < 22)) {
                  return true;
                }
                return false;
            }).as("friend2")
            .sideEffect(id().store("friendIds"))
            .in("hasCreator").hasLabel("Post").as("posts")
            .union(
                groupCount().by(select("friend2").id()).store("postCountMap"),
                out("hasTag").hasLabel("Tag").where(within("personInterests")).select("posts").dedup().groupCount().by(select("friend2").id()).store("commonPostCountMap")
                )
            .iterate();

        Map<UInt128, Long> totalMap = postCountMap.get(0);
        Map<UInt128, Long> commonMap = commonPostCountMap.get(0);
        Map<UInt128, Long> scoreMap = new HashMap<>();

        for (Map.Entry<UInt128, Long> entry : totalMap.entrySet()) {
          UInt128 id = entry.getKey();
          Long totalPosts = entry.getValue();

          Long commonPosts = 0l;
          if (commonMap.containsKey(id)) {
            commonPosts = commonMap.get(id);
          }

          Long commonInterestScore = 2*commonPosts - totalPosts;

          scoreMap.put(id, commonInterestScore);
        }

        for (UInt128 friendId : friendIds) {
          if (!scoreMap.containsKey(friendId)) {
            scoreMap.put(friendId, 0l);
          }
        }

        List<Map.Entry<UInt128, Long>> scoreList =
            new LinkedList<>(scoreMap.entrySet());

        Collections.sort(scoreList, 
            new Comparator<Map.Entry<UInt128, Long>>() {
              public int compare( Map.Entry<UInt128, Long> o1, 
                  Map.Entry<UInt128, Long> o2 ) {
                if ((o1.getValue()).compareTo(o2.getValue()) != 0) {
                  return -1*(o1.getValue()).compareTo(o2.getValue());
                } else {
                  return (o1.getKey()).compareTo(o2.getKey());
                }
              }
            } 
        );

        List<UInt128> topFriends = new ArrayList<>();

        for (int i = 0; i < limit; i++) {
          topFriends.add(scoreList.get(i).getKey());
        }

        List<LdbcQuery10Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result)
            .V(topFriends.toArray())
            .project("personId", 
                "personFirstName", 
                "personLastName", 
                "personGender",
                "personCityName")
                .by(id())
                .by(values("firstName"))
                .by(values("lastName"))
                .by(values("gender"))
                .by(out("isLocatedIn").values("name"))
            .map(t -> new LdbcQuery10Result(
                ((UInt128)t.get().get("personId")).getLowerLong(),
                (String)t.get().get("personFirstName"), 
                (String)t.get().get("personLastName"),
                scoreMap.get(t.get().get("personId")).intValue(),
                (String)t.get().get("personGender"), 
                (String)t.get().get("personCityName")))
            .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery11Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
          Long pid = personIDs.get(n1);
          result.add(new LdbcQuery11Result(
              pid,
              null,
              null,
              null,
              0));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }
      
      // Parameters of this query
      final long personId = operation.personId();
      final String countryName = operation.countryName();
      final int workFromYear = operation.workFromYear();
      final int limit = operation.limit();

      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery11Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result).V(torcPersonId).as("person")
            .aggregate("done")
            .union(
                out("knows"),
                out("knows").out("knows"))
            .dedup().where(without("done")).as("friend")
            .outE("workAt").has("workFrom", lt(String.valueOf(workFromYear)))
            .as("workAt")
            .inV().as("company")
            .out("isLocatedIn").has("name", countryName)
            .order()
                .by(select("workAt").values("workFrom"), incr)
                .by(select("friend").id())
                .by(select("company").values("name"), decr)
            .limit(limit)
            .project("personId", 
                "personFirstName", 
                "personLastName", 
                "organizationName", 
                "organizationWorkFromYear")
                .by(select("friend").id())
                .by(select("friend").values("firstName"))
                .by(select("friend").values("lastName"))
                .by(select("company").values("name"))
                .by(select("workAt").values("workFrom"))
            .map(t -> new LdbcQuery11Result(
                ((UInt128)t.get().get("personId")).getLowerLong(),
                (String)t.get().get("personFirstName"), 
                (String)t.get().get("personLastName"),
                (String)t.get().get("organizationName"),
                Integer.valueOf((String)t.get().get("organizationWorkFromYear"))))
            .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        List<LdbcQuery12Result> result = new ArrayList<>(operation.limit());

        for (int i = 0; i < operation.limit(); i++) {
          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
          Long pid = personIDs.get(n1);
          result.add(new LdbcQuery12Result(
              pid,
              null,
              null,
              null,
              0));
        }

        resultReporter.report(result.size(), result, operation);
        return;
      }

      // Parameters of this query
      final long personId = operation.personId();
      final String tagClassName = operation.tagClassName();
      final int limit = operation.limit();

      final UInt128 torcPersonId = 
          new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery12Result> result = new ArrayList<>(limit);

        g.withSideEffect("result", result).V(torcPersonId).as("person")
          .out("knows").as("friend")
          .in("hasCreator").as("comment")
          .hasLabel("Comment")
          .out("replyOf")
          .hasLabel("Post")
          .out("hasTag")
          .where(repeat(out("hasType")).until(values("name").is(eq(tagClassName))))
          .values("name")
          .group()
            .by(select("friend"))
            .by(group()
                  .by(select("comment"))
                  .by(dedup().fold()))
          .unfold()
          .project("personId", 
              "personFirstName",
              "personLastName",
              "tags", 
              "count")
            .by(select(keys).id())
            .by(select(keys).values("firstName"))
            .by(select(keys).values("lastName"))
            .by(select(values).select(values).unfold().dedup())
            .by(select(values).count(local))
          .map(t -> new LdbcQuery12Result(
              ((UInt128)t.get().get("personId")).getLowerLong(),
              (String)t.get().get("personFirstName"), 
              (String)t.get().get("personLastName"),
              (Iterable<String>)t.get().get("tags"), 
              ((Long)t.get().get("count")).intValue()))
          .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      if (fakeComplexReads) {
        resultReporter.report(1, new LdbcQuery13Result(0), operation);
        return;
      }
      
      // Parameters of this query
      final long person1Id = operation.person1Id();
      final long person2Id = operation.person2Id();

      if (person1Id == person2Id) {
        resultReporter.report(1, new LdbcQuery13Result(0), operation);
        return;        
      }

      final UInt128 torcPerson1Id = 
          new UInt128(TorcEntity.PERSON.idSpace, person1Id);
      final UInt128 torcPerson2Id = 
          new UInt128(TorcEntity.PERSON.idSpace, person2Id);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        Long pathLength = g.V(torcPerson1Id)
            .choose(where(out("knows")),
                repeat(out("knows").simplePath())
                    .until(hasId(torcPerson2Id)
                        .or()
                        .path().count(local).is(gt(5)))
                .limit(1)
                .choose(id().is(eq(torcPerson2Id)), 
                    union(path().count(local), constant(-1l)).sum(),
                    constant(-1l)),
                constant(-1l))
            .next();

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(1, new LdbcQuery13Result(pathLength.intValue()), 
            operation);
        break;
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

    final static Logger logger =
        LoggerFactory.getLogger(LdbcQuery14Handler.class);

    @Override
    public void executeOperation(final LdbcQuery14 operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      if (fakeComplexReads) {
        List<LdbcQuery14Result> result = new ArrayList<>(1);
        
        List<Long> personIDsInPath = new ArrayList<>(2);
        personIDsInPath.add(operation.person1Id());
        personIDsInPath.add(operation.person2Id());

        result.add(new LdbcQuery14Result(
            personIDsInPath,
            42.0));

        resultReporter.report(result.size(), result, operation);
        return;
      }

      // Parameters of this query
      final long person1Id = operation.person1Id();
      final long person2Id = operation.person2Id();

      final UInt128 torcPerson1Id = 
          new UInt128(TorcEntity.PERSON.idSpace, person1Id);
      final UInt128 torcPerson2Id = 
          new UInt128(TorcEntity.PERSON.idSpace, person2Id);

      Graph graph = ((TorcDbConnectionState) dbConnectionState).getClient();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        GraphTraversalSource g = graph.traversal();

        List<LdbcQuery14Result> result = new ArrayList<>();

        // First get the length of the shortest path
        Long minPathLen = g.V(torcPerson1Id)
          .repeat(outE("knows").inV().simplePath())
            .until(hasId(torcPerson2Id))
          .limit(1)
          .path()
          .count(local)
          .next();

        g.withSideEffect("result", result).V(torcPerson1Id)
          .repeat(outE("knows").as("e").inV().simplePath())
            .until(hasId(torcPerson2Id).or().path().count(local).is(eq(minPathLen)))
          .where(id().is(eq(torcPerson2Id)))
          .select(all, "e")
          .sideEffect(aggregate("paths"))
          .unfold()
          .dedup()
          .as("edge")
          .map(
            union(
              match(
                as("i").outV().as("outV"),
                as("i").inV().as("inV"),
                as("outV").in("hasCreator").hasLabel("Comment").as("cP").out("replyOf").hasLabel("Post").out("hasCreator").as("inV")
              ).select("outV", "cP", "inV").map(t -> 1.0f),
              match(
                as("i").outV().as("outV"),
                as("i").inV().as("inV"),
                as("outV").in("hasCreator").hasLabel("Comment").as("cC").out("replyOf").hasLabel("Comment").out("hasCreator").as("inV")
              ).select("outV", "cC", "inV").map(t -> 0.5f),
              match(
                as("i").outV().as("outV"),
                as("i").inV().as("inV"),
                as("inV").in("hasCreator").hasLabel("Comment").as("cP").out("replyOf").hasLabel("Post").out("hasCreator").as("outV")
              ).select("inV", "cP", "outV").map(t -> 1.0f),
              match(
                as("i").outV().as("outV"),
                as("i").inV().as("inV"),
                as("inV").in("hasCreator").hasLabel("Comment").as("cC").out("replyOf").hasLabel("Comment").out("hasCreator").as("outV")
              ).select("inV", "cC", "outV").map(t -> 0.5f)
            ).sum()
          )
          .as("score")
          .group()
            .by(select("edge"))
          .as("scoreMap")
          .select("paths")
          .unfold()
          .as("path")
          .unfold()
          .group()
            .by(select("path").map(t -> {
                                      List<TorcEdge> eList = (List)t.get();
                                      List<Number> personIdsList = new ArrayList<>(eList.size()+1);
                                      for (int i = 0; i < eList.size(); i++) {
                                        personIdsList.add(eList.get(i).getV1Id().getLowerLong());
                                      }
                                      personIdsList.add(eList.get(eList.size()-1).getV2Id().getLowerLong());
                                      return personIdsList;
                                    }))
            .by(map(t -> {
                      Map<TorcEdge, List<Number>> m = t.path("scoreMap");
                      return m.get(t.get()).get(0).doubleValue();
                }).sum())
          .unfold()
          .order(local)
            .by(select(values))
          .project("personIdsInPath", 
              "pathWeight")
              .by(select(keys))
              .by(select(values))
          .map(t -> new LdbcQuery14Result(
              (Iterable<Number>)t.get().get("personIdsInPath"), 
              ((Double)t.get().get("pathWeight"))))
          .store("result").iterate(); 

        if (doTransactionalReads) {
          try {
            graph.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          graph.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        long person_id = operation.personId();
        Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

        Vertex person = client.vertices(
            new UInt128(TorcEntity.PERSON.idSpace, person_id)).next();
        Iterator<VertexProperty<String>> props = person.properties();
        Map<String, String> propertyMap = new HashMap<>();
        props.forEachRemaining((prop) -> {
          propertyMap.put(prop.key(), prop.value());
        });

        Vertex place =
            ((TorcVertex) person).edges(Direction.OUT, 
              new String[] {"isLocatedIn"}, 
              new String[] {TorcEntity.PLACE.label}).next().inVertex();
        long placeId = ((UInt128) place.id()).getLowerLong();

        LdbcShortQuery1PersonProfileResult res =
            new LdbcShortQuery1PersonProfileResult(
                propertyMap.get("firstName"),
                propertyMap.get("lastName"),
                Long.parseLong(propertyMap.get("birthday")),
                propertyMap.get("locationIP"),
                propertyMap.get("browserUsed"),
                placeId,
                propertyMap.get("gender"),
                Long.parseLong(propertyMap.get("creationDate")));

        if (doTransactionalReads) {
          try {
            client.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          client.tx().rollback();
        }

        resultReporter.report(0, res, operation);
        break;
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

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery2PersonPostsHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery2PersonPosts operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

        List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>();

        Vertex person = client.vertices(
            new UInt128(TorcEntity.PERSON.idSpace, operation.personId()))
            .next();
        Iterator<Edge> edges = ((TorcVertex) person).edges(Direction.IN, 
            new String[] {"hasCreator"}, 
            new String[] {TorcEntity.POST.label, TorcEntity.COMMENT.label});

        List<Vertex> messageList = new ArrayList<>();
        edges.forEachRemaining((e) -> messageList.add(e.outVertex()));
        messageList.sort((a, b) -> {
          Vertex v1 = (Vertex) a;
          Vertex v2 = (Vertex) b;

          long v1Date =
              Long.decode(v1.<String>property("creationDate").value());
          long v2Date =
              Long.decode(v2.<String>property("creationDate").value());

          if (v1Date > v2Date) {
            return -1;
          } else if (v1Date < v2Date) {
            return 1;
          } else {
            long v1Id = ((UInt128) v1.id()).getLowerLong();
            long v2Id = ((UInt128) v2.id()).getLowerLong();
            if (v1Id > v2Id) {
              return -1;
            } else if (v1Id < v2Id) {
              return 1;
            } else {
              return 0;
            }
          }
        });

        for (int i = 0; i < Integer.min(operation.limit(), messageList.size());
            i++) {
          Vertex message = messageList.get(i);

          Map<String, String> propMap = new HashMap<>();
          message.<String>properties().forEachRemaining((vp) -> {
            propMap.put(vp.key(), vp.value());
          });

          long messageId = ((UInt128) message.id()).getLowerLong();

          String messageContent;
          if (propMap.get("content").length() != 0) {
            messageContent = propMap.get("content");
          } else {
            messageContent = propMap.get("imageFile");
          }

          long messageCreationDate = Long.decode(propMap.get("creationDate"));

          long originalPostId;
          long originalPostAuthorId;
          String originalPostAuthorFirstName;
          String originalPostAuthorLastName;
          if (message.label().equals(TorcEntity.POST.label)) {
            originalPostId = messageId;
            originalPostAuthorId = ((UInt128) person.id()).getLowerLong();
            originalPostAuthorFirstName =
                person.<String>property("firstName").value();
            originalPostAuthorLastName =
                person.<String>property("lastName").value();
          } else {
            Vertex parentMessage =
                ((TorcVertex) message).edges(Direction.OUT, 
                  new String[] {"replyOf"},
                  new String[] {TorcEntity.POST.label, 
                    TorcEntity.COMMENT.label})
                  .next().inVertex();
            while (true) {
              if (parentMessage.label().equals(TorcEntity.POST.label)) {
                originalPostId = ((UInt128) parentMessage.id()).getLowerLong();

                Vertex author =
                    ((TorcVertex) parentMessage).edges(Direction.OUT, 
                      new String[] {"hasCreator"}, 
                      new String[] {TorcEntity.PERSON.label})
                    .next().inVertex();
                originalPostAuthorId = ((UInt128) author.id()).getLowerLong();
                originalPostAuthorFirstName =
                    author.<String>property("firstName").value();
                originalPostAuthorLastName =
                    author.<String>property("lastName").value();
                break;
              } else {
                parentMessage =
                    ((TorcVertex) parentMessage).edges(Direction.OUT, 
                      new String[] {"replyOf"},
                      new String[] {TorcEntity.POST.label, 
                        TorcEntity.COMMENT.label})
                    .next().inVertex();
              }
            }
          }

          LdbcShortQuery2PersonPostsResult res =
              new LdbcShortQuery2PersonPostsResult(
                  messageId,
                  messageContent,
                  messageCreationDate,
                  originalPostId,
                  originalPostAuthorId,
                  originalPostAuthorFirstName,
                  originalPostAuthorLastName);

          result.add(res);
        }

        if (doTransactionalReads) {
          try {
            client.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          client.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

        List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();

        Vertex person = client.vertices(
            new UInt128(TorcEntity.PERSON.idSpace, operation.personId()))
            .next();

        Iterator<Edge> edges = ((TorcVertex) person).edges(Direction.OUT, 
            new String[] {"knows"}, 
            new String[] {TorcEntity.PERSON.label});

        edges.forEachRemaining((e) -> {
          long creationDate = Long.decode(e.<String>property("creationDate")
              .value());

          Vertex friend = e.inVertex();

          long personId = ((UInt128) friend.id()).getLowerLong();

          String firstName = friend.<String>property("firstName").value();
          String lastName = friend.<String>property("lastName").value();

          LdbcShortQuery3PersonFriendsResult res =
              new LdbcShortQuery3PersonFriendsResult(
                  personId,
                  firstName,
                  lastName,
                  creationDate);
          result.add(res);
        });

        // Sort the result here.
        result.sort((a, b) -> {
          LdbcShortQuery3PersonFriendsResult r1 =
              (LdbcShortQuery3PersonFriendsResult) a;
          LdbcShortQuery3PersonFriendsResult r2 =
              (LdbcShortQuery3PersonFriendsResult) b;

          if (r1.friendshipCreationDate() > r2.friendshipCreationDate()) {
            return -1;
          } else if (r1.friendshipCreationDate()
              < r2.friendshipCreationDate()) {
            return 1;
          } else {
            if (r1.personId() > r2.personId()) {
              return 1;
            } else if (r1.personId() < r2.personId()) {
              return -1;
            } else {
              return 0;
            }
          }
        });

        if (doTransactionalReads) {
          try {
            client.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          client.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }

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
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

        Vertex message = client.vertices(
            new UInt128(TorcEntity.COMMENT.idSpace, operation.messageId()))
            .next();

        long creationDate =
            Long.decode(message.<String>property("creationDate").value());
        String content = message.<String>property("content").value();
        if (content.length() == 0) {
          content = message.<String>property("imageFile").value();
        }

        LdbcShortQuery4MessageContentResult result =
            new LdbcShortQuery4MessageContentResult(
                content,
                creationDate);

        if (doTransactionalReads) {
          try {
            client.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          client.tx().rollback();
        }

        resultReporter.report(1, result, operation);
        break;
      }

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
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

        Vertex message = client.vertices(
            new UInt128(TorcEntity.COMMENT.idSpace, operation.messageId()))
            .next();

        Vertex creator =
            ((TorcVertex) message).edges(Direction.OUT, 
                new String[] {"hasCreator"}, 
                new String[] {TorcEntity.PERSON.label}).next().inVertex();

        long creatorId = ((UInt128) creator.id()).getLowerLong();

        String creatorFirstName =
            creator.<String>property("firstName").value();
        String creatorLastName =
            creator.<String>property("lastName").value();

        LdbcShortQuery5MessageCreatorResult result =
            new LdbcShortQuery5MessageCreatorResult(
                creatorId,
                creatorFirstName,
                creatorLastName);

        if (doTransactionalReads) {
          try {
            client.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          client.tx().rollback();
        }

        resultReporter.report(1, result, operation);
        break;
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

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery6MessageForumHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery6MessageForum operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

        Vertex vertex = client.vertices(
            new UInt128(TorcEntity.COMMENT.idSpace, operation.messageId()))
            .next();

        LdbcShortQuery6MessageForumResult result;
        while (true) {
          if (vertex.label().equals(TorcEntity.FORUM.label)) {
            long forumId = ((UInt128) vertex.id()).getLowerLong();
            String forumTitle = vertex.<String>property("title").value();

            Vertex moderator =
                ((TorcVertex) vertex).edges(Direction.OUT, 
                  new String[] {"hasModerator"},
                  new String[] {TorcEntity.PERSON.label}).next().inVertex();

            long moderatorId = ((UInt128) moderator.id()).getLowerLong();
            String moderatorFirstName =
                moderator.<String>property("firstName").value();
            String moderatorLastName =
                moderator.<String>property("lastName").value();

            result = new LdbcShortQuery6MessageForumResult(
                forumId,
                forumTitle,
                moderatorId,
                moderatorFirstName,
                moderatorLastName);

            break;
          } else if (vertex.label().equals(TorcEntity.POST.label)) {
            vertex =
                ((TorcVertex) vertex).edges(Direction.IN, 
                  new String[] {"containerOf"},
                  new String[] {TorcEntity.FORUM.label}).next().outVertex();
          } else {
            vertex = ((TorcVertex) vertex).edges(Direction.OUT, 
                  new String[] {"replyOf"},
                  new String[] {TorcEntity.POST.label, 
                    TorcEntity.COMMENT.label})
                  .next().inVertex();
          }
        }

        if (doTransactionalReads) {
          try {
            client.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          client.tx().rollback();
        }

        resultReporter.report(1, result, operation);
        break;
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

    final static Logger logger =
        LoggerFactory.getLogger(LdbcShortQuery7MessageRepliesHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery7MessageReplies operation,
        DbConnectionState dbConnectionState,
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

        Vertex message = client.vertices(
            new UInt128(TorcEntity.COMMENT.idSpace, operation.messageId()))
            .next();
        Vertex messageAuthor =
            ((TorcVertex) message).edges(Direction.OUT, 
              new String[] {"hasCreator"},
              new String[] {TorcEntity.PERSON.label}).next().inVertex();
        long messageAuthorId = ((UInt128) messageAuthor.id()).getLowerLong();

        List<Vertex> replies = new ArrayList<>();
        ((TorcVertex) message).edges(Direction.IN, 
          new String[] {"replyOf"},
          new String[] {TorcEntity.COMMENT.label}).forEachRemaining((e) -> {
          replies.add(e.outVertex());
        });

        List<Long> messageAuthorFriendIds = new ArrayList<>();
        ((TorcVertex) messageAuthor).edges(Direction.OUT, 
          new String[] {"knows"},
          new String[] {TorcEntity.PERSON.label}).forEachRemaining((e) -> {
          messageAuthorFriendIds.add(((UInt128) e.inVertex().id())
              .getLowerLong());
        });

        List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();

        for (Vertex reply : replies) {
          long replyId = ((UInt128) reply.id()).getLowerLong();
          String replyContent = reply.<String>property("content").value();
          long replyCreationDate =
              Long.decode(reply.<String>property("creationDate").value());

          Vertex replyAuthor =
              ((TorcVertex) reply).edges(Direction.OUT, 
                  new String[] {"hasCreator"},
                  new String[] {TorcEntity.PERSON.label}).next().inVertex();
          long replyAuthorId = ((UInt128) replyAuthor.id()).getLowerLong();
          String replyAuthorFirstName =
              replyAuthor.<String>property("firstName").value();
          String replyAuthorLastName =
              replyAuthor.<String>property("lastName").value();

          boolean knows = false;
          if (messageAuthorId != replyAuthorId) {
            knows = messageAuthorFriendIds.contains(replyAuthorId);
          }

          LdbcShortQuery7MessageRepliesResult res =
              new LdbcShortQuery7MessageRepliesResult(
                  replyId,
                  replyContent,
                  replyCreationDate,
                  replyAuthorId,
                  replyAuthorFirstName,
                  replyAuthorLastName,
                  knows
              );

          result.add(res);
        }

        // Sort the result here.
        result.sort((a, b) -> {
          LdbcShortQuery7MessageRepliesResult r1 =
              (LdbcShortQuery7MessageRepliesResult) a;
          LdbcShortQuery7MessageRepliesResult r2 =
              (LdbcShortQuery7MessageRepliesResult) b;

          if (r1.commentCreationDate() > r2.commentCreationDate()) {
            return -1;
          } else if (r1.commentCreationDate() < r2.commentCreationDate()) {
            return 1;
          } else {
            if (r1.replyAuthorId() > r2.replyAuthorId()) {
              return 1;
            } else if (r1.replyAuthorId() < r2.replyAuthorId()) {
              return -1;
            } else {
              return 0;
            }
          }
        });

        if (doTransactionalReads) {
          try {
            client.tx().commit();
          } catch (RuntimeException e) {
            txAttempts++;
            continue;
          }
        } else {
          client.tx().rollback();
        }

        resultReporter.report(result.size(), result, operation);
        break;
      }
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

    private final Calendar calendar;

    public LdbcUpdate1AddPersonHandler() {
      this.calendar = new GregorianCalendar();
    }

    @Override
    public void executeOperation(LdbcUpdate1AddPerson operation,
        DbConnectionState dbConnectionState,
        ResultReporter reporter) throws DbException {
      Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

      // Build key value properties array
      List<Object> personKeyValues =
          new ArrayList<>(18 + 2 * operation.languages().size()
              + 2 * operation.emails().size());
      personKeyValues.add(T.id);
      personKeyValues.add(
          new UInt128(TorcEntity.PERSON.idSpace, operation.personId()));
      personKeyValues.add(T.label);
      personKeyValues.add(TorcEntity.PERSON.label);
      personKeyValues.add("firstName");
      personKeyValues.add(operation.personFirstName());
      personKeyValues.add("lastName");
      personKeyValues.add(operation.personLastName());
      personKeyValues.add("gender");
      personKeyValues.add(operation.gender());
      personKeyValues.add("birthday");
      personKeyValues.add(String.valueOf(operation.birthday().getTime()));
      personKeyValues.add("creationDate");
      personKeyValues.add(String.valueOf(operation.creationDate().getTime()));
      personKeyValues.add("locationIP");
      personKeyValues.add(operation.locationIp());
      personKeyValues.add("browserUsed");
      personKeyValues.add(operation.browserUsed());

      for (String language : operation.languages()) {
        personKeyValues.add("language");
        personKeyValues.add(language);
      }

      for (String email : operation.emails()) {
        personKeyValues.add("email");
        personKeyValues.add(email);
      }

      boolean txSucceeded = false;
      int txFailCount = 0;
      do {
        // Add person
        Vertex person = client.addVertex(personKeyValues.toArray());

        // Add edge to place
        Vertex place = client.vertices(
            new UInt128(TorcEntity.PLACE.idSpace, operation.cityId())).next();
        person.addEdge("isLocatedIn", place);

        // Add edges to tags
        List<UInt128> tagIds = new ArrayList<>(operation.tagIds().size());
        operation.tagIds().forEach((id) ->
            tagIds.add(new UInt128(TorcEntity.TAG.idSpace, id)));
        Iterator<Vertex> tagVItr = client.vertices(tagIds.toArray());
        tagVItr.forEachRemaining((tag) -> {
          person.addEdge("hasInterest", tag);
        });

        // Add edges to universities
        List<Object> studiedAtKeyValues = new ArrayList<>(2);
        for (LdbcUpdate1AddPerson.Organization org : operation.studyAt()) {
          studiedAtKeyValues.clear();
          studiedAtKeyValues.add("classYear");
          studiedAtKeyValues.add(String.valueOf(org.year()));
          Vertex orgV = client.vertices(
              new UInt128(TorcEntity.ORGANISATION.idSpace,
                  org.organizationId()))
              .next();
          person.addEdge("studyAt", orgV, studiedAtKeyValues.toArray());
        }

        // Add edges to companies
        List<Object> workedAtKeyValues = new ArrayList<>(2);
        for (LdbcUpdate1AddPerson.Organization org : operation.workAt()) {
          workedAtKeyValues.clear();
          workedAtKeyValues.add("workFrom");
          workedAtKeyValues.add(String.valueOf(org.year()));
          Vertex orgV = client.vertices(
              new UInt128(TorcEntity.ORGANISATION.idSpace,
                  org.organizationId())).next();
          person.addEdge("workAt", orgV, workedAtKeyValues.toArray());
        }

        try {
          client.tx().commit();
          txSucceeded = true;
        } catch (Exception e) {
          txFailCount++;
        }

        if (txFailCount >= MAX_TX_ATTEMPTS) {
          throw new RuntimeException(String.format(
              "ERROR: Transaction failed %d times, aborting...",
              txFailCount));
        }
      } while (!txSucceeded);

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
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
        ResultReporter reporter) throws DbException {
      Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

      UInt128 personId =
          new UInt128(TorcEntity.PERSON.idSpace, operation.personId());
      UInt128 postId =
          new UInt128(TorcEntity.POST.idSpace, operation.postId());

      boolean txSucceeded = false;
      int txFailCount = 0;
      do {
        Iterator<Vertex> results = client.vertices(personId, postId);
        Vertex person = results.next();
        Vertex post = results.next();
        List<Object> keyValues = new ArrayList<>(2);
        keyValues.add("creationDate");
        keyValues.add(String.valueOf(operation.creationDate().getTime()));
        person.addEdge("likes", post, keyValues.toArray());

        try {
          client.tx().commit();
          txSucceeded = true;
        } catch (Exception e) {
          txFailCount++;
        }

        if (txFailCount >= MAX_TX_ATTEMPTS) {
          throw new RuntimeException(String.format(
              "ERROR: Transaction failed %d times, aborting...",
              txFailCount));
        }
      } while (!txSucceeded);

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
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
        ResultReporter reporter) throws DbException {
      Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

      UInt128 personId =
          new UInt128(TorcEntity.PERSON.idSpace, operation.personId());
      UInt128 commentId =
          new UInt128(TorcEntity.COMMENT.idSpace, operation.commentId());

      boolean txSucceeded = false;
      int txFailCount = 0;
      do {
        Iterator<Vertex> results = client.vertices(personId, commentId);
        Vertex person = results.next();
        Vertex comment = results.next();
        List<Object> keyValues = new ArrayList<>(2);
        keyValues.add("creationDate");
        keyValues.add(String.valueOf(operation.creationDate().getTime()));
        person.addEdge("likes", comment, keyValues.toArray());

        try {
          client.tx().commit();
          txSucceeded = true;
        } catch (Exception e) {
          txFailCount++;
        }

        if (txFailCount >= MAX_TX_ATTEMPTS) {
          throw new RuntimeException(String.format(
              "ERROR: Transaction failed %d times, aborting...",
              txFailCount));
        }
      } while (!txSucceeded);

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
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
        ResultReporter reporter) throws DbException {
      Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

      List<Object> forumKeyValues = new ArrayList<>(8);
      forumKeyValues.add(T.id);
      forumKeyValues.add(
          new UInt128(TorcEntity.FORUM.idSpace, operation.forumId()));
      forumKeyValues.add(T.label);
      forumKeyValues.add(TorcEntity.FORUM.label);
      forumKeyValues.add("title");
      forumKeyValues.add(operation.forumTitle());
      forumKeyValues.add("creationDate");
      forumKeyValues.add(String.valueOf(operation.creationDate().getTime()));

      boolean txSucceeded = false;
      int txFailCount = 0;
      do {
        Vertex forum = client.addVertex(forumKeyValues.toArray());

        List<UInt128> ids = new ArrayList<>(operation.tagIds().size() + 1);
        operation.tagIds().forEach((id) -> {
          ids.add(new UInt128(TorcEntity.TAG.idSpace, id));
        });
        ids.add(new UInt128(TorcEntity.PERSON.idSpace,
            operation.moderatorPersonId()));

        client.vertices(ids.toArray()).forEachRemaining((v) -> {
          if (v.label().equals(TorcEntity.TAG.label)) {
            forum.addEdge("hasTag", v);
          } else if (v.label().equals(TorcEntity.PERSON.label)) {
            forum.addEdge("hasModerator", v);
          } else {
            throw new RuntimeException(String.format(
                "ERROR: LdbcUpdate4AddForum query read a vertex with an "
                + "unexpected label: %s", v.label()));
          }
        });

        try {
          client.tx().commit();
          txSucceeded = true;
        } catch (Exception e) {
          txFailCount++;
        }

        if (txFailCount >= MAX_TX_ATTEMPTS) {
          throw new RuntimeException(String.format(
              "ERROR: Transaction failed %d times, aborting...",
              txFailCount));
        }
      } while (!txSucceeded);

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
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
        ResultReporter reporter) throws DbException {
      Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

      List<UInt128> ids = new ArrayList<>(2);
      ids.add(new UInt128(TorcEntity.FORUM.idSpace, operation.forumId()));
      ids.add(new UInt128(TorcEntity.PERSON.idSpace, operation.personId()));

      boolean txSucceeded = false;
      int txFailCount = 0;
      do {
        Iterator<Vertex> vItr = client.vertices(ids.toArray());
        Vertex forum = vItr.next();
        Vertex member = vItr.next();

        List<Object> edgeKeyValues = new ArrayList<>(2);
        edgeKeyValues.add("joinDate");
        edgeKeyValues.add(String.valueOf(operation.joinDate().getTime()));

        forum.addEdge("hasMember", member, edgeKeyValues.toArray());

        try {
          client.tx().commit();
          txSucceeded = true;
        } catch (Exception e) {
          txFailCount++;
        }

        if (txFailCount >= MAX_TX_ATTEMPTS) {
          throw new RuntimeException(String.format(
              "ERROR: Transaction failed %d times, aborting...",
              txFailCount));
        }
      } while (!txSucceeded);

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
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
        ResultReporter reporter) throws DbException {
      Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

      List<Object> postKeyValues = new ArrayList<>(18);
      postKeyValues.add(T.id);
      postKeyValues.add(
          new UInt128(TorcEntity.POST.idSpace, operation.postId()));
      postKeyValues.add(T.label);
      postKeyValues.add(TorcEntity.POST.label);
      postKeyValues.add("imageFile");
      postKeyValues.add(operation.imageFile());
      postKeyValues.add("creationDate");
      postKeyValues.add(String.valueOf(operation.creationDate().getTime()));
      postKeyValues.add("locationIP");
      postKeyValues.add(operation.locationIp());
      postKeyValues.add("browserUsed");
      postKeyValues.add(operation.browserUsed());
      postKeyValues.add("language");
      postKeyValues.add(operation.language());
      postKeyValues.add("content");
      postKeyValues.add(operation.content());
      postKeyValues.add("length");
      postKeyValues.add(String.valueOf(operation.length()));

      boolean txSucceeded = false;
      int txFailCount = 0;
      do {
        Vertex post = client.addVertex(postKeyValues.toArray());

        List<UInt128> ids = new ArrayList<>(2);
        ids.add(new UInt128(TorcEntity.PERSON.idSpace,
            operation.authorPersonId()));
        ids.add(new UInt128(TorcEntity.FORUM.idSpace, operation.forumId()));
        ids.add(new UInt128(TorcEntity.PLACE.idSpace, operation.countryId()));
        operation.tagIds().forEach((id) -> {
          ids.add(new UInt128(TorcEntity.TAG.idSpace, id));
        });

        client.vertices(ids.toArray()).forEachRemaining((v) -> {
          if (v.label().equals(TorcEntity.PERSON.label)) {
            post.addEdge("hasCreator", v);
          } else if (v.label().equals(TorcEntity.FORUM.label)) {
            v.addEdge("containerOf", post);
          } else if (v.label().equals(TorcEntity.PLACE.label)) {
            post.addEdge("isLocatedIn", v);
          } else if (v.label().equals(TorcEntity.TAG.label)) {
            post.addEdge("hasTag", v);
          } else {
            throw new RuntimeException(String.format(
                "ERROR: LdbcUpdate6AddPostHandler query read a vertex with an "
                + "unexpected label: %s", v.label()));
          }
        });

        try {
          client.tx().commit();
          txSucceeded = true;
        } catch (Exception e) {
          txFailCount++;
        }

        if (txFailCount >= MAX_TX_ATTEMPTS) {
          throw new RuntimeException(String.format(
              "ERROR: Transaction failed %d times, aborting...",
              txFailCount));
        }
      } while (!txSucceeded);

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
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
        ResultReporter reporter) throws DbException {
      Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

      List<Object> commentKeyValues = new ArrayList<>(14);
      commentKeyValues.add(T.id);
      commentKeyValues.add(
          new UInt128(TorcEntity.COMMENT.idSpace, operation.commentId()));
      commentKeyValues.add(T.label);
      commentKeyValues.add(TorcEntity.COMMENT.label);
      commentKeyValues.add("creationDate");
      commentKeyValues.add(String.valueOf(operation.creationDate().getTime()));
      commentKeyValues.add("locationIP");
      commentKeyValues.add(operation.locationIp());
      commentKeyValues.add("browserUsed");
      commentKeyValues.add(operation.browserUsed());
      commentKeyValues.add("content");
      commentKeyValues.add(operation.content());
      commentKeyValues.add("length");
      commentKeyValues.add(String.valueOf(operation.length()));

      boolean txSucceeded = false;
      int txFailCount = 0;
      do {
        Vertex comment = client.addVertex(commentKeyValues.toArray());

        List<UInt128> ids = new ArrayList<>(2);
        ids.add(new UInt128(TorcEntity.PERSON.idSpace,
            operation.authorPersonId()));
        ids.add(new UInt128(TorcEntity.PLACE.idSpace, operation.countryId()));
        operation.tagIds().forEach((id) -> {
          ids.add(new UInt128(TorcEntity.TAG.idSpace, id));
        });
        if (operation.replyToCommentId() != -1) {
          ids.add(new UInt128(TorcEntity.COMMENT.idSpace,
              operation.replyToCommentId()));
        }
        if (operation.replyToPostId() != -1) {
          ids.add(
              new UInt128(TorcEntity.POST.idSpace, operation.replyToPostId()));
        }

        client.vertices(ids.toArray()).forEachRemaining((v) -> {
          if (v.label().equals(TorcEntity.PERSON.label)) {
            comment.addEdge("hasCreator", v);
          } else if (v.label().equals(TorcEntity.PLACE.label)) {
            comment.addEdge("isLocatedIn", v);
          } else if (v.label().equals(TorcEntity.COMMENT.label)) {
            comment.addEdge("replyOf", v);
          } else if (v.label().equals(TorcEntity.POST.label)) {
            comment.addEdge("replyOf", v);
          } else if (v.label().equals(TorcEntity.TAG.label)) {
            comment.addEdge("hasTag", v);
          } else {
            throw new RuntimeException(String.format(
                "ERROR: LdbcUpdate7AddCommentHandler query read a vertex with "
                + "an unexpected label: %s, id: %s", v.label(), v.id()));
          }
        });

        try {
          client.tx().commit();
          txSucceeded = true;
        } catch (Exception e) {
          txFailCount++;
        }

        if (txFailCount >= MAX_TX_ATTEMPTS) {
          throw new RuntimeException(String.format(
              "ERROR: Transaction failed %d times, aborting...",
              txFailCount));
        }
      } while (!txSucceeded);

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
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
        ResultReporter reporter) throws DbException {
      Graph client = ((TorcDbConnectionState) dbConnectionState).getClient();

      List<Object> knowsEdgeKeyValues = new ArrayList<>(2);
      knowsEdgeKeyValues.add("creationDate");
      knowsEdgeKeyValues.add(
          String.valueOf(operation.creationDate().getTime()));

      List<UInt128> ids = new ArrayList<>(2);
      ids.add(new UInt128(TorcEntity.PERSON.idSpace, operation.person1Id()));
      ids.add(new UInt128(TorcEntity.PERSON.idSpace, operation.person2Id()));

      boolean txSucceeded = false;
      int txFailCount = 0;
      do {
        Iterator<Vertex> vItr = client.vertices(ids.toArray());

        Vertex person1 = vItr.next();
        Vertex person2 = vItr.next();

        person1.addEdge("knows", person2, knowsEdgeKeyValues.toArray());
        person2.addEdge("knows", person1, knowsEdgeKeyValues.toArray());

        try {
          client.tx().commit();
          txSucceeded = true;
        } catch (Exception e) {
          txFailCount++;
        }

        if (txFailCount >= MAX_TX_ATTEMPTS) {
          throw new RuntimeException(String.format(
              "ERROR: Transaction failed %d times, aborting...",
              txFailCount));
        }
      } while (!txSucceeded);

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }
}
