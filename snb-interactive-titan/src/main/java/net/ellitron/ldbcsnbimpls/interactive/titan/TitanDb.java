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
package net.ellitron.ldbcsnbimpls.interative.titan;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.id;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.incr;
import static org.apache.tinkerpop.gremlin.process.traversal.P.lt;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;

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

import com.thinkaurelius.titan.core.TitanFactory;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

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
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TitanDb extends Db {

  private BasicDbConnectionState connectionState = null;
  private static boolean doTransactionalReads = false;

  // Maximum number of times to try a transaction before giving up.
  private static int MAX_TX_ATTEMPTS = 100;

  static class BasicDbConnectionState extends DbConnectionState {

    private Graph client;

    private BasicDbConnectionState(Map<String, String> properties) {
      BaseConfiguration config = new BaseConfiguration();
      config.setDelimiterParsingDisabled(true);

      config.setProperty("storage.backend", "cassandra");
      config.setProperty("storage.hostname", 
          properties.get("cassandraLocator"));
      config.setProperty("storage.cassandra.keyspace", 
          properties.get("graphName"));

      client = TitanFactory.open(config);
    }

    public Graph client() {
      return client;
    }

    @Override
    public void close() throws IOException {
      try {
        client.close();
      } catch (Exception ex) {
        java.util.logging.Logger.getLogger(TitanDb.class.getName())
            .log(Level.SEVERE, null, ex);
      }
    }
  }

  /* Returns the original LDBC SNB assigned 64-bit ID of the given vertex (this
   * is not the ID that is assigned to the vertex by TitanDB during the data
   * loading phase).
   */
  private static Long getSNBId(Vertex v) {
    return Long.decode(v.<String>property("iid").value().split(":")[1]);
  }

  /* Return a String representing the globally unique Iid property on all
   * vertices in the graph. This Iid property is a function of both the Entity
   * type and the 64-bit LDBC SNB assigned ID to the node (which is only unique
   * across vertices of that type).
   */
  private static String makeIid(Entity type, long id) {
    return type.getName() + ":" + String.valueOf(id);
  }

  @Override
  protected void onInit(Map<String, String> properties, 
      LoggingService loggingService) throws DbException {
    registerOperationHandler(LdbcQuery1.class, LdbcQuery1Handler.class);
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

    connectionState = new BasicDbConnectionState(properties);
    if (properties.containsKey("txReads")) {
      doTransactionalReads = true;
    }
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
  public static class LdbcQuery1Handler 
        implements OperationHandler<LdbcQuery1, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery1Handler.class);

    @Override
    public void executeOperation(final LdbcQuery1 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      executeOperationGremlin2(operation, dbConnectionState, resultReporter);
    }

    public void executeOperationGremlin2(final LdbcQuery1 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      //            int NUMTIMERS = 1;
      //            long[][] timers = new long[NUMTIMERS][2];
      //            for (int i = 0; i < NUMTIMERS; i++) {
      //                timers[i][0] = 0;
      //                timers[i][1] = 0;
      //            }
      //            timers[NUMTIMERS-1][0] = System.nanoTime();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        long personId = operation.personId();
        String firstName = operation.firstName();
        int resultLimit = operation.limit();
        int maxLevels = 3;
        Graph graph = dbConnectionState.client();

        GraphTraversalSource g = graph.traversal();

        List<Long> distList = new ArrayList<>(resultLimit);
        List<Vertex> matchList = new ArrayList<>(resultLimit);

        Vertex root = g.V().has("iid", 
            makeIid(Entity.PERSON, personId)).next();

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
              propertiesMap.put((Vertex)map.get("person"), 
                  (Map<String,List<String>>)map.get("props"));
            });

        Map<Vertex, String> placeNameMap = new HashMap<>(matchList.size());
        g.V(matchList.toArray()).as("person")
            .out("isLocatedIn")
            .<String>values("name")
            .as("placeName")
            .select("person", "placeName")
            .forEachRemaining(map -> {
              placeNameMap.put((Vertex)map.get("person"), 
                  (String)map.get("placeName"));
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
          int distance = (i < distList.get(0)) ? 1 : 
              (i < distList.get(1)) ? 2 : 3;
          Map<String, List<String>> properties = propertiesMap.get(match);
          List<String> emails = properties.get("email");
          if (emails == null)
            emails = new ArrayList<>();
          List<String> languages = properties.get("language");
          if (languages == null)
            languages = new ArrayList<>();
          String placeName = placeNameMap.get(match);
          List<List<Object>> universityInfo = universityInfoMap.get(match);
          if (universityInfo == null)
            universityInfo = new ArrayList<>();
          List<List<Object>> companyInfo = companyInfoMap.get(match);
          if (companyInfo == null)
            companyInfo = new ArrayList<>();
          result.add(new LdbcQuery1Result(
                getSNBId(match),
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

      //            timers[NUMTIMERS-1][1] = System.nanoTime();
      //            for (int i = 0; i < NUMTIMERS; i++)
      //                System.out.println(String.format(
      //                    "LdbcQuery1: %d: time: %dus", i, 
      //                    (timers[i][1] - timers[i][0])/1000l));
    }

    public void executeOperationGremlin1(final LdbcQuery1 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      //            int NUMTIMERS = 1;
      //            long[][] timers = new long[NUMTIMERS][2];
      //            for (int i = 0; i < NUMTIMERS; i++) {
      //                timers[i][0] = 0;
      //                timers[i][1] = 0;
      //            }
      //            timers[NUMTIMERS-1][0] = System.nanoTime();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        long personId = operation.personId();
        String firstName = operation.firstName();
        int resultLimit = operation.limit();
        int maxLevels = 3;
        Graph graph = dbConnectionState.client();

        GraphTraversalSource g = graph.traversal();

        List<Integer> distList = new ArrayList<>(resultLimit);
        List<Vertex> matchList = new ArrayList<>(resultLimit);

        Vertex root = g.V().has("iid", 
            makeIid(Entity.PERSON, personId)).next();

        List<Vertex> l1Friends = new ArrayList<>();
        g.V(root).out("knows")
            .sideEffect(v -> l1Friends.add(v.get()))
            .has("firstName", firstName)
            .order()
            .by("lastName", incr)
            .by(id(), incr)
            .sideEffect(v -> {
              if (matchList.size() < resultLimit) {
                matchList.add(v.get());
                distList.add(1);
              }
            })
        .iterate();

        if (matchList.size() < resultLimit && l1Friends.size() > 0) {
          List<Vertex> l2Friends = new ArrayList<>();
          g.V(l1Friends.toArray())
              .out("knows")
              .dedup()
              .is(without(l1Friends))
              .is(without(root))
              .sideEffect(v -> l2Friends.add(v.get()))
              .has("firstName", firstName)
              .order()
              .by("lastName", incr)
              .by(id(), incr)
              .sideEffect(v -> {
                if (matchList.size() < resultLimit) {
                  matchList.add(v.get());
                  distList.add(2);
                }
              })
          .iterate();

          if (matchList.size() < resultLimit && l2Friends.size() > 0) {
            g.V(l2Friends.toArray())
                .out("knows")
                .dedup()
                .is(without(l2Friends))
                .is(without(l1Friends))
                .has("firstName", firstName)
                .order()
                .by("lastName", incr)
                .by(id(), incr)
                .sideEffect(v -> {
                  if (matchList.size() < resultLimit) {
                    matchList.add(v.get());
                    distList.add(3);
                  }
                })
            .iterate();
          }
        }

        Map<Vertex, Map<String, List<String>>> propertiesMap = 
            new HashMap<>(matchList.size());
        g.V(matchList.toArray()).as("person")
            .<List<String>>valueMap().as("props")
            .select("person", "props")
            .forEachRemaining(map -> {
              propertiesMap.put((Vertex)map.get("person"), 
                  (Map<String,List<String>>)map.get("props"));
            });

        Map<Vertex, String> placeNameMap = new HashMap<>(matchList.size());
        g.V(matchList.toArray()).as("person")
            .out("isLocatedIn")
            .<String>values("name")
            .as("placeName")
            .select("person", "placeName")
            .forEachRemaining(map -> {
              placeNameMap.put((Vertex)map.get("person"), 
                  (String)map.get("placeName"));
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
          Map<String, List<String>> properties = propertiesMap.get(match);
          List<String> emails = properties.get("email");
          if (emails == null)
            emails = new ArrayList<>();
          List<String> languages = properties.get("language");
          if (languages == null)
            languages = new ArrayList<>();
          String placeName = placeNameMap.get(match);
          List<List<Object>> universityInfo = universityInfoMap.get(match);
          if (universityInfo == null)
            universityInfo = new ArrayList<>();
          List<List<Object>> companyInfo = companyInfoMap.get(match);
          if (companyInfo == null)
            companyInfo = new ArrayList<>();
          result.add(new LdbcQuery1Result(
                getSNBId(match),
                properties.get("lastName").get(0),
                distList.get(i),
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

      //            timers[NUMTIMERS-1][1] = System.nanoTime();
      //            for (int i = 0; i < NUMTIMERS; i++)
      //                System.out.println(String.format(
      //                "LdbcQuery1: %d: time: %dus", i, 
      //                (timers[i][1] - timers[i][0])/1000l));
    }

    public void executeOperationRaw(final LdbcQuery1 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      //            int NUMTIMERS = 1;
      //            long[][] timers = new long[NUMTIMERS][2];
      //            for (int i = 0; i < NUMTIMERS; i++) {
      //                timers[i][0] = 0;
      //                timers[i][1] = 0;
      //            }
      //            timers[NUMTIMERS-1][0] = System.nanoTime();

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        long personId = operation.personId();
        String firstName = operation.firstName();
        int maxLevels = 3;

        Graph client = dbConnectionState.client();
        GraphTraversalSource g = client.traversal();


        Vertex rootPerson = g.V().has("iid", 
            makeIid(Entity.PERSON, personId)).next();

        List<Vertex> friends = new ArrayList<>();
        List<Integer> levelIndices = new ArrayList<>();
        List<Integer> matchIndices = new ArrayList<>();

        friends.add(rootPerson);
        levelIndices.add(friends.size());
        int currentLevel = 0;

        for (int i = 0; i < friends.size(); i++) {
          if (i == levelIndices.get(levelIndices.size() - 1)) {
            levelIndices.add(friends.size());
            currentLevel++;

            if (currentLevel == maxLevels) {
              break;
            }

            if (matchIndices.size() >= operation.limit()) {
              break;
            }
          }

          Vertex person = friends.get(i);

          person.edges(Direction.OUT, "knows").forEachRemaining((e) -> {
            Vertex friend = e.inVertex();
            if (!friends.contains(friend)) {
              friends.add(friend);
              if (friend.<String>property("firstName").value()
                  .equals(firstName)) {
                matchIndices.add(friends.size() - 1);
              }
            }
          });
        }

        List<LdbcQuery1Result> result = new ArrayList<>();

        int matchNumber = 0;
        for (int level = 1; level < levelIndices.size(); level++) {
          int endIndex = levelIndices.get(level);

          List<Vertex> equidistantVertices = new ArrayList<>();
          while (matchNumber < matchIndices.size()
              && matchIndices.get(matchNumber) < endIndex) {
            Vertex friend = friends.get(matchIndices.get(matchNumber));
            equidistantVertices.add(friend);
            matchNumber++;
              }

          equidistantVertices.sort((a, b) -> {
            Vertex v1 = (Vertex) a;
            Vertex v2 = (Vertex) b;

            String v1LastName = v1.<String>property("lastName").value();
            String v2LastName = v2.<String>property("lastName").value();

            int lastNameCompareVal = v1LastName.compareTo(v2LastName);
            if (lastNameCompareVal != 0) {
              return lastNameCompareVal;
            } else {
              return getSNBId(v1).compareTo(getSNBId(v2));
            }
          });

          for (Vertex f : equidistantVertices) {
            long friendId = getSNBId(f);
            String friendLastName = null;
            int distanceFromPerson = level;
            long friendBirthday = 0;
            long friendCreationDate = 0;
            String friendGender = null;
            String friendBrowserUsed = null;
            String friendLocationIp = null;
            List<String> friendEmails = new ArrayList<>();
            List<String> friendLanguages = new ArrayList<>();
            String friendCityName = null;
            List<List<Object>> friendUniversities = new ArrayList<>();
            List<List<Object>> friendCompanies = new ArrayList<>();

            // Extract normal properties.
            Iterator<VertexProperty<String>> props = f.properties();
            while (props.hasNext()) {
              VertexProperty<String> prop = props.next();

              switch (prop.key()) {
                case "lastName":
                  friendLastName = prop.value();
                  break;
                case "birthday":
                  friendBirthday = Long.decode(prop.value());
                  break;
                case "creationDate":
                  friendCreationDate = Long.decode(prop.value());
                  break;
                case "gender":
                  friendGender = prop.value();
                  break;
                case "browserUsed":
                  friendBrowserUsed = prop.value();
                  break;
                case "locationIP":
                  friendLocationIp = prop.value();
                  break;
                case "email":
                  friendEmails.add(prop.value());
                  break;
                case "language":
                  friendLanguages.add(prop.value());
                  break;
              }
            }

            // Fetch where person is located
            Vertex friendPlace = 
                f.edges(Direction.OUT, "isLocatedIn").next().inVertex();
            friendCityName = friendPlace.<String>property("name").value();

            // Fetch universities studied at
            f.edges(Direction.OUT, "studyAt").forEachRemaining((e) -> {
              Integer classYear = 
                  Integer.decode(e.<String>property("classYear").value());
              Vertex organization = e.inVertex();
              String orgName = organization.<String>property("name").value();
              Vertex place = organization.edges(Direction.OUT, "isLocatedIn")
                  .next().inVertex();
              String placeName = place.<String>property("name").value();

              List<Object> universityInfo = new ArrayList<>();
              universityInfo.add(orgName);
              universityInfo.add(classYear);
              universityInfo.add(placeName);

              friendUniversities.add(universityInfo);
            });

            // Fetch companies worked at
            f.edges(Direction.OUT, "workAt").forEachRemaining((e) -> {
              Integer workFrom = 
                  Integer.decode(e.<String>property("workFrom").value());
              Vertex company = e.inVertex();
              String compName = company.<String>property("name").value();
              Vertex place = company.edges(Direction.OUT, "isLocatedIn")
                  .next().inVertex();
              String placeName = place.<String>property("name").value();

              List<Object> companyInfo = new ArrayList<>();
              companyInfo.add(compName);
              companyInfo.add(workFrom);
              companyInfo.add(placeName);

              friendCompanies.add(companyInfo);
            });

            LdbcQuery1Result res = new LdbcQuery1Result(
                friendId,
                friendLastName,
                level,
                friendBirthday,
                friendCreationDate,
                friendGender,
                friendBrowserUsed,
                friendLocationIp,
                friendEmails,
                friendLanguages,
                friendCityName,
                friendUniversities,
                friendCompanies
                );

            result.add(res);

            if (result.size() == operation.limit()) {
              break;
            }
          }

          if (result.size() == operation.limit() || 
              matchNumber == matchIndices.size()) {
            break;
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

        resultReporter.report(result.size(), result, operation);
        break;
      }

      //            timers[NUMTIMERS-1][1] = System.nanoTime();
      //            for (int i = 0; i < NUMTIMERS; i++)
      //                System.out.println(String.format(
      //                "LdbcQuery1: %d: time: %dus", i, 
      //                (timers[i][1] - timers[i][0])/1000l));
    }

  }

  public static class LdbcQuery2Handler 
        implements OperationHandler<LdbcQuery2, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery2Handler.class);

    @Override
    public void executeOperation(final LdbcQuery2 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery3Handler 
        implements OperationHandler<LdbcQuery3, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery3Handler.class);

    @Override
    public void executeOperation(final LdbcQuery3 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery4Handler 
        implements OperationHandler<LdbcQuery4, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery4Handler.class);

    @Override
    public void executeOperation(final LdbcQuery4 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery5Handler 
        implements OperationHandler<LdbcQuery5, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery5Handler.class);

    @Override
    public void executeOperation(final LdbcQuery5 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery6Handler 
        implements OperationHandler<LdbcQuery6, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery6Handler.class);

    @Override
    public void executeOperation(final LdbcQuery6 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery7Handler 
        implements OperationHandler<LdbcQuery7, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery7Handler.class);

    @Override
    public void executeOperation(final LdbcQuery7 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery8Handler 
        implements OperationHandler<LdbcQuery8, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery8Handler.class);

    @Override
    public void executeOperation(final LdbcQuery8 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery9Handler 
        implements OperationHandler<LdbcQuery9, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery9Handler.class);

    @Override
    public void executeOperation(final LdbcQuery9 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery10Handler 
        implements OperationHandler<LdbcQuery10, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery10Handler.class);

    @Override
    public void executeOperation(final LdbcQuery10 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery11Handler 
        implements OperationHandler<LdbcQuery11, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery11Handler.class);

    @Override
    public void executeOperation(final LdbcQuery11 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery12Handler 
        implements OperationHandler<LdbcQuery12, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery12Handler.class);

    @Override
    public void executeOperation(final LdbcQuery12 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery13Handler 
        implements OperationHandler<LdbcQuery13, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery13Handler.class);

    @Override
    public void executeOperation(final LdbcQuery13 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  public static class LdbcQuery14Handler 
        implements OperationHandler<LdbcQuery14, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcQuery14Handler.class);

    @Override
    public void executeOperation(final LdbcQuery14 operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {

    }

  }

  /**
   * ------------------------------------------------------------------------
   * Short Queries
   * ------------------------------------------------------------------------
   */
  public static class LdbcShortQuery1PersonProfileHandler implements 
      OperationHandler<LdbcShortQuery1PersonProfile, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcShortQuery1PersonProfileHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery1PersonProfile operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        long person_id = operation.personId();
        Graph client = dbConnectionState.client();
        GraphTraversalSource g = client.traversal();

        Vertex person = g.V().has("iid", 
            makeIid(Entity.PERSON, person_id)).next();

        Iterator<VertexProperty<String>> props = person.properties();
        Map<String, String> propertyMap = new HashMap<>();
        props.forEachRemaining((prop) -> {
          propertyMap.put(prop.key(), prop.value());
        });

        Vertex place = 
            person.edges(Direction.OUT, "isLocatedIn").next().inVertex();
        long placeId = getSNBId(place);

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

  public static class LdbcShortQuery2PersonPostsHandler implements 
      OperationHandler<LdbcShortQuery2PersonPosts, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcShortQuery2PersonPostsHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery2PersonPosts operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = dbConnectionState.client();
        GraphTraversalSource g = client.traversal();

        List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>();

        Vertex person = g.V().has("iid", 
            makeIid(Entity.PERSON, operation.personId())).next();
        Iterator<Edge> edges = person.edges(Direction.IN, "hasCreator");

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
            long v1Id = getSNBId(v1);
            long v2Id = getSNBId(v2);
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

          long messageId = getSNBId(message);

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
          if (message.label().equals(Entity.POST.getName())) {
            originalPostId = messageId;
            originalPostAuthorId = getSNBId(person);
            originalPostAuthorFirstName = 
                person.<String>property("firstName").value();
            originalPostAuthorLastName = 
                person.<String>property("lastName").value();
          } else {
            Vertex parentMessage = 
                message.edges(Direction.OUT, "replyOf").next().inVertex();
            while (true) {
              if (parentMessage.label().equals(Entity.POST.getName())) {
                originalPostId = getSNBId(parentMessage);

                Vertex author = parentMessage.edges(Direction.OUT, 
                    "hasCreator").next().inVertex();
                originalPostAuthorId = getSNBId(author);
                originalPostAuthorFirstName = 
                    author.<String>property("firstName").value();
                originalPostAuthorLastName = 
                    author.<String>property("lastName").value();
                break;
              } else {
                parentMessage = parentMessage.edges(Direction.OUT, "replyOf")
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

  public static class LdbcShortQuery3PersonFriendsHandler implements 
      OperationHandler<LdbcShortQuery3PersonFriends, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcShortQuery3PersonFriendsHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery3PersonFriends operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = dbConnectionState.client();
        GraphTraversalSource g = client.traversal();

        List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();

        Vertex person = g.V().has("iid", 
            makeIid(Entity.PERSON, operation.personId())).next();

        Iterator<Edge> edges = person.edges(Direction.OUT, "knows");

        edges.forEachRemaining((e) -> {
          long creationDate = 
              Long.decode(e.<String>property("creationDate").value());

          Vertex friend = e.inVertex();

          long personId = getSNBId(friend);

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
          } else if (r1.friendshipCreationDate() < 
              r2.friendshipCreationDate()) {
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

  public static class LdbcShortQuery4MessageContentHandler implements 
      OperationHandler<LdbcShortQuery4MessageContent, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcShortQuery4MessageContentHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery4MessageContent operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = dbConnectionState.client();
        GraphTraversalSource g = client.traversal();

        Vertex message = g.V().has("iid", 
            makeIid(Entity.MESSAGE, operation.messageId())).next();

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

  public static class LdbcShortQuery5MessageCreatorHandler implements 
      OperationHandler<LdbcShortQuery5MessageCreator, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcShortQuery5MessageCreatorHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery5MessageCreator operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = dbConnectionState.client();
        GraphTraversalSource g = client.traversal();

        Vertex message = g.V().has("iid", 
            makeIid(Entity.MESSAGE, operation.messageId())).next();

        Vertex creator = 
            message.edges(Direction.OUT, "hasCreator").next().inVertex();

        long creatorId = getSNBId(creator);
        String creatorFirstName = 
            creator.<String>property("firstName").value();
        String creatorLastName = creator.<String>property("lastName").value();

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

  public static class LdbcShortQuery6MessageForumHandler implements 
      OperationHandler<LdbcShortQuery6MessageForum, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcShortQuery6MessageForumHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery6MessageForum operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = dbConnectionState.client();
        GraphTraversalSource g = client.traversal();

        Vertex vertex = g.V().has("iid", 
            makeIid(Entity.MESSAGE, operation.messageId())).next();

        LdbcShortQuery6MessageForumResult result;
        while (true) {
          if (vertex.label().equals(Entity.FORUM.getName())) {
            long forumId = getSNBId(vertex);
            String forumTitle = vertex.<String>property("title").value();

            Vertex moderator = 
                vertex.edges(Direction.OUT, "hasModerator").next().inVertex();

            long moderatorId = getSNBId(moderator);
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
          } else if (vertex.label().equals(Entity.POST.getName())) {
            vertex = 
                vertex.edges(Direction.IN, "containerOf").next().outVertex();
          } else {
            vertex = vertex.edges(Direction.OUT, "replyOf").next().inVertex();
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

  public static class LdbcShortQuery7MessageRepliesHandler implements 
      OperationHandler<LdbcShortQuery7MessageReplies, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcShortQuery7MessageRepliesHandler.class);

    @Override
    public void executeOperation(final LdbcShortQuery7MessageReplies operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter resultReporter) throws DbException {
      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        Graph client = dbConnectionState.client();
        GraphTraversalSource g = client.traversal();

        Vertex message = g.V().has("iid", 
            makeIid(Entity.MESSAGE, operation.messageId())).next();
        Vertex messageAuthor = 
            message.edges(Direction.OUT, "hasCreator").next().inVertex();
        long messageAuthorId = getSNBId(messageAuthor);

        List<Vertex> replies = new ArrayList<>();
        message.edges(Direction.IN, "replyOf").forEachRemaining((e) -> {
          replies.add(e.outVertex());
        });

        List<Long> messageAuthorFriendIds = new ArrayList<>();
        messageAuthor.edges(Direction.OUT, "knows").forEachRemaining((e) -> {
          messageAuthorFriendIds.add(getSNBId(e.inVertex()));
        });

        List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();

        for (Vertex reply : replies) {
          long replyId = getSNBId(reply);
          String replyContent = reply.<String>property("content").value();
          long replyCreationDate = 
              Long.decode(reply.<String>property("creationDate").value());

          Vertex replyAuthor = 
              reply.edges(Direction.OUT, "hasCreator").next().inVertex();
          long replyAuthorId = getSNBId(replyAuthor);
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
  public static class LdbcUpdate1AddPersonHandler implements 
      OperationHandler<LdbcUpdate1AddPerson, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcUpdate1AddPersonHandler.class);

    private final Calendar calendar;

    public LdbcUpdate1AddPersonHandler() {
      this.calendar = new GregorianCalendar();
    }

    @Override
    public void executeOperation(LdbcUpdate1AddPerson operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter reporter) throws DbException {
      Graph client = dbConnectionState.client();
      GraphTraversalSource g = client.traversal();
      
      // Build key value properties array
      List<Object> personKeyValues = 
          new ArrayList<>(18 + 2 * operation.languages().size() + 
              2 * operation.emails().size());
      personKeyValues.add("iid");
      personKeyValues.add(makeIid(Entity.PERSON, operation.personId()));
      personKeyValues.add(T.label);
      personKeyValues.add("person");
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

      // Add person
      Vertex person = client.addVertex(personKeyValues.toArray());

      // Add edge to place
      Vertex place = g.V().has("iid", 
          makeIid(Entity.PLACE, operation.cityId())).next();
      person.addEdge("isLocatedIn", place);

      // Add edges to tags
      List<String> tagIds = new ArrayList<>(operation.tagIds().size());
      operation.tagIds().forEach((id) -> 
          tagIds.add(Entity.TAG.getName() + ":" + String.valueOf(id)));
      Iterator<Vertex> tagVItr = 
          client.vertices(tagIds.toArray());
      tagVItr.forEachRemaining((tag) -> {
        person.addEdge("hasInterest", tag);
      });

      // Add edges to universities
      List<Object> studiedAtKeyValues = new ArrayList<>(2);
      for (LdbcUpdate1AddPerson.Organization org : operation.studyAt()) {
        studiedAtKeyValues.clear();
        studiedAtKeyValues.add("classYear");
        studiedAtKeyValues.add(String.valueOf(org.year()));
        Vertex orgV = g.V().has("iid", 
            makeIid(Entity.ORGANISATION, org.organizationId())).next();
        person.addEdge("studyAt", orgV, studiedAtKeyValues.toArray());
      }

      // Add edges to companies
      List<Object> workedAtKeyValues = new ArrayList<>(2);
      for (LdbcUpdate1AddPerson.Organization org : operation.workAt()) {
        workedAtKeyValues.clear();
        workedAtKeyValues.add("workFrom");
        workedAtKeyValues.add(String.valueOf(org.year()));
        Vertex orgV = g.V().has("iid", 
            makeIid(Entity.ORGANISATION, org.organizationId())).next();
        person.addEdge("workAt", orgV, workedAtKeyValues.toArray());
      }

      client.tx().commit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  public static class LdbcUpdate2AddPostLikeHandler implements 
      OperationHandler<LdbcUpdate2AddPostLike, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcUpdate2AddPostLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate2AddPostLike operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter reporter) throws DbException {
      Graph client = dbConnectionState.client();
      GraphTraversalSource g = client.traversal();

      Vertex person = g.V().has("iid", 
          makeIid(Entity.PERSON, operation.personId())).next();
      Vertex post = g.V().has("iid", 
        makeIid(Entity.POST, operation.postId())).next();
      List<Object> keyValues = new ArrayList<>(2);
      keyValues.add("creationDate");
      keyValues.add(String.valueOf(operation.creationDate().getTime()));
      person.addEdge("likes", post, keyValues.toArray());

      client.tx().commit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  public static class LdbcUpdate3AddCommentLikeHandler implements 
      OperationHandler<LdbcUpdate3AddCommentLike, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcUpdate3AddCommentLikeHandler.class);

    @Override
    public void executeOperation(LdbcUpdate3AddCommentLike operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter reporter) throws DbException {
      Graph client = dbConnectionState.client();
      GraphTraversalSource g = client.traversal();

      Vertex person = g.V().has("iid", 
          makeIid(Entity.PERSON, operation.personId())).next();
      Vertex comment = g.V().has("iid", 
        makeIid(Entity.COMMENT, operation.commentId())).next();
      List<Object> keyValues = new ArrayList<>(2);
      keyValues.add("creationDate");
      keyValues.add(String.valueOf(operation.creationDate().getTime()));
      person.addEdge("likes", comment, keyValues.toArray());

      client.tx().commit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  public static class LdbcUpdate4AddForumHandler implements 
      OperationHandler<LdbcUpdate4AddForum, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate4AddForum operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter reporter) throws DbException {
      Graph client = dbConnectionState.client();
      GraphTraversalSource g = client.traversal();

      List<Object> forumKeyValues = new ArrayList<>(8);
      forumKeyValues.add("iid");
      forumKeyValues.add(makeIid(Entity.FORUM, operation.forumId()));
      forumKeyValues.add(T.label);
      forumKeyValues.add(Entity.FORUM.getName());
      forumKeyValues.add("title");
      forumKeyValues.add(operation.forumTitle());
      forumKeyValues.add("creationDate");
      forumKeyValues.add(String.valueOf(operation.creationDate().getTime()));

      Vertex forum = client.addVertex(forumKeyValues.toArray());

      List<String> ids = new ArrayList<>(operation.tagIds().size() + 1);
      operation.tagIds().forEach((id) -> {
        ids.add(makeIid(Entity.TAG, id));
      });
      ids.add(makeIid(Entity.PERSON, operation.moderatorPersonId()));

      g.V().has("iid", within(ids)).forEachRemaining((v) -> {
        if (v.label().equals(Entity.TAG.getName())) {
          forum.addEdge("hasTag", v);
        } else if (v.label().equals(Entity.PERSON.getName())) {
          forum.addEdge("hasModerator", v);
        } else {
          throw new RuntimeException(
              "ERROR: LdbcUpdate4AddForum query tried to add an edge to a " + 
              "vertex that is neither a tag nor a person.");
        }
      });

      client.tx().commit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  public static class LdbcUpdate5AddForumMembershipHandler implements 
      OperationHandler<LdbcUpdate5AddForumMembership, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate5AddForumMembership operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter reporter) throws DbException {
      Graph client = dbConnectionState.client();
      GraphTraversalSource g = client.traversal();

      Vertex forum = g.V().has("iid", 
          makeIid(Entity.FORUM, operation.forumId())).next();
      Vertex member = g.V().has("iid", 
        makeIid(Entity.PERSON, operation.personId())).next();

      List<Object> edgeKeyValues = new ArrayList<>(2);
      edgeKeyValues.add("joinDate");
      edgeKeyValues.add(String.valueOf(operation.joinDate().getTime()));

      forum.addEdge("hasMember", member, edgeKeyValues.toArray());

      client.tx().commit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  public static class LdbcUpdate6AddPostHandler implements 
      OperationHandler<LdbcUpdate6AddPost, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate6AddPost operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter reporter) throws DbException {
      Graph client = dbConnectionState.client();
      GraphTraversalSource g = client.traversal();

      List<Object> postKeyValues = new ArrayList<>(18);
      postKeyValues.add("iid");
      postKeyValues.add(makeIid(Entity.POST, operation.postId()));
      postKeyValues.add(T.label);
      postKeyValues.add(Entity.POST.getName());
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

      Vertex post = client.addVertex(postKeyValues.toArray());

      List<String> ids = new ArrayList<>(3 + operation.tagIds().size());
      ids.add(makeIid(Entity.PERSON, operation.authorPersonId()));
      ids.add(makeIid(Entity.FORUM, operation.forumId()));
      ids.add(makeIid(Entity.PLACE, operation.countryId()));
      operation.tagIds().forEach((id) -> {
        ids.add(makeIid(Entity.TAG, id));
      });

      g.V().has("iid", within(ids)).forEachRemaining((v) -> {
        if (v.label().equals(Entity.PERSON.getName())) {
          post.addEdge("hasCreator", v);
        } else if (v.label().equals(Entity.FORUM.getName())) {
          v.addEdge("containerOf", post);
        } else if (v.label().equals(Entity.PLACE.getName())) {
          post.addEdge("isLocatedIn", v);
        } else if (v.label().equals(Entity.TAG.getName())) {
          post.addEdge("hasTag", v);
        } else {
          throw new RuntimeException(
              "ERROR: LdbcUpdate6AddPostHandler query tried to add an edge " + 
              "to a vertex that is none of {person, forum, place, tag}.");
        }
      });

      client.tx().commit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  public static class LdbcUpdate7AddCommentHandler implements 
      OperationHandler<LdbcUpdate7AddComment, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate7AddComment operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter reporter) throws DbException {
      Graph client = dbConnectionState.client();
      GraphTraversalSource g = client.traversal();

      List<Object> commentKeyValues = new ArrayList<>(14);
      commentKeyValues.add("iid");
      commentKeyValues.add(makeIid(Entity.COMMENT, operation.commentId()));
      commentKeyValues.add(T.label);
      commentKeyValues.add(Entity.COMMENT.getName());
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

      Vertex comment = client.addVertex(commentKeyValues.toArray());

      List<String> ids = new ArrayList<>(4 + operation.tagIds().size());
      ids.add(makeIid(Entity.PERSON, operation.authorPersonId()));
      ids.add(makeIid(Entity.PLACE, operation.countryId()));
      operation.tagIds().forEach((id) -> {
        ids.add(makeIid(Entity.TAG, id));
      });
      if (operation.replyToCommentId() != -1) {
        ids.add(makeIid(Entity.COMMENT, operation.replyToCommentId()));
      }
      if (operation.replyToPostId() != -1) {
        ids.add(makeIid(Entity.POST, operation.replyToPostId()));
      }

      g.V().has("iid", within(ids)).forEachRemaining((v) -> {
        if (v.label().equals(Entity.PERSON.getName())) {
          comment.addEdge("hasCreator", v);
        } else if (v.label().equals(Entity.PLACE.getName())) {
          comment.addEdge("isLocatedIn", v);
        } else if (v.label().equals(Entity.COMMENT.getName())) {
          comment.addEdge("replyOf", v);
        } else if (v.label().equals(Entity.POST.getName())) {
          comment.addEdge("replyOf", v);
        } else if (v.label().equals(Entity.TAG.getName())) {
          comment.addEdge("hasTag", v);
        } else {
          throw new RuntimeException(
              "ERROR: LdbcUpdate7AddCommentHandler query tried to add an " + 
              "edge to a vertex that is none of {person, place, comment, " + 
              "post, tag}.");
        }
      });

      client.tx().commit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }

  public static class LdbcUpdate8AddFriendshipHandler implements 
      OperationHandler<LdbcUpdate8AddFriendship, BasicDbConnectionState> {

    final static Logger logger = 
        LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

    @Override
    public void executeOperation(LdbcUpdate8AddFriendship operation, 
        BasicDbConnectionState dbConnectionState, 
        ResultReporter reporter) throws DbException {
      Graph client = dbConnectionState.client();
      GraphTraversalSource g = client.traversal();

      List<Object> knowsEdgeKeyValues = new ArrayList<>(2);
      knowsEdgeKeyValues.add("creationDate");
      knowsEdgeKeyValues.add(
          String.valueOf(operation.creationDate().getTime()));

      String p1Iid = makeIid(Entity.PERSON, operation.person1Id());
      String p2Iid = makeIid(Entity.PERSON, operation.person2Id());

      Vertex person1 = g.V().has("iid", p1Iid).next();
      Vertex person2 = g.V().has("iid", p2Iid).next();

      person1.addEdge("knows", person2, knowsEdgeKeyValues.toArray());
      person2.addEdge("knows", person1, knowsEdgeKeyValues.toArray());

      client.tx().commit();

      reporter.report(0, LdbcNoResult.INSTANCE, operation);
    }
  }
}
