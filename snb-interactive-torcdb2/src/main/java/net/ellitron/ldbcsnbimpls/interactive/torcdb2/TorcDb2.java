/* 
 * Copyright (C) 2015-2019 Stanford University
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
package net.ellitron.ldbcsnbimpls.interactive.torcdb2;

import net.ellitron.ldbcsnbimpls.interactive.torc.TorcEntity;

import net.ellitron.torcdb2.*;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * An implementation of the LDBC SNB interactive workload[1] for TorcDB2.
 *
 * References:
 * [1]: Prat, Arnau (UPC) and Boncz, Peter (VUA) and Larriba, Josep Lluís (UPC)
 * and Angles, Renzo (TALCA) and Averbuch, Alex (NEO) and Erling, Orri (OGL)
 * and Gubichev, Andrey (TUM) and Spasić, Mirko (OGL) and Pham, Minh-Duc (VUA)
 * and Martínez, Norbert (SPARSITY). "LDBC Social Network Benchmark (SNB) -
 * v0.2.2 First Public Draft Release". http://www.ldbcouncil.org/.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcDb2 extends Db {

  private TorcDb2ConnectionState connState;

  // Maximum number of times to try a transaction before giving up.
  private static int MAX_TX_ATTEMPTS = 100;

  public TorcDb2() {
    this.connState = null;
  }

  @Override
  protected void onInit(Map<String, String> props, LoggingService loggingService) 
      throws DbException {
    connState = new TorcDb2ConnectionState(props);

    /*
     * Register operation handlers with the benchmark.
     */
    registerOperationHandler(LdbcQuery1.class,
        LdbcQuery1Handler.class);
//    registerOperationHandler(LdbcQuery2.class,
//        LdbcQuery2Handler.class);
//    registerOperationHandler(LdbcQuery3.class,
//        LdbcQuery3Handler.class);
//    registerOperationHandler(LdbcQuery4.class,
//        LdbcQuery4Handler.class);
//    registerOperationHandler(LdbcQuery5.class,
//        LdbcQuery5Handler.class);
//    registerOperationHandler(LdbcQuery6.class,
//        LdbcQuery6Handler.class);
//    registerOperationHandler(LdbcQuery7.class,
//        LdbcQuery7Handler.class);
//    registerOperationHandler(LdbcQuery8.class,
//        LdbcQuery8Handler.class);
//    registerOperationHandler(LdbcQuery9.class,
//        LdbcQuery9Handler.class);
//    registerOperationHandler(LdbcQuery10.class,
//        LdbcQuery10Handler.class);
//    registerOperationHandler(LdbcQuery11.class,
//        LdbcQuery11Handler.class);
//    registerOperationHandler(LdbcQuery12.class,
//        LdbcQuery12Handler.class);
//    registerOperationHandler(LdbcQuery13.class,
//        LdbcQuery13Handler.class);
//    registerOperationHandler(LdbcQuery14.class,
//        LdbcQuery14Handler.class);
//
    registerOperationHandler(LdbcShortQuery1PersonProfile.class,
        LdbcShortQuery1PersonProfileHandler.class);
//    registerOperationHandler(LdbcShortQuery2PersonPosts.class,
//        LdbcShortQuery2PersonPostsHandler.class);
//    registerOperationHandler(LdbcShortQuery3PersonFriends.class,
//        LdbcShortQuery3PersonFriendsHandler.class);
//    registerOperationHandler(LdbcShortQuery4MessageContent.class,
//        LdbcShortQuery4MessageContentHandler.class);
//    registerOperationHandler(LdbcShortQuery5MessageCreator.class,
//        LdbcShortQuery5MessageCreatorHandler.class);
//    registerOperationHandler(LdbcShortQuery6MessageForum.class,
//        LdbcShortQuery6MessageForumHandler.class);
//    registerOperationHandler(LdbcShortQuery7MessageReplies.class,
//        LdbcShortQuery7MessageRepliesHandler.class);
//
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
    connState.close();
  }

  @Override
  protected DbConnectionState getConnectionState() throws DbException {
    return connState;
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

    @Override
    public void executeOperation(final LdbcQuery1 op, DbConnectionState dbConnState,
        ResultReporter resultReporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState)dbConnState;

      if (cState.fakeComplexReads()) {
        List<LdbcQuery1Result> result = new ArrayList<>(op.limit());

        for (int i = 0; i < op.limit(); i++) {
          List<Long> personIDs = cState.personIDFeed();
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

        resultReporter.report(result.size(), result, op);
        return;
      }

      // Parameters of this query
      final long personId = op.personId();
      final String firstName = op.firstName();
      final int limit = op.limit();

      final UInt128 torcPersonId = new UInt128(TorcEntity.PERSON.idSpace, personId);

      Graph graph = cState.getGraph();

      List<LdbcQuery1Result> result = new ArrayList<>(limit);

      // Vertices that match our search criteria.
      List<Vertex> l1_matches = new ArrayList<>();
      List<Vertex> l2_matches = new ArrayList<>();
      List<Vertex> l3_matches = new ArrayList<>();

      Vertex start = new Vertex(torcPersonId, TorcEntity.PERSON.label);
      TraversalResult l1_friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");

      graph.fillProperties(l1_friends);
      for (Vertex v : l1_friends.vSet) {
        if (((String)v.getProperty("firstName")).equals(firstName)) {
          l1_matches.add(v);
        }
      }

      Set<Vertex> seenSet = new HashSet<>();
      seenSet.add(start);
      seenSet.addAll(l1_friends.vSet);

      if (l1_matches.size() < limit) {
        TraversalResult l2_friends = graph.traverse(l1_friends, "knows", Direction.OUT, false, "Person");

        GraphHelper.subtract(l2_friends, seenSet);

        graph.fillProperties(l2_friends);
        for (Vertex v : l2_friends.vSet) {
          if (((String)v.getProperty("firstName")).equals(firstName)) {
            l2_matches.add(v);
          }
        }

        seenSet.addAll(l2_friends.vSet);

        if (l1_matches.size() + l2_matches.size() < limit) {
          TraversalResult l3_friends = graph.traverse(l2_friends, "knows", Direction.OUT, false, "Person");

          GraphHelper.subtract(l3_friends, seenSet);

          graph.fillProperties(l3_friends);
          for (Vertex v : l3_friends.vSet) {
            if (((String)v.getProperty("firstName")).equals(firstName)) {
              l3_matches.add(v);
            }
          }
        }
      }

      // Sort the matches ascending by lastname and then ascending by
      // identifier for a given distance.
      Comparator<Vertex> c = new Comparator<Vertex>() {
            public int compare(Vertex v1, Vertex v2) {
              String v1LastName = ((String)v1.getProperty("lastName"));
              String v2LastName = ((String)v2.getProperty("lastName"));
              
              if (v1LastName.compareTo(v2LastName) != 0) {
                return v1LastName.compareTo(v2LastName);
              } else {
                Long v1Id = v1.id().getLowerLong();
                Long v2Id = v2.id().getLowerLong();
                if (v1Id > v2Id)
                  return 1;
                else if (v1Id < v2Id)
                  return -1;
                else
                  return 0;
              }
            }
          };

      Collections.sort(l1_matches, c);
      Collections.sort(l2_matches, c);
      Collections.sort(l3_matches, c);
      
      List<Vertex> matches = new ArrayList<>();
      matches.addAll(l1_matches);
      matches.addAll(l2_matches);
      matches.addAll(l3_matches);

      matches = matches.subList(0, Math.min(matches.size(), limit));

      TraversalResult match_place = graph.traverse(matches, "isLocatedIn", Direction.OUT, false, "Place");
      TraversalResult match_universities = graph.traverse(matches, "studyAt", Direction.OUT, true, "Organisation");
      TraversalResult match_companies = graph.traverse(matches, "workAt", Direction.OUT, true, "Organisation");
      TraversalResult university_place = graph.traverse(match_universities, "isLocatedIn", Direction.OUT, false, "Place");
      TraversalResult company_place = graph.traverse(match_companies, "isLocatedIn", Direction.OUT, false, "Place");

      graph.fillProperties(match_place, match_universities, match_companies, university_place, company_place);

      for (int j = 0; j < matches.size(); j++) {
        Vertex f = matches.get(j);
        int distance;
        if (j < l1_matches.size())
          distance = 1;
        else if (j < l1_matches.size() + l2_matches.size())
          distance = 2;
        else
          distance = 3;

        List<Vertex> universities = match_universities.vMap.get(f);
        List<Map<Object, Object>> uniProps = match_universities.pMap.get(f);
        List<List<Object>> universityInfo = new ArrayList<>();
        if (universities != null) {
          for (int i = 0; i < universities.size(); i++) {
            Vertex university = universities.get(i);
            Map<Object, Object> props = uniProps.get(i);

            List<Object> info = new ArrayList<>(3);
            info.add(university.getProperty("name"));
            info.add(props.get("classYear"));
            info.add(university_place.vMap.get(university).get(0).getProperty("name"));
            universityInfo.add(info);
          }
        }

        List<Vertex> companies = match_companies.vMap.get(f);
        List<Map<Object, Object>> comProps = match_companies.pMap.get(f);
        List<List<Object>> companyInfo = new ArrayList<>();
        if (companies != null) {
          for (int i = 0; i < companies.size(); i++) {
            Vertex company = companies.get(i);
            Map<Object, Object> props = comProps.get(i);

            List<Object> info = new ArrayList<>(3);
            info.add(company.getProperty("name"));
            info.add(props.get("workFrom"));
            info.add(company_place.vMap.get(company).get(0).getProperty("name"));
            companyInfo.add(info);
          }
        }

        result.add(new LdbcQuery1Result(
            f.id().getLowerLong(), //((UInt128)t.get().get("friendId")).getLowerLong(),
            ((String)f.getProperty("lastName")), //(String)t.get().get("lastName"),
            distance, //((Long)t.get().get("distance")).intValue() - 1,
            ((Long)f.getProperty("birthday")), //Long.valueOf((String)t.get().get("birthday")),
            ((Long)f.getProperty("creationDate")), //Long.valueOf((String)t.get().get("creationDate")),
            ((String)f.getProperty("gender")), //(String)t.get().get("gender"),
            ((String)f.getProperty("browserUsed")), //(String)t.get().get("browserUsed"),
            ((String)f.getProperty("locationIP")), //(String)t.get().get("locationIP"),
            ((List<String>)f.getProperty("email")), //(List<String>)t.get().get("emails"),
            ((List<String>)f.getProperty("language")), //(List<String>)t.get().get("languages"),
            ((String)match_place.vMap.get(f).get(0).getProperty("name")), //(String)t.get().get("placeName"),
            universityInfo, //(List<List<Object>>)t.get().get("universityInfo"),
            companyInfo)); //(List<List<Object>>)t.get().get("companyInfo")));
      }

      resultReporter.report(result.size(), result, op);
    }
  }

  /**
   * Given a start Person, find (most recent) Posts and Comments from all of
   * that Person’s friends, that were created before (and including) a given
   * date. Return the top 20 Posts/Comments, and the Person that created each
   * of them. Sort results descending by creation date, and then ascending by
   * Post identifier.[1]
   */
//  public static class LdbcQuery2Handler
//      implements OperationHandler<LdbcQuery2, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery2 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery2Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
//          int n2 = ThreadLocalRandom.current().nextInt(0, messageIDs.size());
//          Long pid = personIDs.get(n1);
//          Long mid = messageIDs.get(n2);
//          result.add(new LdbcQuery2Result(
//              pid, 
//              null,
//              null,
//              mid,
//              null,
//              0));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//      
//      // Parameters of this query
//      final long personId = op.personId();
//      final long maxDate = op.maxDate().getTime();
//      final int limit = op.limit();
//      
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = 
//        (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery2Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//        TraversalResult friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");
//
//        TraversalResult messages = graph.traverse(friends, "hasCreator", Direction.IN, false, "Post", "Comment");
//
//        graph.fillProperties(messages);
//        
//        // Sort the Posts and Comments by their creation date.
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex v1, TorcVertex v2) {
//                Long v1creationDate = ((Long)v1.getProperty("creationDate"));
//                Long v2creationDate = ((Long)v2.getProperty("creationDate"));
//                if (v1creationDate > v2creationDate)
//                  return 1;
//                else if (v1creationDate < v2creationDate)
//                  return -1;
//                else if (v1.id().getLowerLong() > v2.id().getLowerLong())
//                  return -1;
//                else
//                  return 1;
//              }
//            };
//
//        PriorityQueue<TorcVertex> pq = new PriorityQueue(limit, c);
//        for (TorcVertex m : messages.vSet) {
//          Long creationDate = (Long)m.getProperty("creationDate");
//         
//          if (creationDate > maxDate)
//            continue;
//
//          if (pq.size() < limit) {
//            pq.add(m);
//            continue;
//          }
//
//          if (creationDate > (Long)pq.peek().getProperty("creationDate")) {
//            pq.add(m);
//            pq.poll();
//          }
//        }
//
//        // Create a list from the priority queue. This list will contain the
//        // messages in reverse order.
//        List<TorcVertex> msgList = new ArrayList<>(pq.size());
//        while (pq.size() > 0)
//          msgList.add(pq.poll());
//
//        // Wish there was a good way to go back and find the authors from what
//        // we have already read, but we don't have a great way to do that now,
//        // so go and read the authors.
//        TraversalResult authors = graph.traverse(msgList, "hasCreator", Direction.OUT, false, "Person");
//
//        graph.fillProperties(authors);
//
//        for (int i = msgList.size()-1; i >= 0; i--) {
//          TorcVertex m = msgList.get(i);
//          TorcVertex f = authors.vMap.get(m).get(0);
//
//          String content = (String)m.getProperty("content");
//          if (content.equals(""))
//            content = (String)m.getProperty("imageFile");
//
//          result.add(new LdbcQuery2Result(
//              f.id().getLowerLong(), //((UInt128)t.get().get("personId")).getLowerLong(),
//              ((String)f.getProperty("firstName")), //(String)t.get().get("firstName"), 
//              ((String)f.getProperty("lastName")), //(String)t.get().get("lastName"),
//              m.id().getLowerLong(), //((UInt128)t.get().get("messageId")).getLowerLong(), 
//              content, //(String)t.get().get("content"),
//              ((Long)m.getProperty("creationDate")))); //Long.valueOf((String)t.get().get("creationDate"))))
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person, find Persons that are their friends and friends of
//   * friends (excluding start Person) that have made Posts/Comments in both of
//   * the given Countries, X and Y, within a given period. Only Persons that are
//   * foreign to Countries X and Y are considered, that is Persons whose
//   * Location is not Country X or Country Y. Return top 20 Persons, and their
//   * Post/Comment counts, in the given countries and period. Sort results
//   * descending by total number of Posts/Comments, and then ascending by Person
//   * identifier.[1]
//   */
//  public static class LdbcQuery3Handler
//      implements OperationHandler<LdbcQuery3, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery3 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery3Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
//          Long pid = personIDs.get(n1);
//          result.add(new LdbcQuery3Result(
//              pid,
//              null,
//              null,
//              0,
//              0,
//              0));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//
//      // Parameters of this query
//      final long personId = op.personId();
//      final String countryXName = op.countryXName();
//      final String countryYName = op.countryYName();
//      final long startDate = op.startDate().getTime();
//      final long durationDays = op.durationDays();
//      final int limit = op.limit();
//
//      final long endDate = startDate + (durationDays * 24L * 60L * 60L * 1000L);
//
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery3Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//
//        TraversalResult l1_friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");
//        TraversalResult l2_friends = graph.traverse(l1_friends, "knows", Direction.OUT, false, "Person");
//
//        Set<TorcVertex> friends = new HashSet<>(l1_friends.vSet.size() + l2_friends.vSet.size());
//        friends.addAll(l1_friends.vSet);
//        friends.addAll(l2_friends.vSet);
//        friends.remove(start);
//
//        TraversalResult friendCity = graph.traverse(friends, "isLocatedIn", Direction.OUT, false, "Place");
//        TraversalResult cityCountry = graph.traverse(friendCity, "isPartOf", Direction.OUT, false, "Place");
//        graph.fillProperties(cityCountry);
//
//        // Filter out all friends located in either countryX or countryY.
//        friends.removeIf(f -> {
//          String placeName = (String)cityCountry.vMap.get(friendCity.vMap.get(f).get(0)).get(0).getProperty("name");
//          return placeName.equals(countryXName) || placeName.equals(countryYName);
//        });
//
//        TraversalResult messages = graph.traverse(friends, "hasCreator", Direction.IN, false, "Post", "Comment");
//       
//        graph.fillProperties(messages.vSet, "creationDate");
//
//        // Filter out all messages not in the given time window.
//        messages.vSet.removeIf(m -> {
//          Long creationDate = (Long)m.getProperty("creationDate");
//          return !(startDate <= creationDate && creationDate <= endDate);
//        });
//
//        TraversalResult messageLocation = graph.traverse(messages.vSet, "isLocatedIn", Direction.OUT, false, "Place");
//
//        graph.fillProperties(messageLocation.vSet, "name");
//
//        // Filter out all messages not in countryX or countryY.
//        messages.vSet.removeIf(m -> {
//          String placeName = (String)messageLocation.vMap.get(m).get(0).getProperty("name");
//          return !(placeName.equals(countryXName) || placeName.equals(countryYName));
//        });
//
//        // Once we intersect with the filtered messages, only friends with
//        // non-zero number of messages will be part of the messages.vMap keyset.
//        GraphHelper.intersect(messages, messages.vSet);
//
//        Map<TorcVertex, Long> friendCountryXMsgCounts = new HashMap<>(messages.vMap.size());
//        Map<TorcVertex, Long> friendCountryYMsgCounts = new HashMap<>(messages.vMap.size()); 
//        List<TorcVertex> friendResults = new ArrayList<>(messages.vMap.size());
//        for (TorcVertex f : messages.vMap.keySet()) {
//          List<TorcVertex> mList = messages.vMap.get(f);
//          long countryXCount = 0;
//          long countryYCount = 0;
//          for (TorcVertex m : mList) {
//            String placeName = (String)messageLocation.vMap.get(m).get(0).getProperty("name");
//
//            if (placeName.equals(countryXName))
//              countryXCount++;
//
//            if (placeName.equals(countryYName))
//              countryYCount++;
//          }
//
//          if (countryXCount > 0 && countryYCount > 0) {
//            friendCountryXMsgCounts.put(f, countryXCount);
//            friendCountryYMsgCounts.put(f, countryYCount);
//            friendResults.add(f);
//          }
//        }
//       
//        // Sort friends by post count, then ascending by person identifier.
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex v1, TorcVertex v2) {
//                Long v1MsgCount = friendCountryXMsgCounts.get(v1) + friendCountryYMsgCounts.get(v1);
//                Long v2MsgCount = friendCountryXMsgCounts.get(v2) + friendCountryYMsgCounts.get(v2);
//
//                if (v1MsgCount != v2MsgCount) {
//                  // Post count sort is descending
//                  if (v1MsgCount > v2MsgCount)
//                    return -1;
//                  else
//                    return 1;
//                } else {
//                  Long v1Id = v1.id().getLowerLong();
//                  Long v2Id = v2.id().getLowerLong();
//                  // IDs are ascending
//                  if (v1Id > v2Id)
//                    return 1;
//                  else
//                    return -1;
//                }
//              }
//            };
//
//        Collections.sort(friendResults, c);
//
//        // Take top limit
//        friendResults = friendResults.subList(0, Math.min(friendResults.size(), limit));
//
//        graph.fillProperties(friendResults);
//
//        for (int i = 0; i < friendResults.size(); i++) {
//          TorcVertex f = friendResults.get(i);
//
//          result.add(new LdbcQuery3Result(
//              f.id().getLowerLong(), //((UInt128)((Traverser<Map>)t).get().get("personId")).getLowerLong(),
//              (String)f.getProperty("firstName"), //(String)((Traverser<Map>)t).get().get("firstName"), 
//              (String)f.getProperty("lastName"), //(String)((Traverser<Map>)t).get().get("lastName"),
//              friendCountryXMsgCounts.get(f), //(Long)((Traverser<Map>)t).get().get("countryXCount"),
//              friendCountryYMsgCounts.get(f), //(Long)((Traverser<Map>)t).get().get("countryYCount"),
//              friendCountryXMsgCounts.get(f) + friendCountryYMsgCounts.get(f))); //(Long)((Traverser<Map>)t).get().get("totalCount")))
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          ((TorcGraph)graph).enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person, find Tags that are attached to Posts that were
//   * created by that Person’s friends. Only include Tags that were attached to
//   * friends’ Posts created within a given time interval, and that were never
//   * attached to friends’ Posts created before this interval. Return top 10
//   * Tags, and the count of Posts, which were created within the given time
//   * interval, that this Tag was attached to. Sort results descending by Post
//   * count, and then ascending by Tag name.[1]
//   */
//  public static class LdbcQuery4Handler
//      implements OperationHandler<LdbcQuery4, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery4 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery4Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          result.add(new LdbcQuery4Result(
//              null,
//              0));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//
//      // Parameters of this query
//      final long personId = op.personId();
//      final long startDate = op.startDate().getTime();
//      final long durationDays = op.durationDays();
//      final int limit = op.limit();
//
//      final long endDate = startDate + (durationDays * 24L * 60L * 60L * 1000L);
//
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery4Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//
//        TraversalResult friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");
//        TraversalResult posts = graph.traverse(friends, "hasCreator", Direction.IN, false, "Post");
//
//        graph.fillProperties(posts);
//
//        // Filter out posts that are more recent than endDate. Don't want to do
//        // extra work for them.
//        posts.vSet.removeIf(p -> {
//          Long creationDate = (Long)p.getProperty("creationDate");
//          return creationDate > endDate;
//        });
//
//        TraversalResult tags = graph.traverse(posts.vSet, "hasTag", Direction.OUT, false, "Tag");
//
//        // Separate out tags before the window and in the window.
//        Set<TorcVertex> tagsWithinWindow = new HashSet<>();
//        Set<TorcVertex> tagsBeforeWindow = new HashSet<>();
//        Map<TorcVertex, Long> tagCounts = new HashMap<>();
//        for (TorcVertex p : tags.vMap.keySet()) {
//          Long pCreationDate = (Long)p.getProperty("creationDate");
//          if (pCreationDate >= startDate && pCreationDate <= endDate) {
//            for (TorcVertex t : tags.vMap.get(p)) {
//              tagsWithinWindow.add(t);
//              if (tagCounts.containsKey(t))
//                tagCounts.put(t, tagCounts.get(t) + 1);
//              else
//                tagCounts.put(t, 1L);
//            }
//          } else if (pCreationDate < startDate) {
//            for (TorcVertex t : tags.vMap.get(p))
//              tagsBeforeWindow.add(t);
//          }
//        }
//
//        tagsWithinWindow.removeAll(tagsBeforeWindow);
//
//        List<TorcVertex> matchedTags = new ArrayList<>(tagsWithinWindow);
//
//        graph.fillProperties(matchedTags);
//
//        // Sort tags by count
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex t1, TorcVertex t2) {
//                Long t1Count = tagCounts.get(t1);
//                Long t2Count = tagCounts.get(t2);
//
//                if (t1Count != t2Count) {
//                  // Tag count sort is descending
//                  if (t1Count > t2Count)
//                    return -1;
//                  else
//                    return 1;
//                } else {
//                  String t1Name = (String)t1.getProperty("name");
//                  String t2Name = (String)t2.getProperty("name");
//                  return t1Name.compareTo(t2Name);
//                }
//              }
//            };
//
//        Collections.sort(matchedTags, c);
//
//        List<TorcVertex> topTags = matchedTags.subList(0, Math.min(matchedTags.size(), limit));
//
//        for (int i = 0; i < topTags.size(); i++) {
//          TorcVertex t = topTags.get(i);
//
//          result.add(new LdbcQuery4Result(
//                (String)t.getProperty("name"),
//                tagCounts.get(t).intValue()));
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person, find the Forums which that Person’s friends and
//   * friends of friends (excluding start Person) became Members of after a
//   * given date. Return top 20 Forums, and the number of Posts in each Forum
//   * that was Created by any of these Persons. For each Forum consider only
//   * those Persons which joined that particular Forum after the given date.
//   * Sort results descending by the count of Posts, and then ascending by Forum
//   * identifier.[1]
//   */
//  public static class LdbcQuery5Handler
//      implements OperationHandler<LdbcQuery5, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery5 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery5Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          result.add(new LdbcQuery5Result(
//              null,
//              0));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//
//      // Parameters of this query
//      final long personId = op.personId();
//      final long minDate = op.minDate().getTime();
//      final int limit = op.limit();
//
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery5Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//        
//        TraversalResult l1_friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");
//        TraversalResult l2_friends = graph.traverse(l1_friends, "knows", Direction.OUT, false, "Person");
//
//        Set<TorcVertex> friends = new HashSet<>(l1_friends.vSet.size() + l2_friends.vSet.size());
//        friends.addAll(l1_friends.vSet);
//        friends.addAll(l2_friends.vSet);
//        friends.remove(start);
//
//        TraversalResult friendForums = graph.traverse(friends, "hasMember", Direction.IN, true, "Forum");
//
//        // Filter out all edges with joinDate <= minDate
//        GraphHelper.removeEdgeIf(friendForums, (v, p) -> { 
//          if ((Long)p.get("joinDate") <= minDate)
//            return true;
//          else 
//            return false;
//        });
//
//        // Invert the friendForums mapping so we get a list of all the friends
//        // that joined a given forum after a certain date.
//        Map<TorcVertex, Set<TorcVertex>> forumFriends = new HashMap<>(friendForums.vSet.size());
//        for (TorcVertex friend : friendForums.vMap.keySet()) {
//          List<TorcVertex> forums = friendForums.vMap.get(friend);
//          for (TorcVertex forum : forums) {
//            if (forumFriends.containsKey(forum))
//              forumFriends.get(forum).add(friend);
//            else {
//              Set<TorcVertex> fSet = new HashSet<>();
//              fSet.add(friend);
//              forumFriends.put(forum, fSet);
//            }
//          }
//        }
//
//        TraversalResult forumPosts = graph.traverse(friendForums, "containerOf", Direction.OUT, false, "Post");
//        TraversalResult postAuthor = graph.traverse(forumPosts, "hasCreator", Direction.OUT, false, "Person");
//        TraversalResult forumAuthors = GraphHelper.fuse(forumPosts, postAuthor, false);
//
//        Map<TorcVertex, Integer> forumFriendPostCounts = new HashMap<>(forumAuthors.vMap.size());
//        for (TorcVertex forum : friendForums.vSet) {
//          if (forumAuthors.vMap.containsKey(forum)) {
//            List<TorcVertex> authors = forumAuthors.vMap.get(forum);
//            authors.retainAll(forumFriends.get(forum));
//            forumFriendPostCounts.put(forum, authors.size());
//          } else {
//            forumFriendPostCounts.put(forum, 0);
//          }
//        }
//
//        List<TorcVertex> forums = new ArrayList<>(forumFriendPostCounts.keySet());
//
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex v1, TorcVertex v2) {
//                Integer forum1FriendPostCount = forumFriendPostCounts.get(v1);
//                Integer forum2FriendPostCount = forumFriendPostCounts.get(v2);
//
//                if (forum1FriendPostCount != forum2FriendPostCount) {
//                  // Post count sort is descending
//                  if (forum1FriendPostCount > forum2FriendPostCount)
//                    return -1;
//                  else
//                    return 1;
//                } else {
//                  Long v1Id = v1.id().getLowerLong();
//                  Long v2Id = v2.id().getLowerLong();
//                  // IDs are ascending
//                  if (v1Id > v2Id)
//                    return 1;
//                  else
//                    return -1;
//                }
//              }
//            };
//
//        Collections.sort(forums, c);
//
//        // Take top limit
//        forums = forums.subList(0, Math.min(forums.size(), limit));
//
//        graph.fillProperties(forums);
//
//        for (int i = 0; i < forums.size(); i++) {
//          TorcVertex forum = forums.get(i);
//
//          result.add(new LdbcQuery5Result(
//              (String)forum.getProperty("title"), 
//              forumFriendPostCounts.get(forum)));
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person and some Tag, find the other Tags that occur together
//   * with this Tag on Posts that were created by start Person’s friends and
//   * friends of friends (excluding start Person). Return top 10 Tags, and the
//   * count of Posts that were created by these Persons, which contain both this
//   * Tag and the given Tag. Sort results descending by count, and then
//   * ascending by Tag name.[1]
//   */
//  public static class LdbcQuery6Handler
//      implements OperationHandler<LdbcQuery6, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery6 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery6Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          result.add(new LdbcQuery6Result(
//              null,
//              0));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//
//      // Parameters of this query
//      final long personId = op.personId();
//      final String tagName = op.tagName();
//      final int limit = op.limit();
//
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery6Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//
//        TraversalResult l1_friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");
//        TraversalResult l2_friends = graph.traverse(l1_friends, "knows", Direction.OUT, false, "Person");
//
//        Set<TorcVertex> friends = new HashSet<>(l1_friends.vSet.size() + l2_friends.vSet.size());
//        friends.addAll(l1_friends.vSet);
//        friends.addAll(l2_friends.vSet);
//        friends.remove(start);
//
//        TraversalResult posts = graph.traverse(friends, "hasCreator", Direction.IN, false, "Post");
//        TraversalResult tags = graph.traverse(posts, "hasTag", Direction.OUT, false, "Tag");
//
//        graph.fillProperties(tags);
//
//        Map<TorcVertex, Long> coTagCounts = new HashMap<>();
//        for (TorcVertex p : tags.vMap.keySet()) {
//          boolean hasTag = false;
//          for (TorcVertex t : tags.vMap.get(p)) {
//            if (((String)t.getProperty("name")).equals(tagName)) {
//              hasTag = true;
//              break;
//            }
//          }
//
//          if (hasTag) {
//            for (TorcVertex t : tags.vMap.get(p)) {
//              if (!((String)t.getProperty("name")).equals(tagName)) {
//                if (coTagCounts.containsKey(t)) {
//                  coTagCounts.put(t, coTagCounts.get(t) + 1);
//                } else {
//                  coTagCounts.put(t, 1L);
//                }
//              }
//            } 
//          }
//        }
//
//        List<TorcVertex> coTags = new ArrayList<>(coTagCounts.keySet());
//
//        // Sort tags by count
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex t1, TorcVertex t2) {
//                Long t1Count = coTagCounts.get(t1);
//                Long t2Count = coTagCounts.get(t2);
//
//                if (t1Count != t2Count) {
//                  // Tag count sort is descending
//                  if (t1Count > t2Count)
//                    return -1;
//                  else
//                    return 1;
//                } else {
//                  String t1Name = (String)t1.getProperty("name");
//                  String t2Name = (String)t2.getProperty("name");
//                  return t1Name.compareTo(t2Name);
//                }
//              }
//            };
//
//        Collections.sort(coTags, c);
//
//        List<TorcVertex> topCoTags = coTags.subList(0, Math.min(coTags.size(), limit));
//
//        for (int i = 0; i < topCoTags.size(); i++) {
//          TorcVertex t = topCoTags.get(i);
//
//          result.add(new LdbcQuery6Result(
//                (String)t.getProperty("name"),
//                coTagCounts.get(t).intValue()));
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person, find (most recent) Likes on any of start Person’s
//   * Posts/Comments. Return top 20 Persons that Liked any of start Person’s
//   * Posts/Comments, the Post/Comment they liked most recently, creation date
//   * of that Like, and the latency (in minutes) between creation of
//   * Post/Comment and Like. Additionally, return a flag indicating whether the
//   * liker is a friend of start Person. In the case that a Person Liked
//   * multiple Posts/Comments at the same time, return the Post/Comment with
//   * lowest identifier. Sort results descending by creation time of Like, then
//   * ascending by Person identifier of liker.[1]
//   */
//  public static class LdbcQuery7Handler
//      implements OperationHandler<LdbcQuery7, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery7 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery7Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
//          int n2 = ThreadLocalRandom.current().nextInt(0, messageIDs.size());
//          Long pid = personIDs.get(n1);
//          Long mid = messageIDs.get(n2);
//          result.add(new LdbcQuery7Result(
//              pid,
//              null,
//              null,
//              0,
//              mid,
//              null,
//              0,
//              false));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//      
//      // Parameters of this query
//      final long personId = op.personId();
//      final int limit = op.limit();
//      
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery7Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//
//        TraversalResult friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");        
//        TraversalResult messages = graph.traverse(start, "hasCreator", Direction.IN, false, "Post", "Comment");
//        TraversalResult likes = graph.traverse(messages, "likes", Direction.IN, true, "Person");
//
//        Map<TorcVertex, Long> personMostRecentLikeDate = new HashMap<>();
//        Map<TorcVertex, TorcVertex> personMostRecentLikeMsg = new HashMap<>();
//        Long minLikeDate = Long.MAX_VALUE;
//        int numMinLikeDates = 0;
//        for (TorcVertex msg : likes.vMap.keySet()) {
//          List<TorcVertex> likers = likes.vMap.get(msg);
//          List<Map<Object, Object>> likeProps = likes.pMap.get(msg);
//
//          for (int i = 0; i < likers.size(); i++) {
//            TorcVertex liker = likers.get(i);
//            Long likeDate = (Long)likeProps.get(i).get("creationDate");
//            if (personMostRecentLikeDate.containsKey(liker)) {
//              // We already have a most recent like date registered for this
//              // person. Check if the new like date is more recent and, if so,
//              // update the map. Also check if this changes the least recent
//              // like date contained in the map.
//              Long currLikeDate = personMostRecentLikeDate.get(liker);
//              if (currLikeDate < likeDate) {
//                personMostRecentLikeDate.put(liker, likeDate);
//                personMostRecentLikeMsg.put(liker, msg);
//                if (currLikeDate == minLikeDate) {
//                  if (numMinLikeDates == 1) { 
//                    Long newMinLikeDate = Long.MAX_VALUE;
//                    for (Long date : personMostRecentLikeDate.values()) {
//                      if (date < newMinLikeDate) {
//                        newMinLikeDate = date;
//                        numMinLikeDates = 1;
//                      } else if (date == newMinLikeDate) {
//                        numMinLikeDates++;
//                      }
//                    }
//                    minLikeDate = newMinLikeDate;
//                  } else {
//                    numMinLikeDates--;
//                  }
//                }
//              } else if (currLikeDate == likeDate) {
//                // In this case when a person has liked more than one message at
//                // the same time, we are to choose the message that has the
//                // lower identifier.
//                TorcVertex currMsg = personMostRecentLikeMsg.get(liker);
//                if (msg.id().getLowerLong() < currMsg.id().getLowerLong())
//                  personMostRecentLikeMsg.put(liker, msg);
//              }
//            } else if (personMostRecentLikeDate.size() < limit) {
//              // If haven't collected enough people yet, and we have someone we
//              // haven't seen before here, then automatically insert them into
//              // the map.
//              personMostRecentLikeDate.put(liker, likeDate);
//              personMostRecentLikeMsg.put(liker, msg);
//              if (likeDate < minLikeDate) {
//                minLikeDate = likeDate;
//                numMinLikeDates = 1;
//              } else if (likeDate == minLikeDate) {
//                numMinLikeDates++;
//              }
//            } else {
//              // The map is full of "limit" entries and we haven't seen this
//              // person before. If the likeDate is less recent than our current
//              // minimum, then we can reject this entry outright. If the
//              // likeDate is equal to our current minimum, then we just keep it,
//              // and we'll sort out the minimums by vertex ID in the end to
//              // figure out which ones make it into the final result. Otherwise,
//              // if the likeDate is more recent than the minimum, then we add
//              // it, and check if the number above the minimum has hit our
//              // limit... in this case we can cut off the minimums entirely.
//              if (likeDate < minLikeDate) {
//                continue;
//              } else if (likeDate == minLikeDate) {
//                personMostRecentLikeDate.put(liker, likeDate);
//                personMostRecentLikeMsg.put(liker, msg);
//                numMinLikeDates++;
//              } else {
//                personMostRecentLikeDate.put(liker, likeDate);
//                personMostRecentLikeMsg.put(liker, msg);
//
//                if (personMostRecentLikeDate.size() - numMinLikeDates >= limit) {
//                  Map<TorcVertex, Long> newPersonMostRecentLikeDate = new HashMap<>();
//                  Map<TorcVertex, TorcVertex> newPersonMostRecentLikeMsg = new HashMap<>();
//
//                  Long newMinLikeDate = Long.MAX_VALUE;
//                  for (TorcVertex v : personMostRecentLikeDate.keySet()) {
//                    Long date = personMostRecentLikeDate.get(v);
//                    if (date != minLikeDate) {
//                      newPersonMostRecentLikeDate.put(v, date);
//                      newPersonMostRecentLikeMsg.put(v, personMostRecentLikeMsg.get(v));
//
//                      if (date < newMinLikeDate) {
//                        newMinLikeDate = date;
//                        numMinLikeDates = 1;
//                      } else if (date == newMinLikeDate) {
//                        numMinLikeDates++;
//                      }
//                    }
//                  }
//
//                  personMostRecentLikeDate = newPersonMostRecentLikeDate;
//                  personMostRecentLikeMsg = newPersonMostRecentLikeMsg;
//                  minLikeDate = newMinLikeDate;
//                }
//              }
//            }
//          }
//        }
//
//        List<TorcVertex> likersList = new ArrayList<>(personMostRecentLikeDate.keySet());
//
//        // Sort the likers by their creation date (descending in creationDate
//        // and ascending in id).
//        final Map<TorcVertex, Long> likeDates = personMostRecentLikeDate;
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex v1, TorcVertex v2) {
//                Long v1likeDate = likeDates.get(v1);
//                Long v2likeDate = likeDates.get(v2);
//                if (v1likeDate > v2likeDate)
//                  return -1;
//                else if (v1likeDate < v2likeDate)
//                  return 1;
//                else if (v1.id().getLowerLong() > v2.id().getLowerLong())
//                  return 1;
//                else
//                  return -1;
//              }
//            };
//
//        Collections.sort(likersList, c);
//        
//        List<TorcVertex> topLikers = likersList.subList(0, Math.min(likersList.size(), limit));
//
//        graph.fillProperties(topLikers);
//
//        List<TorcVertex> msgList = new ArrayList<>(topLikers.size());
//
//        for (TorcVertex tLiker : topLikers) 
//          msgList.add(personMostRecentLikeMsg.get(tLiker));
//
//        graph.fillProperties(msgList);
//
//        for (int i = 0; i < topLikers.size(); i++) {
//          TorcVertex liker = topLikers.get(i);
//          Long likeDate = personMostRecentLikeDate.get(liker);
//          TorcVertex msg = personMostRecentLikeMsg.get(liker);
//
//          String content = (String)msg.getProperty("content");
//          if (content.equals(""))
//            content = (String)msg.getProperty("imageFile");
//
//          Long latencyMinutes = 
//            (likeDate - (Long)msg.getProperty("creationDate")) / (1000l * 60l);
//
//          result.add(new LdbcQuery7Result(
//              liker.id().getLowerLong(), 
//              (String)liker.getProperty("firstName"),
//              (String)liker.getProperty("lastName"),
//              likeDate,
//              msg.id().getLowerLong(),
//              content,
//              latencyMinutes.intValue(),
//              !friends.vSet.contains(liker)));
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person, find (most recent) Comments that are replies to
//   * Posts/Comments of the start Person. Only consider immediate (1-hop)
//   * replies, not the transitive (multi-hop) case. Return the top 20 reply
//   * Comments, and the Person that created each reply Comment. Sort results
//   * descending by creation date of reply Comment, and then ascending by
//   * identifier of reply Comment.[1]
//   */
//  public static class LdbcQuery8Handler
//      implements OperationHandler<LdbcQuery8, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery8 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery8Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
//          int n2 = ThreadLocalRandom.current().nextInt(0, messageIDs.size());
//          Long pid = personIDs.get(n1);
//          Long mid = messageIDs.get(n2);
//          result.add(new LdbcQuery8Result(
//              pid,
//              null,
//              null,
//              0,
//              mid,
//              null));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//      
//      // Parameters of this query
//      final long personId = op.personId();
//      final int limit = op.limit();
//      
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery8Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//
//        TraversalResult posts = graph.traverse(start, "hasCreator", Direction.IN, false, "Post", "Comment");
//
//        TraversalResult replies = graph.traverse(posts, "replyOf", Direction.IN, false, "Post", "Comment");
//
//        graph.fillProperties(replies.vSet, "creationDate");
//
//        // Sort the replies by their creation date.
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex v1, TorcVertex v2) {
//                Long v1creationDate = ((Long)v1.getProperty("creationDate"));
//                Long v2creationDate = ((Long)v2.getProperty("creationDate"));
//                if (v1creationDate > v2creationDate)
//                  return 1;
//                else if (v1creationDate < v2creationDate)
//                  return -1;
//                else if (v1.id().getLowerLong() > v2.id().getLowerLong())
//                  return -1;
//                else
//                  return 1;
//              }
//            };
//
//        PriorityQueue<TorcVertex> pq = new PriorityQueue(limit, c);
//        for (TorcVertex r : replies.vSet) {
//          Long creationDate = (Long)r.getProperty("creationDate");
//         
//          if (pq.size() < limit) {
//            pq.add(r);
//            continue;
//          }
//
//          if (creationDate > (Long)pq.peek().getProperty("creationDate")) {
//            pq.add(r);
//            pq.poll();
//          }
//        }
//
//        // Create a list from the priority queue. This list will contain the
//        // messages in reverse order.
//        List<TorcVertex> replyList = new ArrayList<>(pq.size());
//        while (pq.size() > 0)
//          replyList.add(pq.poll());
//
//        TraversalResult authors = graph.traverse(replyList, "hasCreator", Direction.OUT, false, "Person");
//
//        graph.fillProperties(authors);
//        graph.fillProperties(replyList);
//
//        for (int i = replyList.size()-1; i >= 0; i--) {
//          TorcVertex r = replyList.get(i);
//          TorcVertex a = authors.vMap.get(r).get(0);
//
//          String content = (String)r.getProperty("content");
//          if (content.equals(""))
//            content = (String)r.getProperty("imageFile");
//
//          result.add(new LdbcQuery8Result(
//                a.id().getLowerLong(),
//                (String)a.getProperty("firstName"),
//                (String)a.getProperty("lastName"),
//                (Long)r.getProperty("creationDate"),
//                r.id().getLowerLong(),
//                content));
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person, find the (most recent) Posts/Comments created by
//   * that Person’s friends or friends of friends (excluding start Person). Only
//   * consider the Posts/Comments created before a given date (excluding that
//   * date). Return the top 20 Posts/Comments, and the Person that created each
//   * of those Posts/Comments. Sort results descending by creation date of
//   * Post/Comment, and then ascending by Post/Comment identifier.[1]
//   */
//  public static class LdbcQuery9Handler
//      implements OperationHandler<LdbcQuery9, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery9 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery9Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
//          int n2 = ThreadLocalRandom.current().nextInt(0, messageIDs.size());
//          Long pid = personIDs.get(n1);
//          Long mid = messageIDs.get(n2);
//          result.add(new LdbcQuery9Result(
//              pid,
//              null,
//              null,
//              mid,
//              null,
//              0));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//
//      // Parameters of this query
//      final long personId = op.personId();
//      final long maxDate = op.maxDate().getTime();
//      final int limit = op.limit();
//      
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery9Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//
//        TraversalResult l1_friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");
//        TraversalResult l2_friends = graph.traverse(l1_friends, "knows", Direction.OUT, false, "Person");
//
//        Set<TorcVertex> friends = new HashSet<>(l1_friends.vSet.size() + l2_friends.vSet.size());
//        friends.addAll(l1_friends.vSet);
//        friends.addAll(l2_friends.vSet);
//        friends.remove(start);
//
//        TraversalResult messages = graph.traverse(friends, "hasCreator", Direction.IN, false, "Post", "Comment");
//        
//        graph.fillProperties(messages.vSet, "creationDate");
//
//        // Sort the Posts and Comments by their creation date.
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex v1, TorcVertex v2) {
//                Long v1creationDate = ((Long)v1.getProperty("creationDate"));
//                Long v2creationDate = ((Long)v2.getProperty("creationDate"));
//                if (v1creationDate > v2creationDate)
//                  return 1;
//                else if (v1creationDate < v2creationDate)
//                  return -1;
//                else if (v1.id().getLowerLong() > v2.id().getLowerLong())
//                  return -1;
//                else
//                  return 1;
//              }
//            };
//
//        PriorityQueue<TorcVertex> pq = new PriorityQueue(limit, c);
//        for (TorcVertex m : messages.vSet) {
//          Long creationDate = (Long)m.getProperty("creationDate");
//         
//          if (creationDate >= maxDate)
//            continue;
//
//          if (pq.size() < limit) {
//            pq.add(m);
//            continue;
//          }
//
//          if (creationDate > (Long)pq.peek().getProperty("creationDate")) {
//            pq.add(m);
//            pq.poll();
//          }
//        }
//
//        // Create a list from the priority queue. This list will contain the
//        // messages in reverse order.
//        List<TorcVertex> msgList = new ArrayList<>(pq.size());
//        while (pq.size() > 0)
//          msgList.add(pq.poll());
//
//        // Wish there was a good way to go back and find the authors from what
//        // we have already read, but we don't have a great way to do that now,
//        // so go and read the authors.
//        TraversalResult authors = graph.traverse(msgList, "hasCreator", Direction.OUT, false, "Person");
//
//        graph.fillProperties(authors);
//        graph.fillProperties(msgList);
//
//        for (int i = msgList.size()-1; i >= 0; i--) {
//          TorcVertex m = msgList.get(i);
//          TorcVertex f = authors.vMap.get(m).get(0);
//
//          String content = (String)m.getProperty("content");
//          if (content.equals(""))
//            content = (String)m.getProperty("imageFile");
//
//          result.add(new LdbcQuery9Result(
//              f.id().getLowerLong(), //((UInt128)t.get().get("personId")).getLowerLong(),
//              ((String)f.getProperty("firstName")), //(String)t.get().get("firstName"), 
//              ((String)f.getProperty("lastName")), //(String)t.get().get("lastName"),
//              m.id().getLowerLong(), //((UInt128)t.get().get("messageId")).getLowerLong(), 
//              content, //(String)t.get().get("content"),
//              ((Long)m.getProperty("creationDate")))); //Long.valueOf((String)t.get().get("creationDate"))))
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person, find that Person’s friends of friends (excluding
//   * start Person, and immediate friends), who were born on or after the 21st
//   * of a given month (in any year) and before the 22nd of the following month.
//   * Calculate the similarity between each of these Persons and start Person,
//   * where similarity for any Person is defined as follows:
//   * <ul>
//   * <li>common = number of Posts created by that Person, such that the Post
//   * has a Tag that start Person is Interested in</li>
//   * <li>uncommon = number of Posts created by that Person, such that the Post
//   * has no Tag that start Person is Interested in</li>
//   * <li>similarity = common - uncommon</li>
//   * </ul>
//   * Return top 10 Persons, their Place, and their similarity score. Sort
//   * results descending by similarity score, and then ascending by Person
//   * identifier.[1]
//   */
//  public static class LdbcQuery10Handler
//      implements OperationHandler<LdbcQuery10, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery10 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery10Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
//          Long pid = personIDs.get(n1);
//          result.add(new LdbcQuery10Result(
//              pid,
//              null,
//              null,
//              0,
//              null,
//              null));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//      
//      // Parameters of this query
//      final long personId = op.personId();
//      final int month = op.month() - 1; // make month zero based
//      final int limit = op.limit();
//
//      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
//
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery10Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//        TraversalResult l1_friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");
//        TraversalResult l2_friends = graph.traverse(l1_friends, "knows", Direction.OUT, false, "Person");
//
//        l2_friends.vSet.removeAll(l1_friends.vSet);
//        l2_friends.vSet.remove(start);
//
//        graph.fillProperties(l2_friends.vSet, "birthday"); 
//
//        // Filter by birthday
//        l2_friends.vSet.removeIf(f -> {
//          calendar.setTimeInMillis((Long)f.getProperty("birthday"));
//          int bmonth = calendar.get(Calendar.MONTH); // zero based 
//          int bday = calendar.get(Calendar.DAY_OF_MONTH); // starts with 1
//          if ((bmonth == month && bday >= 21) || 
//              (bmonth == ((month + 1) % 12) && bday < 22)) {
//            return false;
//          }
//          return true;
//        });
//
//        TraversalResult posts = graph.traverse(l2_friends.vSet, "hasCreator", Direction.IN, false, "Post");
//        TraversalResult tags = graph.traverse(posts, "hasTag", Direction.OUT, false, "Tag");
//
//        TraversalResult interests = graph.traverse(start, "hasInterest", Direction.OUT, false, "Tag");
//
//        // For each l2 friend calculate the similarity score.
//        Map<TorcVertex, Long> similarityScore = new HashMap<>();
//        for (TorcVertex f : l2_friends.vSet) {
//          if (posts.vMap.containsKey(f)) {
//            long common = 0;
//            long uncommon = 0;
//            for (TorcVertex p : posts.vMap.get(f)) {
//              if (tags.vMap.containsKey(p)) {
//                for (TorcVertex t : tags.vMap.get(p)) {
//                  if (interests.vSet.contains(t)) {
//                    common++;
//                    break;
//                  }
//                }
//              }
//            }
//            uncommon = posts.vMap.get(f).size() - common;
//            similarityScore.put(f, new Long(common - uncommon));
//          } else {
//            similarityScore.put(f, new Long(0L));
//          }
//        }
//
//        // Sort the friends by their similarity score
//        // Here the comparator defines an ascending order because the priority
//        // queue's head is the first element in sorted order, which we would
//        // like to be the least element.
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex v1, TorcVertex v2) {
//                Long v1similarityScore = similarityScore.get(v1);
//                Long v2similarityScore = similarityScore.get(v2);
//                if (v1similarityScore > v2similarityScore)
//                  return 1;
//                else if (v1similarityScore < v2similarityScore)
//                  return -1;
//                else if (v1.id().getLowerLong() > v2.id().getLowerLong())
//                  return -1;
//                else
//                  return 1;
//              }
//            };
//
//        PriorityQueue<TorcVertex> pq = new PriorityQueue(limit, c);
//        for (TorcVertex f : l2_friends.vSet) {
//          Long score = (Long)similarityScore.get(f);
//         
//          if (pq.size() < limit) {
//            pq.add(f);
//            continue;
//          }
//
//          if (score > similarityScore.get(pq.peek())) {
//            pq.add(f);
//            pq.poll();
//          } else if (score.equals(similarityScore.get(pq.peek())) && 
//              f.id().getLowerLong() < pq.peek().id().getLowerLong()) {
//            pq.add(f);
//            pq.poll();
//          }
//        }
//
//        // Create a list from the priority queue. This list will contain the
//        // results in reverse order.
//        List<TorcVertex> fList = new ArrayList<>(pq.size());
//        while (pq.size() > 0)
//          fList.add(pq.poll());
//
//        graph.fillProperties(fList);
//
//        TraversalResult locations = graph.traverse(fList, "isLocatedIn", Direction.OUT, false, "Place");
//
//        graph.fillProperties(locations);
//
//        for (int i = fList.size()-1; i >= 0; i--) {
//          TorcVertex f = fList.get(i);
//
//          result.add(new LdbcQuery10Result(
//                f.id().getLowerLong(),
//                (String)f.getProperty("firstName"),
//                (String)f.getProperty("lastName"),
//                similarityScore.get(f).intValue(),
//                (String)f.getProperty("gender"),
//                (String)locations.vMap.get(f).get(0).getProperty("name")));
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person, find that Person’s friends and friends of friends
//   * (excluding start Person) who started Working in some Company in a given
//   * Country, before a given date (year). Return top 10 Persons, the Company
//   * they worked at, and the year they started working at that Company. Sort
//   * results ascending by the start date, then ascending by Person identifier,
//   * and lastly by Organization name descending.[1]
//   */
//  public static class LdbcQuery11Handler
//      implements OperationHandler<LdbcQuery11, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery11 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery11Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
//          Long pid = personIDs.get(n1);
//          result.add(new LdbcQuery11Result(
//              pid,
//              null,
//              null,
//              null,
//              0));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//      
//      // Parameters of this query
//      final long personId = op.personId();
//      final String countryName = op.countryName();
//      final int workFromYear = op.workFromYear();
//      final int limit = op.limit();
//
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      class ResultTuple {
//        public int year;
//        public TorcVertex v;
//        public String name;
//
//        public ResultTuple(int year, TorcVertex v, String name) {
//          this.year = year;
//          this.v = v;
//          this.name = name;
//        }
//      };
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery11Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//        TraversalResult l1_friends = graph.traverse(start, "knows", Direction.OUT, false, "Person");
//        TraversalResult l2_friends = graph.traverse(l1_friends, "knows", Direction.OUT, false, "Person");
//
//        Set<TorcVertex> friends = new HashSet<>(l1_friends.vSet.size() + l2_friends.vSet.size());
//        friends.addAll(l1_friends.vSet);
//        friends.addAll(l2_friends.vSet);
//        friends.remove(start);
//       
//        TraversalResult company = graph.traverse(friends, "workAt", Direction.OUT, true, "Organisation");
//
//        GraphHelper.removeEdgeIf(company, (v, p) -> { 
//          if (((Integer)p.get("workFrom")).compareTo(workFromYear) >= 0)
//            return true;
//          else 
//            return false;
//        });
//
//        TraversalResult country = graph.traverse(company, "isLocatedIn", Direction.OUT, false, "Place");
//
//        graph.fillProperties(country.vSet, "name");
//
//        company.vSet.removeIf(c -> {
//          return !((String)country.vMap.get(c).get(0).getProperty("name")).equals(countryName);
//        });
//
//        graph.fillProperties(company.vSet, "name");
//
//        Comparator<ResultTuple> comparator = new Comparator<ResultTuple>() {
//              public int compare(ResultTuple a, ResultTuple b) {
//                if (a.year > b.year)
//                  return -1;
//                else if (a.year < b.year)
//                  return 1;
//                else if (a.v.id().getLowerLong() > b.v.id().getLowerLong())
//                  return -1;
//                else if (a.v.id().getLowerLong() < b.v.id().getLowerLong())
//                  return 1;
//                else
//                  return a.name.compareTo(b.name);
//              }
//            };
//
//        PriorityQueue<ResultTuple> pq = new PriorityQueue(limit, comparator);
//        for (TorcVertex f : company.vMap.keySet()) {
//          List<TorcVertex> cList = company.vMap.get(f);
//          List<Map<Object, Object>> pList = company.pMap.get(f);
//
//          for (int i = 0; i < cList.size(); i++) {
//            TorcVertex c = cList.get(i);
//            Map<Object, Object> p = pList.get(i);
//
//            if (!company.vSet.contains(c))
//              continue;
//            
//            int year = ((Integer)p.get("workFrom")).intValue();
//            String name = (String)c.getProperty("name");
//
//            if (pq.size() < limit) {
//              pq.add(new ResultTuple(year, f, name));
//              continue;
//            }
//
//            if (year < pq.peek().year) {
//              pq.add(new ResultTuple(year, f, name));
//              pq.poll();
//            } else if (year == pq.peek().year) {
//              if (f.id().getLowerLong() < pq.peek().v.id().getLowerLong()) {
//                pq.add(new ResultTuple(year, f, name));
//                pq.poll();
//              } else if (f.id().getLowerLong() == pq.peek().v.id().getLowerLong()) {
//                if (name.compareTo(pq.peek().name) > 0) {
//                  pq.add(new ResultTuple(year, f, name));
//                  pq.poll();
//                }
//              }
//            }
//          }
//        }
//
//        List<ResultTuple> rList = new ArrayList<>(pq.size());
//        Set<TorcVertex> fSet = new HashSet<>(pq.size());
//        while (pq.size() > 0) {
//          ResultTuple rt = pq.poll();
//          rList.add(rt);
//          fSet.add(rt.v);
//        }
//        
//        graph.fillProperties(fSet);
//
//        for (int i = rList.size()-1; i >= 0; i--) {
//          ResultTuple rt = rList.get(i);
//
//          result.add(new LdbcQuery11Result(
//                rt.v.id().getLowerLong(),
//                (String)rt.v.getProperty("firstName"),
//                (String)rt.v.getProperty("lastName"),
//                rt.name,
//                rt.year));
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given a start Person, find the Comments that this Person’s friends made in
//   * reply to Posts, considering only those Comments that are immediate (1-hop)
//   * replies to Posts, not the transitive (multi-hop) case. Only consider Posts
//   * with a Tag in a given TagClass or in a descendent of that TagClass. Count
//   * the number of these reply Comments, and collect the Tags (with valid tag
//   * class) that were attached to the Posts they replied to. Return top 20
//   * Persons with at least one reply, the reply count, and the collection of
//   * Tags. Sort results descending by Comment count, and then ascending by
//   * Person identifier.[1]
//   */
//  public static class LdbcQuery12Handler
//      implements OperationHandler<LdbcQuery12, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery12 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery12Result> result = new ArrayList<>(op.limit());
//
//        for (int i = 0; i < op.limit(); i++) {
//          int n1 = ThreadLocalRandom.current().nextInt(0, personIDs.size());
//          Long pid = personIDs.get(n1);
//          result.add(new LdbcQuery12Result(
//              pid,
//              null,
//              null,
//              null,
//              0));
//        }
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//
//      // Parameters of this query
//      final long personId = op.personId();
//      final String tagClassName = op.tagClassName();
//      final int limit = op.limit();
//
//      final UInt128 torcPersonId = 
//          new UInt128(TorcEntity.PERSON.idSpace, personId);
//
//      TorcGraph graph = 
//        (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery12Result> result = new ArrayList<>(limit);
//
//        TorcVertex start = new TorcVertex(graph, torcPersonId);
//        TraversalResult startFriends = graph.traverse(start, "knows", Direction.OUT, false, "Person");
//        TraversalResult friendComments = graph.traverse(startFriends, "hasCreator", Direction.IN, false, "Comment");
//        TraversalResult commentPost = graph.traverse(friendComments, "replyOf", Direction.OUT, false, "Post");
//        TraversalResult postTags = graph.traverse(commentPost, "hasTag", Direction.OUT, false, "Tag");
//        TraversalResult tagClasses = graph.traverse(postTags, "hasType", Direction.OUT, false, "TagClass");
//
//        // Find all the tags that are of the given type. Here we will comb
//        // through the tagClasses and see which tags have the right type. The
//        // rest may just be of a subType, so we traverse up the hasType tree for
//        // the remaining tags.
//        Set<TorcVertex> matchingTags = new HashSet<>(tagClasses.vMap.size());
//        while (!tagClasses.vMap.isEmpty()) {
//          graph.fillProperties(tagClasses.vSet, "name");
//
//          tagClasses.vMap.entrySet().removeIf( e -> {
//              TorcVertex tag = (TorcVertex)e.getKey();
//              TorcVertex tagClass = ((List<TorcVertex>)e.getValue()).get(0);
//              
//              if (((String)tagClass.getProperty("name")).equals(tagClassName)) {
//                matchingTags.add(tag);
//                return true;
//              }
//
//              return false;
//            });
//
//          if (tagClasses.vMap.isEmpty())
//            break;
//
//          TraversalResult superTagClasses = graph.traverse(tagClasses, "hasType", Direction.OUT, false, "TagClass");
//          tagClasses = GraphHelper.fuse(tagClasses, superTagClasses, false);
//        }
//
//        // We only care about the tags of the given type.
//        GraphHelper.intersect(postTags, matchingTags);
//
//        // Create map of comment to the set of all matching tags that were on
//        // the post that the comment was in reply to.
//        TraversalResult commentTags = GraphHelper.fuse(commentPost, postTags, false);
//
//        // Filter for the comments that have non-zero matching tags.
//        GraphHelper.intersect(friendComments, commentTags.vMap.keySet());
//
//        // Create map of friend to the set of all matching tags that were on
//        // posts that the friend commented on.
//        TraversalResult friendTags = GraphHelper.fuse(friendComments, commentTags, true);
//
//        // Sort in the reverse order from the query result order so that the
//        // priority queue's "top" element is the least element.
//        Comparator<TorcVertex> c = new Comparator<TorcVertex>() {
//              public int compare(TorcVertex v1, TorcVertex v2) {
//                int v1CommentCount = friendComments.vMap.get(v1).size();
//                int v2CommentCount = friendComments.vMap.get(v2).size();
//
//                if (v1CommentCount != v2CommentCount)
//                  return v1CommentCount - v2CommentCount;
//                else
//                  return -1 * v1.id().compareTo(v2.id());
//              }
//            };
//
//        PriorityQueue<TorcVertex> pq = new PriorityQueue(limit, c);
//        for (TorcVertex f : friendComments.vMap.keySet()) {
//          int commentCount = friendComments.vMap.get(f).size();
//
//          if (pq.size() < limit) {
//            pq.add(f);
//            continue;
//          }
//
//          if (commentCount > friendComments.vMap.get(pq.peek()).size()) {
//            pq.add(f);
//            pq.poll();
//          }
//        }
//
//        // Create a list from the priority queue. This list will contain the
//        // friends in reverse order.
//        List<TorcVertex> topFriends = new ArrayList<>(pq.size());
//        while (pq.size() > 0)
//          topFriends.add(pq.poll());
//
//        // Fill in the properties for our results.
//        graph.fillProperties(topFriends);
//        graph.fillProperties(friendTags.vSet, "name");
//
//        for (int i = topFriends.size()-1; i >= 0; i--) {
//          TorcVertex f = topFriends.get(i);
//          List<TorcVertex> tags = friendTags.vMap.get(f);
//
//          List<String> tagNames = new ArrayList<>(tags.size());
//          for (TorcVertex v : tags)
//            tagNames.add(((String)v.getProperty("name")));
//
//          result.add(new LdbcQuery12Result(
//              f.id().getLowerLong(),
//              ((String)f.getProperty("firstName")),
//              ((String)f.getProperty("lastName")),
//              tagNames,
//              friendComments.vMap.get(f).size()));
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given two Persons, find the shortest path between these two Persons in the
//   * subgraph induced by the Knows relationships. Return the length of this
//   * path. -1 should be returned if no path is found, and 0 should be returned
//   * if the start person is the same as the end person.[1]
//   */
//  public static class LdbcQuery13Handler
//      implements OperationHandler<LdbcQuery13, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery13 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        resultReporter.report(1, new LdbcQuery13Result(0), op);
//        return;
//      }
//      
//      // Parameters of this query
//      final long person1Id = op.person1Id();
//      final long person2Id = op.person2Id();
//
//      if (person1Id == person2Id) {
//        resultReporter.report(1, new LdbcQuery13Result(0), op);
//        return;        
//      }
//
//      final UInt128 torcPerson1Id = 
//          new UInt128(TorcEntity.PERSON.idSpace, person1Id);
//      final UInt128 torcPerson2Id = 
//          new UInt128(TorcEntity.PERSON.idSpace, person2Id);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        Set<TorcVertex> start = new HashSet<>();
//        start.add(new TorcVertex(graph, torcPerson1Id));
//
//        TorcVertex end = new TorcVertex(graph, torcPerson2Id);
//
//        TraversalResult friends = new TraversalResult(null, null, start);
//        Set<TorcVertex> seenSet = new HashSet<>();
//        int n = 1;
//        do {
//          friends = graph.traverse(friends, "knows", Direction.OUT, false, "Person");
//          GraphHelper.subtract(friends, seenSet);
//          
//          // No path to destination vertex.
//          if (friends.vSet.size() == 0) {
//            n = -1;
//            break;
//          }
//
//          if (friends.vSet.contains(end))
//            break;
//
//          seenSet.addAll(friends.vSet);
//
//          n++;
//        } while (true);
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(1, new LdbcQuery13Result(n), op);
//        break;
//      }
//    }
//  }
//
//  /**
//   * Given two Persons, find all (unweighted) shortest paths between these two
//   * Persons, in the subgraph induced by the Knows relationship. Then, for each
//   * path calculate a weight. The nodes in the path are Persons, and the weight
//   * of a path is the sum of weights between every pair of consecutive Person
//   * nodes in the path. The weight for a pair of Persons is calculated such
//   * that every reply (by one of the Persons) to a Post (by the other Person)
//   * contributes 1.0, and every reply (by ones of the Persons) to a Comment (by
//   * the other Person) contributes 0.5. Return all the paths with shortest
//   * length, and their weights. Sort results descending by path weight. The
//   * order of paths with the same weight is unspecified.[1]
//   */
//  public static class LdbcQuery14Handler
//      implements OperationHandler<LdbcQuery14, DbConnectionState> {
//
//    @Override
//    public void executeOperation(final LdbcQuery14 op,
//        DbConnectionState dbConnState,
//        ResultReporter resultReporter) throws DbException {
//      if (fakeComplexReads) {
//        List<LdbcQuery14Result> result = new ArrayList<>(1);
//        
//        List<Long> personIDsInPath = new ArrayList<>(2);
//        personIDsInPath.add(op.person1Id());
//        personIDsInPath.add(op.person2Id());
//
//        result.add(new LdbcQuery14Result(
//            personIDsInPath,
//            42.0));
//
//        resultReporter.report(result.size(), result, op);
//        return;
//      }
//
//      // Define a linked-list datatype for paths of vertices.
//      class VertexPath {
//        public TorcVertex v;
//        public VertexPath p;
//
//        public VertexPath(TorcVertex v, VertexPath p) {
//          this.v = v;
//          this.p = p;
//        }
//
//        @Override
//        public int hashCode() {
//          if (p != null)
//            return v.hashCode() ^ p.hashCode();
//          else
//            return v.hashCode();
//        }
//
//        @Override
//        public boolean equals(final Object object) {
//          if (object instanceof VertexPath) {
//            VertexPath other = (VertexPath)object;
//            if (p != null)
//              return this.v.id().equals(other.v.id()) && this.p.equals(other.p);
//            else
//              return this.v.id().equals(other.v.id());
//          }
//
//          return false;
//        }
//      };
//
//      // Define a vertex pair map key.
//      class VertexPair {
//        public TorcVertex v1;
//        public TorcVertex v2;
//
//        public VertexPair(TorcVertex v1, TorcVertex v2) {
//          this.v1 = v1;
//          this.v2 = v2;
//        }
//
//        @Override
//        public int hashCode() {
//          return v1.hashCode() ^ v2.hashCode();
//        }
//
//        @Override
//        public boolean equals(final Object object) {
//          if (object instanceof VertexPair) {
//            VertexPair other = (VertexPair)object;
//            return this.v1.id().equals(other.v1.id()) &&
//                    this.v2.id().equals(other.v2.id());
//          }
//
//          return false;
//        }
//
//        @Override
//        public String toString() {
//          return String.format("(%X,%X)", v1.id().getLowerLong(), v2.id().getLowerLong());
//        }
//      };
//
//      // Parameters of this query
//      final long person1Id = op.person1Id();
//      final long person2Id = op.person2Id();
//
//      final UInt128 torcPerson1Id = 
//          new UInt128(TorcEntity.PERSON.idSpace, person1Id);
//      final UInt128 torcPerson2Id = 
//          new UInt128(TorcEntity.PERSON.idSpace, person2Id);
//
//      TorcGraph graph = (TorcGraph)((TorcDb2ConnectionState) dbConnState).getGraph();
//
//      int txAttempts = 0;
//      while (txAttempts < MAX_TX_ATTEMPTS) {
//        GraphTraversalSource g = graph.traversal();
//
//        if (!(doTransactionalReads || useRAMCloudTransactionAPIForReads))
//          graph.disableTx();
//
//        List<LdbcQuery14Result> result = new ArrayList<>();
//
//        TorcVertex start = new TorcVertex(graph, torcPerson1Id);
//        TorcVertex end = new TorcVertex(graph, torcPerson2Id);
//
//        Set<TorcVertex> startSet = new HashSet<>();
//        startSet.add(new TorcVertex(graph, torcPerson1Id));
//
//        // Handle start == end here
//
//        TraversalResult friends = new TraversalResult(null, null, startSet);
//        Set<TorcVertex> seenSet = new HashSet<>();
//
//        // Keep around each of the traversal results during the serach.
//        List<TraversalResult> trList = new ArrayList<>();
//        int hops = 0;
//        while (!friends.vSet.contains(end)) {
//          seenSet.addAll(friends.vSet);
//
//          friends = graph.traverse(friends, "knows", Direction.OUT, false, "Person");
//          GraphHelper.subtract(friends, seenSet);
//
//          // No path to destination vertex.
//          if (friends.vSet.size() == 0) {
//            hops = -1;
//            break;
//          }
//
//          trList.add(friends);
//          
//          hops++;
//        }
//
//        if (hops != -1) {
//          // Filter for paths that lead to the end vertex.
//          for (int i = trList.size()-1; i >= 0; i--) {
//            if (i == trList.size()-1)
//              GraphHelper.intersect(trList.get(i), end);
//            else
//              GraphHelper.intersect(trList.get(i), trList.get(i+1).vMap.keySet());
//          }
//
//          // Create cache of calculated paths so we don't unnecessarily
//          // recalculate them.
//          Map<TorcVertex, List<VertexPath>> pathCache = new HashMap<>();
//          for (int i = trList.size()-1; i >= 0; i--) {
//            for (TorcVertex b : trList.get(i).vMap.keySet()) {
//              List<VertexPath> paths = new ArrayList<>();
//              for (TorcVertex n : trList.get(i).vMap.get(b)) {
//                if (!pathCache.containsKey(n)) {
//                  List<VertexPath> p = new ArrayList<>();
//                  p.add(new VertexPath(n, null));
//                  pathCache.put(n, p);
//                }
//
//                for (VertexPath path : pathCache.get(n)) {
//                  paths.add(new VertexPath(b, path));
//                }
//              }
//
//              pathCache.put(b, paths);
//            }
//          }
//
//          List<VertexPath> paths = pathCache.get(start);
//
//          // Calculate the path weights.
//          Map<VertexPair, Double> pairWeights = new HashMap<>();
//          Map<VertexPath, Double> pathWeights = new HashMap<>();
//          Map<TorcVertex, TraversalResult[]> traversalResultCache = new HashMap<>();
//          for (int i = 0; i < paths.size(); i++) {
//            VertexPath path = paths.get(i);
//            double pathWeight = 0.0;
//            while (path != null) {
//              if (path.p != null) {
//                VertexPair vpair = new VertexPair(path.v, path.p.v);
//                
//                if (!pairWeights.containsKey(vpair)) {
//                  double pairWeight = 0.0;
//                 
//                  TraversalResult v1p;
//                  TraversalResult v1c;
//                  TraversalResult v1crp;
//                  TraversalResult v1crc;
//                  if (traversalResultCache.containsKey(vpair.v1)) {
//                    TraversalResult results[] = traversalResultCache.get(vpair.v1);
//                    v1p = results[0];
//                    v1c = results[1];
//                    v1crp = results[2];
//                    v1crc = results[3];
//                  } else {
//                    v1p = graph.traverse(vpair.v1, "hasCreator", Direction.IN, false, "Post");
//                    v1c = graph.traverse(vpair.v1, "hasCreator", Direction.IN, false, "Comment");
//                    v1crp = graph.traverse(v1c, "replyOf", Direction.OUT, false, "Post");
//                    v1crc = graph.traverse(v1c, "replyOf", Direction.OUT, false, "Comment");
//                    TraversalResult results[] = new TraversalResult[4];
//                    results[0] = v1p;
//                    results[1] = v1c;
//                    results[2] = v1crp;
//                    results[3] = v1crc;
//                    traversalResultCache.put(vpair.v1, results);
//                  }
//
//                  TraversalResult v2p;
//                  TraversalResult v2c;
//                  TraversalResult v2crp;
//                  TraversalResult v2crc;
//                  if (traversalResultCache.containsKey(vpair.v2)) {
//                    TraversalResult results[] = traversalResultCache.get(vpair.v2);
//                    v2p = results[0];
//                    v2c = results[1];
//                    v2crp = results[2];
//                    v2crc = results[3];
//                  } else {
//                    v2p = graph.traverse(vpair.v2, "hasCreator", Direction.IN, false, "Post");
//                    v2c = graph.traverse(vpair.v2, "hasCreator", Direction.IN, false, "Comment");
//                    v2crp = graph.traverse(v2c, "replyOf", Direction.OUT, false, "Post");
//                    v2crc = graph.traverse(v2c, "replyOf", Direction.OUT, false, "Comment");
//                    TraversalResult results[] = new TraversalResult[4];
//                    results[0] = v2p;
//                    results[1] = v2c;
//                    results[2] = v2crp;
//                    results[3] = v2crc;
//                    traversalResultCache.put(vpair.v2, results);
//                  }
//
//                  // First calculate weights of v1's comments on v2's junk.
//                  for (TorcVertex c : v1crp.vMap.keySet()) {
//                    TorcVertex rp = v1crp.vMap.get(c).get(0);
//                    if (v2p.vSet.contains(rp))
//                      pairWeight += 1.0;
//                  }
//
//                  for (TorcVertex c : v1crc.vMap.keySet()) {
//                    TorcVertex rc = v1crc.vMap.get(c).get(0);
//                    if (v2c.vSet.contains(rc))
//                      pairWeight += 0.5;
//                  }
//
//                  // Now do v2's comments on v1's junk.
//                  for (TorcVertex c : v2crp.vMap.keySet()) {
//                    TorcVertex rp = v2crp.vMap.get(c).get(0);
//                    if (v1p.vSet.contains(rp))
//                      pairWeight += 1.0;
//                  }
//
//                  for (TorcVertex c : v2crc.vMap.keySet()) {
//                    TorcVertex rc = v2crc.vMap.get(c).get(0);
//                    if (v1c.vSet.contains(rc))
//                      pairWeight += 0.5;
//                  }
//
//                  pairWeights.put(vpair, pairWeight);
//                }
//
//                pathWeight += pairWeights.get(vpair);
//              }
//
//              path = path.p;
//            }
//
//            pathWeights.put(paths.get(i), pathWeight);
//          }
//
//          Comparator<VertexPath> c = new Comparator<VertexPath>() {
//                public int compare(VertexPath p1, VertexPath p2) {
//                  Double p1Weight = pathWeights.get(p1);
//                  Double p2Weight = pathWeights.get(p2);
//          
//                  if (p2Weight > p1Weight)
//                    return 1;
//                  else
//                    return -1;
//                }
//              };
//
//          Collections.sort(paths, c);
//
//          for (int i = 0; i < paths.size(); i++) {
//            VertexPath path = paths.get(i);
//            List<Long> ids = new ArrayList<>();
//            while (path != null) {
//              ids.add(path.v.id().getLowerLong());
//              path = path.p;
//            }
//
//            result.add(new LdbcQuery14Result(ids, pathWeights.get(paths.get(i))));
//          }
//        }
//
//        if (doTransactionalReads) {
//          try {
//            graph.tx().commit();
//          } catch (RuntimeException e) {
//            txAttempts++;
//            continue;
//          }
//        } else if (useRAMCloudTransactionAPIForReads) {
//          graph.tx().rollback();
//        } else {
//          graph.enableTx();
//        }
//
//        resultReporter.report(result.size(), result, op);
//        break;
//      }
//    }
//  }
//
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

    @Override
    public void executeOperation(final LdbcShortQuery1PersonProfile op,
        DbConnectionState dbConnState,
        ResultReporter resultReporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;

      Graph graph = cState.getGraph();

      Vertex person = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.personId()), 
          TorcEntity.PERSON.label);

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.fillProperties(person);

        TraversalResult place = graph.traverse(person, "isLocatedIn", Direction.OUT, false, 
            "Place");

        LdbcShortQuery1PersonProfileResult result =
            new LdbcShortQuery1PersonProfileResult(
                (String)person.getProperty("firstName"),
                (String)person.getProperty("lastName"),
                (Long)person.getProperty("birthday"),
                (String)person.getProperty("locationIP"),
                (String)person.getProperty("browserUsed"),
                place.vMap.get(person).get(0).id().getLowerLong(),
                (String)person.getProperty("gender"),
                (Long)person.getProperty("creationDate"));

        if (graph.commitTx()) {
          resultReporter.report(1, result, op);
          break;
        }

        txAttempts++;
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

    @Override
    public void executeOperation(final LdbcShortQuery2PersonPosts op,
        DbConnectionState dbConnState,
        ResultReporter resultReporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;

      Graph graph = cState.getGraph();

      Vertex person = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.personId()), 
          TorcEntity.PERSON.label);

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>();

        TraversalResult messages = graph.traverse(person, "hasCreator", Direction.IN, false, 
            "Post", "Comment");

        graph.fillProperties(messages.vSet, "creationDate");

        // Sort the Posts and Comments descending by creationDate, and descending by message
        // identifier. Comparator defines the reverse order because we use it in a PriorityQueue to
        // efficiently implement finding the top-K results.
        Comparator<Vertex> c = new Comparator<Vertex>() {
              public int compare(Vertex v1, Vertex v2) {
                Long v1creationDate = ((Long)v1.getProperty("creationDate"));
                Long v2creationDate = ((Long)v2.getProperty("creationDate"));
                if (v1creationDate > v2creationDate)
                  return 1;
                else if (v1creationDate < v2creationDate)
                  return -1;
                else if (v1.id().getLowerLong() > v2.id().getLowerLong())
                  return 1;
                else
                  return -1;
              }
            };

        PriorityQueue<Vertex> pq = new PriorityQueue(op.limit(), c);
        for (Vertex m : messages.vSet) {
          Long creationDate = (Long)m.getProperty("creationDate");
         
          if (pq.size() < op.limit()) {
            pq.add(m);
            continue;
          }

          if (creationDate > (Long)pq.peek().getProperty("creationDate")) {
            pq.add(m);
            pq.poll();
          }
        }

        // Create a list from the priority queue with the elements sorted in the desired order.
        LinkedList<Vertex> msgList = new LinkedList<>();
        while (pq.size() > 0)
          msgList.addFirst(pq.poll());

        // Traverse "replyOf" relationships until we find the ancestor Post of all the Comment
        // messages in our result set.
        LinkedList<TraversalResult> trList = new LinkedList<>();
        trList.addLast(new TraversalResult(null, null, new HashSet<>(msgList)));
        Set<Vertex> postSet = new HashSet<>();
        while (true) {
          List<Vertex> cList = new ArrayList<>();
          for (Vertex v : trList.getLast().vSet)
            if (v.label().equals("Comment"))
              cList.add(v);
            else
              if (trList.size() > 1)
                postSet.add(v);

          if (cList.size() > 0) 
            trList.addLast(graph.traverse(cList, "replyOf", Direction.OUT, false, "Post", "Comment"));
          else
            break;
        }
        trList.removeFirst();

        TraversalResult originalAuthors = 
          graph.traverse(postSet, "hasCreator", Direction.OUT, false, "Person");

        List<Vertex> propFetch = new ArrayList<>(msgList.size() + postSet.size() + 
            originalAuthors.vSet.size() + 1);
        propFetch.addAll(msgList);
        propFetch.addAll(postSet);
        propFetch.addAll(originalAuthors.vSet);
        propFetch.add(person);
        graph.fillProperties(propFetch);

        for (int i = 0; i < msgList.size(); i++) {
          Vertex m = msgList.get(i);

          String content = (String)m.getProperty("content");
          if (content.equals(""))
            content = (String)m.getProperty("imageFile");

          long originalPostId = -1;
          long originalPostAuthorId = -1;
          String originalPostAuthorFirstName = "";
          String originalPostAuthorLastName = "";
          if (m.label().equals("Post")) {
            originalPostId = m.id().getLowerLong();
            originalPostAuthorId = person.id().getLowerLong();
            originalPostAuthorFirstName = (String)person.getProperty("firstName");
            originalPostAuthorLastName = (String)person.getProperty("lastName");
          } else {
            Vertex base = m;
            for (int j = 0; j < trList.size(); j++) {
              TraversalResult tr = trList.get(j);
              Vertex replyToV = tr.vMap.get(base).get(0);
              if (replyToV.label().equals("Post")) {
                Vertex author = originalAuthors.vMap.get(replyToV).get(0);
                originalPostId = replyToV.id().getLowerLong();
                originalPostAuthorId = author.id().getLowerLong();
                originalPostAuthorFirstName = (String)author.getProperty("firstName");
                originalPostAuthorLastName = (String)author.getProperty("lastName");
                break;
              }

              base = replyToV;
            }
          }

          result.add(new LdbcShortQuery2PersonPostsResult(
                  m.id().getLowerLong(), //messageId,
                  content, //messageContent,
                  (Long)m.getProperty("creationDate"), //messageCreationDate,
                  originalPostId,
                  originalPostAuthorId,
                  originalPostAuthorFirstName,
                  originalPostAuthorLastName));
        }

        if (graph.commitTx()) {
          resultReporter.report(result.size(), result, op);
          break;
        }

        txAttempts++;
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

    @Override
    public void executeOperation(final LdbcShortQuery3PersonFriends op,
        DbConnectionState dbConnState,
        ResultReporter resultReporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;

      Graph graph = cState.getGraph();

      Vertex person = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.personId()), 
          TorcEntity.PERSON.label);

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();

        TraversalResult friends = graph.traverse(person, "knows", Direction.OUT, true, "Person");

        List<Vertex> friendList = friends.vMap.get(person);

        // Create map from friend to creationDate to use for sorting.
        final Map<Vertex, Long> friendshipDate = new HashMap<>(friendList.size());
        for (int i = 0; i < friendList.size(); i++) {
          Vertex friend = friendList.get(i);
          Map<Object, Object> edgeProps = friends.pMap.get(person).get(i);
          Long creationDate = (Long)edgeProps.get("creationDate");
          friendshipDate.put(friend, creationDate);
        }
        
        // Sort friends descending by creationDate, and ascending by friend identifier.
        Comparator<Vertex> c = new Comparator<Vertex>() {
              public int compare(Vertex v1, Vertex v2) {
                Long v1creationDate = friendshipDate.get(v1);
                Long v2creationDate = friendshipDate.get(v2);
                if (v1creationDate.compareTo(v2creationDate) != 0)
                  return -1*v1creationDate.compareTo(v2creationDate);
                else
                  if (v1.id().getLowerLong() > v2.id().getLowerLong())
                    return 1;
                  else
                    return -1;
              }
            };
        
        Collections.sort(friendList, c);

        graph.fillProperties(friendList);

        for (int i = 0; i < friendList.size(); i++) {
          Vertex f = friendList.get(i);
          result.add(new LdbcShortQuery3PersonFriendsResult(
                  f.id().getLowerLong(),
                  (String)f.getProperty("firstName"),
                  (String)f.getProperty("lastName"),
                  friendshipDate.get(f).longValue()));
        }

        if (graph.commitTx()) {
          resultReporter.report(result.size(), result, op);
          break;
        }

        txAttempts++;
      }
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve its content and creation
   * date.[1]
   */
  public static class LdbcShortQuery4MessageContentHandler implements
      OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {

    @Override
    public void executeOperation(final LdbcShortQuery4MessageContent op,
        DbConnectionState dbConnState,
        ResultReporter resultReporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;

      Graph graph = cState.getGraph();

      // So this is an interesting case. LDBC SNB treats ID spaces and labels seaparately, and this
      // is a perfect example. Here, Message is an ID space, and "Comment" and "Post" are labels
      // that vertices in the "Message" ID space can have. But in TorcDB2, we think of labels
      // themselves as the ID space. So since we don't actually know the label in this case we need
      // to read both, but we rely on the fact that the message ID that's provided refers to either
      // a Post or a Comment, but not both. For now we just do a trick where we read an imaginary
      // "Messaged" labeled vertex.
      Vertex message = new Vertex(new UInt128(TorcEntity.COMMENT.idSpace, op.messageId()), 
          "Message");

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.fillProperties(message);
        
        Map<Object, Object> props = message.getProperties();
         
        String content = (String)props.get("content");
        if (content.equals(""))
          content = (String)props.get("imageFile");
        
        long creationDate = ((Long)props.get("creationDate")).longValue();

        LdbcShortQuery4MessageContentResult result =
            new LdbcShortQuery4MessageContentResult(
                content,
                creationDate);

        if (graph.commitTx()) {
          resultReporter.report(1, result, op);
          break;
        }

        txAttempts++;
      }
    }
  }

  /**
   * Given a Message (Post or Comment), retrieve its author.[1]
   */
  public static class LdbcShortQuery5MessageCreatorHandler implements
      OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {

    @Override
    public void executeOperation(final LdbcShortQuery5MessageCreator op,
        DbConnectionState dbConnState,
        ResultReporter resultReporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;

      Graph graph = cState.getGraph();

      // So this is an interesting case. LDBC SNB treats ID spaces and labels seaparately, and this
      // is a perfect example. Here, Message is an ID space, and "Comment" and "Post" are labels
      // that vertices in the "Message" ID space can have. But in TorcDB2, we think of labels
      // themselves as the ID space. So since we don't actually know the label in this case we need
      // to read both, but we rely on the fact that the message ID that's provided refers to either
      // a Post or a Comment, but not both. For now we just do a trick where we read an imaginary
      // "Messaged" labeled vertex.
      Vertex message = new Vertex(new UInt128(TorcEntity.COMMENT.idSpace, op.messageId()), 
          "Message");

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        TraversalResult creator = graph.traverse(message, "hasCreator", Direction.OUT, false, 
            "Person");

        Vertex author = creator.vMap.get(message).get(0);

        graph.fillProperties(author);

        LdbcShortQuery5MessageCreatorResult result =
            new LdbcShortQuery5MessageCreatorResult(
                author.id().getLowerLong(),
                (String)author.getProperty("firstName"),
                (String)author.getProperty("lastName"));

        if (graph.commitTx()) {
          resultReporter.report(1, result, op);
          break;
        }

        txAttempts++;
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

    @Override
    public void executeOperation(final LdbcShortQuery6MessageForum op,
        DbConnectionState dbConnState,
        ResultReporter resultReporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;

      Graph graph = cState.getGraph();

      // So this is an interesting case. LDBC SNB treats ID spaces and labels seaparately, and this
      // is a perfect example. Here, Message is an ID space, and "Comment" and "Post" are labels
      // that vertices in the "Message" ID space can have. But in TorcDB2, we think of labels
      // themselves as the ID space. So since we don't actually know the label in this case we need
      // to read both, but we rely on the fact that the message ID that's provided refers to either
      // a Post or a Comment, but not both. For now we just do a trick where we read an imaginary
      // "Messaged" labeled vertex.
      Vertex message = new Vertex(new UInt128(TorcEntity.COMMENT.idSpace, op.messageId()), 
          "Message");

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        // Traverse replyOf relationships to get post, forum, and moderator.
        Vertex v = message;
        Vertex post = null;
        Vertex forum = null;
        Vertex moderator = null;
        while (true) {
          TraversalResult replyOf = graph.traverse(v, "replyOf", Direction.OUT, false, "Post", "Comment");
          if (replyOf.vMap.size() == 0) {
            forum = graph.traverse(v, "containerOf", Direction.IN, false, "Forum").vMap.get(v).get(0);
            moderator = graph.traverse(forum, "hasModerator", Direction.OUT, false, "Person").vMap.get(forum).get(0);
            break;
          }
          v = replyOf.vMap.get(v).get(0);
        }

        graph.fillProperties(forum, moderator);

        LdbcShortQuery6MessageForumResult result = 
          new LdbcShortQuery6MessageForumResult(
                forum.id().getLowerLong(),
                (String)forum.getProperty("title"),
                moderator.id().getLowerLong(),
                (String)moderator.getProperty("firstName"),
                (String)moderator.getProperty("lastName"));

        if (graph.commitTx()) {
          resultReporter.report(1, result, op);
          break;
        }

        txAttempts++;
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

    @Override
    public void executeOperation(final LdbcShortQuery7MessageReplies op,
        DbConnectionState dbConnState,
        ResultReporter resultReporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;

      Graph graph = cState.getGraph();

      // So this is an interesting case. LDBC SNB treats ID spaces and labels seaparately, and this
      // is a perfect example. Here, Message is an ID space, and "Comment" and "Post" are labels
      // that vertices in the "Message" ID space can have. But in TorcDB2, we think of labels
      // themselves as the ID space. So since we don't actually know the label in this case we need
      // to read both, but we rely on the fact that the message ID that's provided refers to either
      // a Post or a Comment, but not both. For now we just do a trick where we read an imaginary
      // "Messaged" labeled vertex.
      Vertex message = new Vertex(new UInt128(TorcEntity.COMMENT.idSpace, op.messageId()), 
          "Message");

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        TraversalResult author = graph.traverse(message, "hasCreator", Direction.OUT, false, "Person");
        TraversalResult replies = graph.traverse(message, "replyOf", Direction.IN, false, "Comment");
        TraversalResult replyAuthors = graph.traverse(replies, "hasCreator", Direction.OUT, false, "Person");
        TraversalResult friends = graph.traverse(author, "knows", Direction.OUT, false, "Person");

        graph.fillProperties(replies, replyAuthors);

        List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();

        for (Vertex reply : replies.vMap.get(message)) {
          String content = (String)reply.getProperty("content");
          if (content.equals(""))
            content = (String)reply.getProperty("imageFile");

          Vertex replyAuthor = replyAuthors.vMap.get(reply).get(0);

          result.add(new LdbcShortQuery7MessageRepliesResult(
                  reply.id().getLowerLong(),
                  content,
                  (Long)reply.getProperty("creationDate"),
                  replyAuthor.id().getLowerLong(),
                  (String)replyAuthor.getProperty("firstName"),
                  (String)replyAuthor.getProperty("lastName"),
                  friends.vSet.contains(replyAuthor)));
        }

        // Sort results descending by creationDate, and ascending by author identifier.
        Comparator<LdbcShortQuery7MessageRepliesResult> c = new Comparator<LdbcShortQuery7MessageRepliesResult>() {
              public int compare(LdbcShortQuery7MessageRepliesResult r1, LdbcShortQuery7MessageRepliesResult r2) {
                long r1creationDate = r1.commentCreationDate();
                long r2creationDate = r2.commentCreationDate();
                if (r1creationDate > r2creationDate)
                  return -1;
                else if (r1creationDate < r2creationDate)
                  return 1;
                else if (r1.replyAuthorId() > r2.replyAuthorId())
                  return 1;
                else
                  return -1;
              }
            };
        
        Collections.sort(result, c);

        if (graph.commitTx()) {
          resultReporter.report(result.size(), result, op);
          break;
        }

        txAttempts++;
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

    private final Calendar calendar;

    public LdbcUpdate1AddPersonHandler() {
      this.calendar = new GregorianCalendar();
    }

    @Override
    public void executeOperation(LdbcUpdate1AddPerson op, DbConnectionState dbConnState,
        ResultReporter reporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;
      if (cState.fakeUpdates()) {
        reporter.report(0, LdbcNoResult.INSTANCE, op);
      }

      Graph graph = cState.getGraph();

      // Build key value properties map
      Map<Object, Object> props = new HashMap<>();
      props.put("firstName", op.personFirstName());
      props.put("lastName", op.personLastName());
      props.put("gender", op.gender());
      props.put("birthday", new Long(op.birthday().getTime()));
      props.put("creationDate", new Long(op.creationDate().getTime()));
      props.put("locationIP", op.locationIp());
      props.put("browserUsed", op.browserUsed());
      props.put("language", op.languages());
      props.put("email", op.emails());

      Vertex person = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.personId()), 
          TorcEntity.PERSON.label, props);

      Vertex place = new Vertex(new UInt128(TorcEntity.PLACE.idSpace, op.cityId()), 
          TorcEntity.PLACE.label, null);

      List<Vertex> tags = new ArrayList<>(op.tagIds().size());
      op.tagIds().forEach((tagid) -> tags.add(
            new Vertex(new UInt128(TorcEntity.TAG.idSpace, tagid), TorcEntity.TAG.label, null)));

      List<Vertex> universities = new ArrayList<>(op.studyAt().size());
      List<Map<Object, Object>> studyAtProps = new ArrayList<>(op.studyAt().size());
      op.studyAt().forEach((org) -> {
          universities.add(new Vertex(new UInt128(TorcEntity.ORGANISATION.idSpace, 
                  org.organizationId()), TorcEntity.ORGANISATION.label));
          Map<Object, Object> sprops = new HashMap<>();
          sprops.put("classYear", new Integer(org.year()));
          studyAtProps.add(sprops);
      });

      List<Vertex> companies = new ArrayList<>(op.workAt().size());
      List<Map<Object, Object>> workAtProps = new ArrayList<>(op.workAt().size());
      op.workAt().forEach((org) -> {
          companies.add(new Vertex(new UInt128(TorcEntity.ORGANISATION.idSpace, 
                  org.organizationId()), TorcEntity.ORGANISATION.label));
          Map<Object, Object> wprops = new HashMap<>();
          wprops.put("workFrom", new Integer(org.year()));
          workAtProps.add(wprops);
      });

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.addVertex(person);

        graph.addEdge(person, "isLocatedIn", place, null);

        for (Vertex tag : tags)
          graph.addEdge(person, "hasInterest", tag, null);

        for (int i = 0; i < universities.size(); i++)
          graph.addEdge(person, "studyAt", universities.get(i), studyAtProps.get(i));

        for (int i = 0; i < companies.size(); i++)
          graph.addEdge(person, "workAt", companies.get(i), workAtProps.get(i));

        if (graph.commitTx()) {
          reporter.report(0, LdbcNoResult.INSTANCE, op);
          break;
        }

        txAttempts++;
      }
    }
  }

  /**
   * Add a Like to a Post of the social network.[1]
   */
  public static class LdbcUpdate2AddPostLikeHandler implements
      OperationHandler<LdbcUpdate2AddPostLike, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate2AddPostLike op, DbConnectionState dbConnState,
        ResultReporter reporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;
      if (cState.fakeUpdates()) {
        reporter.report(0, LdbcNoResult.INSTANCE, op);
      }

      Graph graph = cState.getGraph();

      Vertex person = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.personId()), 
          TorcEntity.PERSON.label);
      Vertex post = new Vertex(new UInt128(TorcEntity.POST.idSpace, op.postId()),
          TorcEntity.POST.label);

      Map<Object, Object> eprops = new HashMap<>();
      eprops.put("creationDate", new Long(op.creationDate().getTime()));

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.addEdge(person, "likes", post, eprops);

        if (graph.commitTx()) {
          reporter.report(0, LdbcNoResult.INSTANCE, op);
          break;
        }

        txAttempts++;
      }
    }
  }

  /**
   * Add a Like to a Comment of the social network.[1]
   */
  public static class LdbcUpdate3AddCommentLikeHandler implements
      OperationHandler<LdbcUpdate3AddCommentLike, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate3AddCommentLike op,
        DbConnectionState dbConnState,
        ResultReporter reporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;
      if (cState.fakeUpdates()) {
        reporter.report(0, LdbcNoResult.INSTANCE, op);
      }

      Graph graph = cState.getGraph();

      Vertex person = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.personId()), 
          TorcEntity.PERSON.label);
      Vertex comment = new Vertex(new UInt128(TorcEntity.COMMENT.idSpace, op.commentId()),
          TorcEntity.COMMENT.label);

      Map<Object, Object> eprops = new HashMap<>();
      eprops.put("creationDate", new Long(op.creationDate().getTime()));

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.addEdge(person, "likes", comment, eprops);

        if (graph.commitTx()) {
          reporter.report(0, LdbcNoResult.INSTANCE, op);
          break;
        }

        txAttempts++;
      }
    }
  }

  /**
   * Add a Forum to the social network.[1]
   */
  public static class LdbcUpdate4AddForumHandler implements
      OperationHandler<LdbcUpdate4AddForum, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate4AddForum op,
        DbConnectionState dbConnState,
        ResultReporter reporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;
      if (cState.fakeUpdates()) {
        reporter.report(0, LdbcNoResult.INSTANCE, op);
      }

      Graph graph = cState.getGraph();

      // Build key value properties map
      Map<Object, Object> props = new HashMap<>();
      props.put("title", op.forumTitle());
      props.put("creationDate", new Long(op.creationDate().getTime()));
      
      Vertex forum = new Vertex(new UInt128(TorcEntity.FORUM.idSpace, op.forumId()), 
          TorcEntity.FORUM.label, props);

      List<Vertex> tags = new ArrayList<>(op.tagIds().size());
      op.tagIds().forEach((tagid) -> tags.add(
            new Vertex(new UInt128(TorcEntity.TAG.idSpace, tagid), TorcEntity.TAG.label)));

      Vertex moderator = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.moderatorPersonId()), 
          TorcEntity.PERSON.label);

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.addVertex(forum);

        for (Vertex tag : tags)
          graph.addEdge(forum, "hasTag", tag, null);

        graph.addEdge(forum, "hasModerator", moderator, null);

        if (graph.commitTx()) {
          reporter.report(0, LdbcNoResult.INSTANCE, op);
          break;
        }

        txAttempts++;
      }
    }
  }

  /**
   * Add a Forum membership to the social network.[1]
   */
  public static class LdbcUpdate5AddForumMembershipHandler implements
      OperationHandler<LdbcUpdate5AddForumMembership, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate5AddForumMembership op, DbConnectionState dbConnState,
        ResultReporter reporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;
      if (cState.fakeUpdates()) {
        reporter.report(0, LdbcNoResult.INSTANCE, op);
      }

      Graph graph = cState.getGraph();

      Vertex person = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.personId()), 
          TorcEntity.PERSON.label);
      Vertex forum = new Vertex(new UInt128(TorcEntity.FORUM.idSpace, op.forumId()), 
          TorcEntity.FORUM.label);

      Map<Object, Object> eprops = new HashMap<>();
      eprops.put("joinDate", new Long(op.joinDate().getTime()));

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.addEdge(forum, "hasMember", person, eprops);

        if (graph.commitTx()) {
          reporter.report(0, LdbcNoResult.INSTANCE, op);
          break;
        }

        txAttempts++;
      }
    }
  }

  /**
   * Add a Post to the social network.[1]
   */
  public static class LdbcUpdate6AddPostHandler implements
      OperationHandler<LdbcUpdate6AddPost, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate6AddPost op, DbConnectionState dbConnState,
        ResultReporter reporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;
      if (cState.fakeUpdates()) {
        reporter.report(0, LdbcNoResult.INSTANCE, op);
      }

      Graph graph = cState.getGraph();

      // Build key value properties map
      Map<Object, Object> props = new HashMap<>();
      props.put("imageFile", op.imageFile());
      props.put("creationDate", new Long(op.creationDate().getTime()));
      props.put("locationIP", op.locationIp());
      props.put("browserUsed", op.browserUsed());
      props.put("language", op.language());
      props.put("content", op.content());
      props.put("length", new Integer(op.length()));

      Vertex post = new Vertex(new UInt128(TorcEntity.POST.idSpace, op.postId()),
          TorcEntity.POST.label, props);
      Vertex author = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.authorPersonId()), 
          TorcEntity.PERSON.label);
      Vertex forum = new Vertex(new UInt128(TorcEntity.FORUM.idSpace, op.forumId()), 
          TorcEntity.FORUM.label, props);
      Vertex place = new Vertex(new UInt128(TorcEntity.PLACE.idSpace, op.countryId()), 
          TorcEntity.PLACE.label);

      List<Vertex> tags = new ArrayList<>(op.tagIds().size());
      op.tagIds().forEach((tagid) -> tags.add(
            new Vertex(new UInt128(TorcEntity.TAG.idSpace, tagid), TorcEntity.TAG.label)));

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.addVertex(post);
        graph.addEdge(post, "hasCreator", author, null);
        graph.addEdge(forum, "containerOf", post, null);
        graph.addEdge(post, "isLocatedIn", place, null);

        for (Vertex tag : tags)
          graph.addEdge(post, "hasTag", tag, null);

        if (graph.commitTx()) {
          reporter.report(0, LdbcNoResult.INSTANCE, op);
          break;
        }

        txAttempts++;
      }
    }
  }

  /**
   * Add a Comment replying to a Post/Comment to the social network.[1]
   */
  public static class LdbcUpdate7AddCommentHandler implements
      OperationHandler<LdbcUpdate7AddComment, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate7AddComment op, DbConnectionState dbConnState,
        ResultReporter reporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;
      if (cState.fakeUpdates()) {
        reporter.report(0, LdbcNoResult.INSTANCE, op);
      }

      Graph graph = cState.getGraph();

      // Build key value properties map
      Map<Object, Object> props = new HashMap<>();
      props.put("creationDate", new Long(op.creationDate().getTime()));
      props.put("locationIP", op.locationIp());
      props.put("browserUsed", op.browserUsed());
      props.put("content", op.content());
      props.put("length", new Integer(op.length()));

      Vertex comment = new Vertex(new UInt128(TorcEntity.COMMENT.idSpace, op.commentId()),
          TorcEntity.COMMENT.label, props);
      Vertex author = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.authorPersonId()),
          TorcEntity.PERSON.label);
      Vertex place = new Vertex(new UInt128(TorcEntity.PLACE.idSpace, op.countryId()), 
          TorcEntity.PLACE.label);

      Vertex message = null;
      if (op.replyToCommentId() != -1)
        message = new Vertex(new UInt128(TorcEntity.COMMENT.idSpace, op.replyToCommentId()),
            TorcEntity.COMMENT.label);
      else if (op.replyToPostId() != -1)
        message = new Vertex(new UInt128(TorcEntity.POST.idSpace, op.replyToPostId()),
            TorcEntity.POST.label);

      List<Vertex> tags = new ArrayList<>(op.tagIds().size());
      op.tagIds().forEach((tagid) -> tags.add(
            new Vertex(new UInt128(TorcEntity.TAG.idSpace, tagid), TorcEntity.TAG.label)));

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.addVertex(comment);
        graph.addEdge(comment, "hasCreator", author, null);
        graph.addEdge(comment, "isLocatedIn", place, null);
        graph.addEdge(comment, "replyOf", message, null);

        for (Vertex tag : tags)
          graph.addEdge(comment, "hasTag", tag, null);

        if (graph.commitTx()) {
          reporter.report(0, LdbcNoResult.INSTANCE, op);
          break;
        }

        txAttempts++;
      }
    }
  }

  /**
   * Add a friendship relation to the social network.[1]
   */
  public static class LdbcUpdate8AddFriendshipHandler implements
      OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate8AddFriendship op, DbConnectionState dbConnState,
        ResultReporter reporter) throws DbException {
      TorcDb2ConnectionState cState = (TorcDb2ConnectionState) dbConnState;
      if (cState.fakeUpdates()) {
        reporter.report(0, LdbcNoResult.INSTANCE, op);
      }

      Graph graph = cState.getGraph();

      // Build key value properties map
      Map<Object, Object> props = new HashMap<>();
      props.put("creationDate", new Long(op.creationDate().getTime()));

      Vertex person1 = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.person1Id()), 
          TorcEntity.PERSON.label);
      Vertex person2 = new Vertex(new UInt128(TorcEntity.PERSON.idSpace, op.person2Id()), 
          TorcEntity.PERSON.label);

      int txAttempts = 0;
      while (txAttempts < MAX_TX_ATTEMPTS) {
        graph.beginTx();

        graph.addEdge(person1, "knows", person2, props);
        graph.addEdge(person2, "knows", person1, props);

        if (graph.commitTx()) {
          reporter.report(0, LdbcNoResult.INSTANCE, op);
          break;
        }

        txAttempts++;
      }
    }
  }
}
