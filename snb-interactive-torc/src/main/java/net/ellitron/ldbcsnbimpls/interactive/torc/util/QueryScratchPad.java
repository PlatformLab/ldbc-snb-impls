/* 
 * Copyright (C) 2018 Stanford University
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
package net.ellitron.ldbcsnbimpls.interactive.torc.util;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.incr;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.decr;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.assign;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.mult;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.minus;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Pop.*;
import static org.apache.tinkerpop.gremlin.structure.Column.*;

import net.ellitron.torc.*;
import net.ellitron.torc.util.UInt128;
import net.ellitron.torc.TorcGraphProviderOptimizationStrategy;

import net.ellitron.ldbcsnbimpls.interactive.torc.*;

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

import org.apache.commons.configuration.BaseConfiguration;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import org.docopt.Docopt;

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
 * A scratch pad for playing around with Gremlin queries on LDBC SNB datasets.
 * Here you can play around with Germlin query construction in Java.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class QueryScratchPad {
  private static final String doc =
      "QueryScratchPad: A scratch pad for playing around with Gremlin queries\n"
      + "on LDBC SNB datasets.\n" 
      + "\n"
      + "Usage:\n"
      + "  QueryScratchPad [options] COORDLOC GRAPHNAME\n"
      + "  QueryScratchPad (-h | --help)\n"
      + "  QueryScratchPad --version\n"
      + "\n"
      + "Arguments:\n"
      + "  COORDLOC    RAMCloud coordinator locator string.\n"
      + "  GRAPHNAME   Name of the graph in RAMCloud to connect to.\n"
      + "\n"
      + "Options:\n"
      + "  --dpdkPort=<n>    DPDK port [default: -1].\n"
      + "  -h --help         Show this screen.\n"
      + "  --version         Show version.\n"
      + "\n";


  public static void main(String[] args) throws Exception {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("QueryScratchPad 1.0").parse(args);

    System.out.println(opts);

    String coordLoc = (String) opts.get("COORDLOC");
    String graphName = (String) opts.get("GRAPHNAME");
    int dpdkPort = Integer.decode((String) opts.get("--dpdkPort"));

    BaseConfiguration config = new BaseConfiguration();
    config.setDelimiterParsingDisabled(true);
    
    config.setProperty(
        TorcGraph.CONFIG_COORD_LOCATOR,
        coordLoc);
    
    config.setProperty(
        TorcGraph.CONFIG_GRAPH_NAME,
        graphName);

    config.setProperty(
        TorcGraph.CONFIG_DPDK_PORT,
        dpdkPort);

    Graph graph = TorcGraph.open(config);

    GraphTraversalSource g = graph.traversal();

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

    // Parameters of this query
    final long person1Id = 4398046511979L;
    final long person2Id = 2199023258358L;

    final UInt128 torcPerson1Id = 
        new UInt128(TorcEntity.PERSON.idSpace, person1Id);
    final UInt128 torcPerson2Id = 
        new UInt128(TorcEntity.PERSON.idSpace, person2Id);

    List<LdbcQuery14Result> result = new ArrayList<>();

    // First get the length of the shortest path
    Long minPathLen = g.V(torcPerson1Id)
      .repeat(outE("knows").inV().simplePath())
        .until(hasId(torcPerson2Id))
      .limit(1)
      .path()
      .count(local)
      .next();

    GraphTraversal gt = g.withSideEffect("result", result).V(torcPerson1Id)
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
                  return m.get(t.get()).get(0);
            }).sum())
      .order(local)
        .by(select(values))
      .project("personIdsInPath", 
          "pathWeight")
          .by(select(keys))
          .by(select(values))
      .map(t -> new LdbcQuery14Result(
          (Iterable<Number>)t.get().get("personIdsInPath"), 
          ((Double)((List)t.get().get("pathWeight")).get(0))))
      .store("result").iterate(); 

    long start = System.nanoTime();
    while (gt.hasNext()) {
      System.out.println(gt.next().toString());
    }
    long end = System.nanoTime();

    for (int i = 0; i < result.size(); i++) {
      System.out.println(result.get(i));
    }

    System.out.println(String.format("Query Time: %dms", (end-start)/1000000L));

    graph.close();
  }
}
