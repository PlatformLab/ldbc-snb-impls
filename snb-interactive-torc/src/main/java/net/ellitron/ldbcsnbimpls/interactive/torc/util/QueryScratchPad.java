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
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
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
   * Given a start Person and some Tag, find the other Tags that occur together
   * with this Tag on Posts that were created by start Person’s friends and
   * friends of friends (excluding start Person). Return top 10 Tags, and the
   * count of Posts that were created by these Persons, which contain both this
   * Tag and the given Tag. Sort results descending by count, and then
   * ascending by Tag name.[1]
   */

    long personId = 5497558140453L;
    String tagName = "Ovid";
    int limit = 10;

    final UInt128 torcPersonId = 
        new UInt128(TorcEntity.PERSON.idSpace, personId);

    List<LdbcQuery6Result> result = new ArrayList<>(limit);

    GraphTraversal gt = g.withSideEffect("result", result).V(torcPersonId).as("person")
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
