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
import net.ellitron.torc.util.*;
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
import org.apache.tinkerpop.gremlin.process.traversal.Path;
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

    // Parameters of this query
    final long personId = 17592186052613L;
    final String tagClassName = "BasketballPlayer";
    final int limit = 10;

    final UInt128 torcPersonId = 
        new UInt128(TorcEntity.PERSON.idSpace, personId);

    TorcGraph graph = TorcGraph.open(config);

    graph.disableTx();

//    List<LdbcQuery12Result> result = new ArrayList<>(limit);
//
//    TorcVertex start = new TorcVertex(graph, torcPersonId);
//    Map<TorcVertex, List<TorcVertex>> start_knows_person = graph.getVertices(start, "knows", Direction.OUT, "Person");
//    Map<TorcVertex, List<TorcVertex>> person_hasCreator_comment = graph.getVertices(start_knows_person, "hasCreator", Direction.IN, "Comment");
//    Map<TorcVertex, List<TorcVertex>> comment_replyOf_post = graph.getVertices(person_hasCreator_comment, "replyOf", Direction.OUT, "Post");
//    Map<TorcVertex, List<TorcVertex>> post_hasTag_tag = graph.getVertices(comment_replyOf_post, "hasTag", Direction.OUT, "Tag");
//    Map<TorcVertex, List<TorcVertex>> tag_hasType_tagClass = graph.getVertices(post_hasTag_tag, "hasType", Direction.OUT, "TagClass");
//
//    List<TorcVertex> filteredTags = new ArrayList<>(tag_hasType_tagClass.size());
//    while (!tag_hasType_tagClass.isEmpty()) {
//      graph.fillProperties(tag_hasType_tagClass);
//      tag_hasType_tagClass.entrySet().removeIf( e -> {
//          if (((List<TorcVertex>)e.getValue()).get(0).getProperty("name").get(0).equals(tagClassName)) {
//            filteredTags.add((TorcVertex)e.getKey());
//            return true;
//          }
//
//          return false;
//        });
//
//      if (!tag_hasType_tagClass.isEmpty()) {
//        Map<TorcVertex, List<TorcVertex>> tagClass_hasType_tagClass = graph.getVertices(tag_hasType_tagClass, "hasType", Direction.OUT, "TagClass");
//        tag_hasType_tagClass = TorcHelper.fuse(tag_hasType_tagClass, tagClass_hasType_tagClass, false);
//      } else {
//        break;
//      }
//    }
//
//    
//    TorcHelper.intersect(post_hasTag_tag, filteredTags); 
//
//    Map<TorcVertex, List<TorcVertex>> comment_assocTags_tags = TorcHelper.fuse(comment_replyOf_post, post_hasTag_tag, false);
//
//    List<TorcVertex> filteredComments = TorcHelper.keylist(comment_assocTags_tags);
//
//    TorcHelper.intersect(person_hasCreator_comment, filteredComments);
//
//    Map<TorcVertex, List<TorcVertex>> person_assocTags_tags = TorcHelper.fuse(person_hasCreator_comment, comment_assocTags_tags, true);
//
//    List<TorcVertex> friends = TorcHelper.keylist(person_hasCreator_comment);
//
//    friends.sort((a, b) -> {
//        int a_comments = person_hasCreator_comment.get(a).size();
//        int b_comments = person_hasCreator_comment.get(b).size();
//        if (b_comments != a_comments)
//          return b_comments - a_comments;
//        else
//          if (a.id().compareTo(b.id()) > 0)
//            return 1;
//          else
//            return -1;
//      });
//
//    friends.subList(0, Math.min(friends.size(), limit));
//
//    graph.fillProperties(friends);
//
//    graph.fillProperties(person_assocTags_tags);
//
//    for (int i = 0; i < friends.size(); i++) {
//      TorcVertex f = friends.get(i);
//      List<TorcVertex> tagVertices = person_assocTags_tags.get(f);
//      List<String> tagNameStrings = new ArrayList<>(tagVertices.size());
//      for (TorcVertex v : tagVertices) {
//        tagNameStrings.add(v.getProperty("name").get(0));
//      }
//      result.add(new LdbcQuery12Result(
//          f.id().getLowerLong(),
//          f.getProperty("firstName").get(0),
//          f.getProperty("lastName").get(0),
//          tagNameStrings,
//          person_hasCreator_comment.get(f).size()));
//    }

//    GraphTraversalSource g = graph.traversal();
//    GraphTraversal gt = 
//      g.withStrategies(TorcGraphProviderOptimizationStrategy.instance())
//        .withSideEffect("result", result).V(torcPersonId).as("person")
//        .out("knows").hasLabel("Person").as("friend")
//        .in("hasCreator").hasLabel("Comment").as("comment")
//        .out("replyOf").hasLabel("Post")
//        .out("hasTag").hasLabel("Tag")
//        .where(repeat(out("hasType").hasLabel("TagClass")).until(values("name").is(eq(tagClassName))))
//        .values("name")
//        .group()
//          .by(select("friend"))
//          .by(group()
//                .by(select("comment"))
//                .by(dedup().fold()))
//        .unfold()
//	.order()
//	  .by(select(values).count(local), decr)
//	  .by(select(keys).id(), incr)
//	.limit(limit)
//        .project("personId", 
//            "personFirstName",
//            "personLastName",
//            "tags", 
//            "count")
//          .by(select(keys).id())
//          .by(select(keys).values("firstName"))
//          .by(select(keys).values("lastName"))
//          .by(select(values).select(values).unfold().dedup())
//          .by(select(values).count(local))
//        .map(t -> new LdbcQuery12Result(
//            ((UInt128)t.get().get("personId")).getLowerLong(),
//            (String)t.get().get("personFirstName"), 
//            (String)t.get().get("personLastName"),
//            (Iterable<String>)t.get().get("tags"), 
//            ((Long)t.get().get("count")).intValue()))
//        .store("result").iterate(); 
//
//    long start = System.nanoTime();
//    while (gt.hasNext()) {
//      System.out.println(gt.next());
//    }
//    long end = System.nanoTime();

//    for (int i = 0; i < result.size(); i++) {
//      System.out.println(result.get(i));
//    }

    //System.out.println(String.format("Query Time: %dms", (end-start)/1000000L));

//    graph.close();
  }
}
