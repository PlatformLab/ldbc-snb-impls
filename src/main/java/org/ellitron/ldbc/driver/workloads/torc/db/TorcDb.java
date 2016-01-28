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
package org.ellitron.ldbc.driver.workloads.torc.db;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.ellitron.ldbc.driver.workloads.torc.Entity;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcGraph;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcVertex;
import org.ellitron.tinkerpop.gremlin.torc.structure.util.UInt128;
import org.ellitron.tinkerpop.gremlin.torc.structure.util.TorcHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TorcDb extends Db {

    private BasicDbConnectionState connectionState = null;

    static class BasicDbConnectionState extends DbConnectionState {

        private TorcGraph client;

        private BasicDbConnectionState(Map<String, String> properties) {
            BaseConfiguration config = new BaseConfiguration();
            config.setDelimiterParsingDisabled(true);
            config.setProperty(TorcGraph.CONFIG_GRAPH_NAME, properties.get("graphName"));
            config.setProperty(TorcGraph.CONFIG_COORD_LOCATOR, properties.get("coordinatorLocator"));
            config.setProperty(TorcGraph.CONFIG_NUM_MASTER_SERVERS, properties.get("numMasterServers"));

            client = TorcGraph.open(config);
        }

        public TorcGraph client() {
            return client;
        }

        @Override
        public void close() throws IOException {
            client.close();
        }
    }

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);
        registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonHandler.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeHandler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeHandler.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4AddForumHandler.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipHandler.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6AddPostHandler.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentHandler.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipHandler.class);
        
        connectionState = new BasicDbConnectionState(properties);
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
     * Query handler implementations.
     */
    public static class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery1PersonProfileHandler.class);
        
        @Override
        public void executeOperation(final LdbcShortQuery1PersonProfile operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            long person_id = operation.personId();
            TorcGraph client = dbConnectionState.client();

            TorcVertex root = (TorcVertex) client.vertices(new UInt128(Entity.PERSON.getNumber(), person_id)).next();
            Iterator<VertexProperty<String>> props = root.properties();
            Map<String, String> propertyMap = new HashMap<>();
            props.forEachRemaining((prop) -> {
                propertyMap.put(prop.key(), prop.value());
            });

            LdbcShortQuery1PersonProfileResult res = new LdbcShortQuery1PersonProfileResult(
                    propertyMap.get("firstName"), propertyMap.get("lastName"),
                    Long.parseLong(propertyMap.get("birthday")), propertyMap.get("locationIP"),
                    propertyMap.get("browserUsed"), Long.decode(propertyMap.get("place")), propertyMap.get("gender"),
                    Long.parseLong(propertyMap.get("creationDate")));
            
            resultReporter.report(0, res, operation);
        }

    }

    public static class LdbcUpdate1AddPersonHandler implements OperationHandler<LdbcUpdate1AddPerson, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate1AddPersonHandler.class);

        private final Calendar calendar;
                
        public LdbcUpdate1AddPersonHandler() {
            this.calendar = new GregorianCalendar();
        }
        
        @Override
        public void executeOperation(LdbcUpdate1AddPerson operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
//            int NUMTIMERS = 5;
//            long[][] timers = new long[NUMTIMERS][2];
//            for (int i = 0; i < NUMTIMERS; i++) {
//                timers[i][0] = 0;
//                timers[i][1] = 0;
//            }
//            timers[NUMTIMERS-1][0] = System.nanoTime();
            
            TorcGraph client = dbConnectionState.client();
            
//            timers[1][0] = System.nanoTime();
            List<Object> personKeyValues = new ArrayList<>(20);
            personKeyValues.add(T.id);
            personKeyValues.add(new UInt128(Entity.PERSON.getNumber(), operation.personId()));
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
            personKeyValues.add("place");
            personKeyValues.add(Long.toString(operation.cityId()));
//            timers[1][1] = System.nanoTime();
            
//            timers[3][0] = System.nanoTime();
            client.addVertex(personKeyValues.toArray());
            client.tx().commit();
//            timers[3][1] = System.nanoTime();

//            timers[NUMTIMERS-1][1] = System.nanoTime();
//            for (int i = 0; i < NUMTIMERS; i++)
//                System.out.println(String.format("LdbcUpdate1AddPersonHandler: %d: time: %dus", i, (timers[i][1] - timers[i][0])/1000l));

            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
    
    public static class LdbcUpdate2AddPostLikeHandler implements OperationHandler<LdbcUpdate2AddPostLike, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate2AddPostLikeHandler.class);

        @Override
        public void executeOperation(LdbcUpdate2AddPostLike operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            UInt128 personId = new UInt128(Entity.PERSON.getNumber(), operation.personId());
            UInt128 postId = new UInt128(Entity.POST.getNumber(), operation.postId());
            Iterator<Vertex> results = client.vertices(personId, postId);
            Vertex person = results.next();
            Vertex post = results.next();
            List<Object> keyValues = new ArrayList<>(2);
            keyValues.add("creationDate");
            keyValues.add(String.valueOf(operation.creationDate().getTime()));
            person.addEdge("likes", post, keyValues.toArray());
            client.tx().commit();
            
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
    
    public static class LdbcUpdate3AddCommentLikeHandler implements OperationHandler<LdbcUpdate3AddCommentLike, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate3AddCommentLikeHandler.class);

        @Override
        public void executeOperation(LdbcUpdate3AddCommentLike operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            UInt128 personId = new UInt128(Entity.PERSON.getNumber(), operation.personId());
            UInt128 commentId = new UInt128(Entity.COMMENT.getNumber(), operation.commentId());
            Iterator<Vertex> results = client.vertices(personId, commentId);
            Vertex person = results.next();
            Vertex comment = results.next();
            List<Object> keyValues = new ArrayList<>(2);
            keyValues.add("creationDate");
            keyValues.add(String.valueOf(operation.creationDate().getTime()));
            person.addEdge("likes", comment, keyValues.toArray());
            client.tx().commit();
            
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
    
    public static class LdbcUpdate4AddForumHandler implements OperationHandler<LdbcUpdate4AddForum, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate4AddForum operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();
            
            List<Object> forumKeyValues = new ArrayList<>(8);
            forumKeyValues.add(T.id);
            forumKeyValues.add(new UInt128(Entity.FORUM.getNumber(), operation.forumId()));
            forumKeyValues.add(T.label);
            forumKeyValues.add(Entity.FORUM.getName());
            forumKeyValues.add("title");
            forumKeyValues.add(operation.forumTitle());
            forumKeyValues.add("creationDate");
            forumKeyValues.add(String.valueOf(operation.creationDate().getTime()));
            
            Vertex forum = client.addVertex(forumKeyValues.toArray());
            
            List<UInt128> ids = new ArrayList<>(operation.tagIds().size() + 1);
            operation.tagIds().forEach((id) -> { 
                ids.add(new UInt128(Entity.TAG.getNumber(), id));
            });
            ids.add(new UInt128(Entity.PERSON.getNumber(), operation.moderatorPersonId()));
            
            client.vertices(ids.toArray()).forEachRemaining((v) -> {
                if (v.label().equals(Entity.TAG.getName())) {
                    forum.addEdge("hasTag", v);
                } else if (v.label().equals(Entity.PERSON.getName())) {
                    forum.addEdge("hasModerator", v);
                } else {
                    throw new RuntimeException("ERROR: LdbcUpdate4AddForum query tried to add an edge to a vertex that is neither a tag nor a person.");
                }
            });
            
            client.tx().commit();
            
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
    
    public static class LdbcUpdate5AddForumMembershipHandler implements OperationHandler<LdbcUpdate5AddForumMembership, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate5AddForumMembership operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            List<UInt128> ids = new ArrayList<>(2);
            ids.add(new UInt128(Entity.FORUM.getNumber(), operation.forumId()));
            ids.add(new UInt128(Entity.PERSON.getNumber(), operation.personId()));
            
            Iterator<Vertex> vItr = client.vertices(ids.toArray());
            Vertex forum = vItr.next();
            Vertex member = vItr.next();
            
            List<Object> edgeKeyValues = new ArrayList<>(2);
            edgeKeyValues.add("joinDate");
            edgeKeyValues.add(String.valueOf(operation.joinDate().getTime()));
            
            forum.addEdge("hasMember", member, edgeKeyValues.toArray());
            
            client.tx().commit();
            
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
    
    public static class LdbcUpdate6AddPostHandler implements OperationHandler<LdbcUpdate6AddPost, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate6AddPost operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            List<Object> postKeyValues = new ArrayList<>(18);
            postKeyValues.add(T.id);
            postKeyValues.add(new UInt128(Entity.POST.getNumber(), operation.postId()));
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
            postKeyValues.add("lang");
            postKeyValues.add(operation.language());
            postKeyValues.add("content");
            postKeyValues.add(operation.content());
            postKeyValues.add("length");
            postKeyValues.add(String.valueOf(operation.length()));
            
            Vertex post = client.addVertex(postKeyValues.toArray());
            
            List<UInt128> ids = new ArrayList<>(2);
            ids.add(new UInt128(Entity.PERSON.getNumber(), operation.authorPersonId()));
            ids.add(new UInt128(Entity.FORUM.getNumber(), operation.forumId()));
            ids.add(new UInt128(Entity.PLACE.getNumber(), operation.countryId()));
            operation.tagIds().forEach((id) -> { 
                ids.add(new UInt128(Entity.TAG.getNumber(), id));
            });
            
            client.vertices(ids.toArray()).forEachRemaining((v) -> {
                if (v.label().equals(Entity.PERSON.getName())) {
                    post.addEdge("hasCreator", v);
                } else if (v.label().equals(Entity.FORUM.getName())) {
                    v.addEdge("containerOf", post);
                } else if (v.label().equals(Entity.PLACE.getName())) {
                    post.addEdge("isLocatedIn", v);
                } else if (v.label().equals(Entity.TAG.getName())) {
                    post.addEdge("hasTag", v);
                } else {
                    throw new RuntimeException("ERROR: LdbcUpdate6AddPostHandler query tried to add an edge to a vertex that is none of {person, forum, place, tag}.");
                }
            });
            
            client.tx().commit();
            
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
    
    public static class LdbcUpdate7AddCommentHandler implements OperationHandler<LdbcUpdate7AddComment, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate7AddComment operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            List<Object> commentKeyValues = new ArrayList<>(14);
            commentKeyValues.add(T.id);
            commentKeyValues.add(new UInt128(Entity.COMMENT.getNumber(), operation.commentId()));
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
            
            List<UInt128> ids = new ArrayList<>(2);
            ids.add(new UInt128(Entity.PERSON.getNumber(), operation.authorPersonId()));
            ids.add(new UInt128(Entity.PLACE.getNumber(), operation.countryId()));
            operation.tagIds().forEach((id) -> { 
                ids.add(new UInt128(Entity.TAG.getNumber(), id));
            });
            if (operation.replyToCommentId() != -1) {
                ids.add(new UInt128(Entity.COMMENT.getNumber(), operation.replyToCommentId()));
            }
            if (operation.replyToPostId() != -1) {
                ids.add(new UInt128(Entity.POST.getNumber(), operation.replyToPostId()));
            }
            
            client.vertices(ids.toArray()).forEachRemaining((v) -> {
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
                    throw new RuntimeException("ERROR: LdbcUpdate7AddCommentHandler query tried to add an edge to a vertex that is none of {person, place, comment, post, tag}.");
                }
            });

            client.tx().commit();
            
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
    
    public static class LdbcUpdate8AddFriendshipHandler implements OperationHandler<LdbcUpdate8AddFriendship, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate8AddFriendship operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            List<Object> knowsEdgeKeyValues = new ArrayList<>(2);
            knowsEdgeKeyValues.add("creationDate");
            knowsEdgeKeyValues.add(String.valueOf(operation.creationDate().getTime()));
            
            List<UInt128> ids = new ArrayList<>(2);
            ids.add(new UInt128(Entity.PERSON.getNumber(), operation.person1Id()));
            ids.add(new UInt128(Entity.PERSON.getNumber(), operation.person2Id()));
            
            Iterator<Vertex> vItr = client.vertices(ids.toArray());
            
            Vertex person = vItr.next();
            Vertex friend = vItr.next();
            
            person.addEdge("knows", friend, knowsEdgeKeyValues);
            
            client.tx().commit();
            
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
}
