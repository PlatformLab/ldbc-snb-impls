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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.ellitron.ldbc.driver.workloads.torc.Entity;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcGraph;
import org.ellitron.tinkerpop.gremlin.torc.structure.util.UInt128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TorcDb extends Db {

    private BasicDbConnectionState connectionState = null;

    static class BasicDbConnectionState extends DbConnectionState {

        private Graph client;

        private BasicDbConnectionState(Map<String, String> properties) {
            BaseConfiguration config = new BaseConfiguration();
            config.setDelimiterParsingDisabled(true);
            config.setProperty(TorcGraph.CONFIG_GRAPH_NAME, properties.get("graphName"));
            config.setProperty(TorcGraph.CONFIG_COORD_LOCATOR, properties.get("coordinatorLocator"));
            config.setProperty(TorcGraph.CONFIG_NUM_MASTER_SERVERS, properties.get("numMasterServers"));

            client = TorcGraph.open(config);
        }

        public Graph client() {
            return client;
        }

        @Override
        public void close() throws IOException {
            try {
                client.close();
            } catch (Exception ex) {
                java.util.logging.Logger.getLogger(TorcDb.class.getName()).log(Level.SEVERE, null, ex);
            }
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
            Graph client = dbConnectionState.client();

            Vertex person = client.vertices(new UInt128(Entity.PERSON.getNumber(), person_id)).next();
            Iterator<VertexProperty<String>> props = person.properties();
            Map<String, String> propertyMap = new HashMap<>();
            props.forEachRemaining((prop) -> {
                propertyMap.put(prop.key(), prop.value());
            });

            LdbcShortQuery1PersonProfileResult res = new LdbcShortQuery1PersonProfileResult(
                    propertyMap.get("firstName"), propertyMap.get("lastName"),
                    Long.parseLong(propertyMap.get("birthday")), propertyMap.get("locationIP"),
                    propertyMap.get("browserUsed"), Long.decode(propertyMap.get("place")), propertyMap.get("gender"),
                    Long.parseLong(propertyMap.get("creationDate")));
            
            client.tx().commit();
            
            resultReporter.report(0, res, operation);
        }

    }
    
    public static class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery2PersonPostsHandler.class);
        
        @Override
        public void executeOperation(final LdbcShortQuery2PersonPosts operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            Graph client = dbConnectionState.client();
            
            List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>();
            
            Vertex person = client.vertices(new UInt128(Entity.PERSON.getNumber(), operation.personId())).next();
            Iterator<Edge> edges = person.edges(Direction.IN, "hasCreator");
            
            List<Vertex> messageList = new ArrayList<>();
            edges.forEachRemaining((e) -> messageList.add(e.outVertex()));
            messageList.sort((a, b) -> {
                Vertex v1 = (Vertex)a;
                Vertex v2 = (Vertex)b;
                
                long v1Date = Long.decode(v1.<String>property("creationDate").value());
                long v2Date = Long.decode(v2.<String>property("creationDate").value());
                
                if (v1Date > v2Date) {
                    return -1;
                } else if (v1Date < v2Date) {
                    return 1;
                } else {
                    long v1Id = ((UInt128)v1.id()).getLowerLong();
                    long v2Id = ((UInt128)v2.id()).getLowerLong();
                    if (v1Id > v2Id) {
                        return -1;
                    } else if (v1Id < v2Id) {
                        return 1;
                    } else {
                        return 0;
                    }
                }    
            });
            
            for (int i = 0; i < Integer.min(operation.limit(), messageList.size()); i++) {
                Vertex message = messageList.get(i);
                
                Map<String, String> propMap = new HashMap<>();
                message.<String>properties().forEachRemaining((vp) -> {
                    propMap.put(vp.key(), vp.value());
                });
                
                long messageId = ((UInt128)message.id()).getLowerLong();
                
                String messageContent;
                if (propMap.get("content").length() != 0)
                    messageContent = propMap.get("content");
                else
                    messageContent = propMap.get("imageFile");
                
                long messageCreationDate = Long.decode(propMap.get("creationDate"));
                
                long originalPostId;
                long originalPostAuthorId;
                String originalPostAuthorFirstName;
                String originalPostAuthorLastName;
                if (message.label().equals(Entity.POST.getName())) {
                    originalPostId = messageId;
                    originalPostAuthorId = ((UInt128) person.id()).getLowerLong();
                    originalPostAuthorFirstName = person.<String>property("firstName").value();
                    originalPostAuthorLastName = person.<String>property("lastName").value();
                } else {
                    Vertex parentMessage = message.edges(Direction.OUT, "replyOf").next().inVertex();
                    while(true) {
                        if (parentMessage.label().equals(Entity.POST.getName())) {
                            originalPostId = ((UInt128) parentMessage.id()).getLowerLong();
                            
                            Vertex author = parentMessage.edges(Direction.OUT, "hasCreator").next().inVertex();
                            originalPostAuthorId = ((UInt128) author.id()).getLowerLong();
                            originalPostAuthorFirstName = author.<String>property("firstName").value();
                            originalPostAuthorLastName = author.<String>property("lastName").value();
                            break;
                        } else {
                            parentMessage = parentMessage.edges(Direction.OUT, "replyOf").next().inVertex();
                        }
                    }
                }
                
                LdbcShortQuery2PersonPostsResult res = new LdbcShortQuery2PersonPostsResult(
                        messageId, 
                        messageContent, 
                        messageCreationDate,
                        originalPostId,
                        originalPostAuthorId,
                        originalPostAuthorFirstName, 
                        originalPostAuthorLastName);
                
                result.add(res);
            }
            
            client.tx().commit();
            
            resultReporter.report(result.size(), result, operation);
        }
    }
    
    public static class LdbcShortQuery3PersonFriendsHandler implements OperationHandler<LdbcShortQuery3PersonFriends, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery3PersonFriendsHandler.class);
        
        @Override
        public void executeOperation(final LdbcShortQuery3PersonFriends operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            Graph client = dbConnectionState.client();
            
            List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();
            
            Vertex person = client.vertices(new UInt128(Entity.PERSON.getNumber(), operation.personId())).next();
            
            Iterator<Edge> edges = person.edges(Direction.OUT, "knows");
            
            edges.forEachRemaining((e) -> {
                long creationDate = Long.decode(e.<String>property("creationDate").value());
                
                Vertex friend = e.inVertex();
                
                long personId = ((UInt128)friend.id()).getLowerLong();
                
                String firstName = friend.<String>property("firstName").value();
                String lastName = friend.<String>property("lastName").value();
                
                LdbcShortQuery3PersonFriendsResult res = new LdbcShortQuery3PersonFriendsResult(
                        personId,
                        firstName, 
                        lastName,
                        creationDate);
                result.add(res);
            });
            
            // Sort the result here.
            result.sort((a, b) -> {
                LdbcShortQuery3PersonFriendsResult r1 = (LdbcShortQuery3PersonFriendsResult) a;
                LdbcShortQuery3PersonFriendsResult r2 = (LdbcShortQuery3PersonFriendsResult) b;
                
                if (r1.friendshipCreationDate() > r2.friendshipCreationDate()) {
                    return -1;
                } else if (r1.friendshipCreationDate() < r2.friendshipCreationDate()) {
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
            
            client.tx().commit();
            
            resultReporter.report(result.size(), result, operation);
        }

    }
    
    public static class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery4MessageContentHandler.class);

        @Override
        public void executeOperation(final LdbcShortQuery4MessageContent operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            Graph client = dbConnectionState.client();

            Vertex message = client.vertices(new UInt128(Entity.MESSAGE.getNumber(), operation.messageId())).next();

            long creationDate = Long.decode(message.<String>property("creationDate").value());
            String content = message.<String>property("content").value();
            if (content.length() == 0) {
                content = message.<String>property("imageFile").value();
            }

            LdbcShortQuery4MessageContentResult result = new LdbcShortQuery4MessageContentResult(
                    content,
                    creationDate);

            client.tx().commit();
            
            resultReporter.report(1, result, operation);
        }

    }
    
    public static class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery5MessageCreatorHandler.class);
        
        @Override
        public void executeOperation(final LdbcShortQuery5MessageCreator operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            Graph client = dbConnectionState.client();
            
            Vertex message = client.vertices(new UInt128(Entity.MESSAGE.getNumber(), operation.messageId())).next();
            
            Vertex creator = message.edges(Direction.OUT, "hasCreator").next().inVertex();
            
            long creatorId = ((UInt128)creator.id()).getLowerLong();
            String creatorFirstName = creator.<String>property("firstName").value();
            String creatorLastName = creator.<String>property("lastName").value();
            
            LdbcShortQuery5MessageCreatorResult result = new LdbcShortQuery5MessageCreatorResult(
                        creatorId,
                        creatorFirstName,
                        creatorLastName);
            
            client.tx().commit();
            
            resultReporter.report(1, result, operation);
        }

    }
    
    public static class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery6MessageForumHandler.class);
        
        @Override
        public void executeOperation(final LdbcShortQuery6MessageForum operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            Graph client = dbConnectionState.client();
            
            Vertex message = client.vertices(new UInt128(Entity.MESSAGE.getNumber(), operation.messageId())).next();
            
            Vertex parent = message.edges(Direction.OUT, "replyOf").next().inVertex();
            while (true) {
                if (parent.label().equals(Entity.FORUM.getName())) {
                    long forumId = ((UInt128) parent.id()).getLowerLong();
                    String forumTitle = parent.<String>property("title").value();

                    Vertex moderator = parent.edges(Direction.OUT, "hasModerator").next().inVertex();

                    long moderatorId = ((UInt128) moderator.id()).getLowerLong();
                    String moderatorFirstName = moderator.<String>property("firstName").value();
                    String moderatorLastName = moderator.<String>property("lastName").value();

                    LdbcShortQuery6MessageForumResult result = new LdbcShortQuery6MessageForumResult(
                            forumId,
                            forumTitle,
                            moderatorId,
                            moderatorFirstName,
                            moderatorLastName);

                    client.tx().commit();
                    
                    resultReporter.report(1, result, operation);

                    return;
                } else {
                    parent = parent.edges(Direction.OUT, "replyOf").next().inVertex();
                }
            }
        }

    }
    
    public static class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcShortQuery7MessageRepliesHandler.class);
        
        @Override
        public void executeOperation(final LdbcShortQuery7MessageReplies operation, BasicDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            Graph client = dbConnectionState.client();
            
            Vertex message = client.vertices(new UInt128(Entity.MESSAGE.getNumber(), operation.messageId())).next();
            Vertex messageAuthor = message.edges(Direction.OUT, "hasCreator").next().inVertex();
            long messageAuthorId = ((UInt128)messageAuthor.id()).getLowerLong();
            
            List<Vertex> replies = new ArrayList<>();
            message.edges(Direction.IN, "replyOf").forEachRemaining((e) -> {
                replies.add(e.outVertex());
            });
            
            List<Long> messageAuthorFriendIds = new ArrayList<>();
            messageAuthor.edges(Direction.OUT, "knows").forEachRemaining((e) -> {
                messageAuthorFriendIds.add(((UInt128)e.inVertex().id()).getLowerLong());
            });
            
            List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();
            
            for (Vertex reply : replies) {
                long replyId = ((UInt128)reply.id()).getLowerLong();
                String replyContent = reply.<String>property("content").value();
                long replyCreationDate = Long.decode(reply.<String>property("creationDate").value());
                
                Vertex replyAuthor = reply.edges(Direction.OUT, "hasCreator").next().inVertex();
                long replyAuthorId = ((UInt128)replyAuthor.id()).getLowerLong();
                String replyAuthorFirstName = replyAuthor.<String>property("firstName").value();
                String replyAuthorLastName = replyAuthor.<String>property("lastName").value();
                
                boolean knows = false;
                if (messageAuthorId != replyAuthorId) {
                    knows = messageAuthorFriendIds.contains(replyAuthorId);
                }
                
                LdbcShortQuery7MessageRepliesResult res = new LdbcShortQuery7MessageRepliesResult(
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
            
            client.tx().commit();
            
            resultReporter.report(result.size(), result, operation);
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
            
            Graph client = dbConnectionState.client();
            
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
            Graph client = dbConnectionState.client();

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
            Graph client = dbConnectionState.client();

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
            Graph client = dbConnectionState.client();
            
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
            Graph client = dbConnectionState.client();

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
            Graph client = dbConnectionState.client();

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
            Graph client = dbConnectionState.client();

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
            Graph client = dbConnectionState.client();

            List<Object> knowsEdgeKeyValues = new ArrayList<>(2);
            knowsEdgeKeyValues.add("creationDate");
            knowsEdgeKeyValues.add(String.valueOf(operation.creationDate().getTime()));
            
            List<UInt128> ids = new ArrayList<>(2);
            ids.add(new UInt128(Entity.PERSON.getNumber(), operation.person1Id()));
            ids.add(new UInt128(Entity.PERSON.getNumber(), operation.person2Id()));
            
            Iterator<Vertex> vItr = client.vertices(ids.toArray());
            
            Vertex person1 = vItr.next();
            Vertex person2 = vItr.next();
            
            person1.addEdge("knows", person2, knowsEdgeKeyValues.toArray());
            person2.addEdge("knows", person1, knowsEdgeKeyValues.toArray());
            
            client.tx().commit();
            
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
}
