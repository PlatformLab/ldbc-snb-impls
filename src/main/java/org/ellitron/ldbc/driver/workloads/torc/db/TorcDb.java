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
            
//            timers[0][0] = System.nanoTime();
//            calendar.setTime(operation.birthday());
            String birthday = String.valueOf(operation.birthday().getTime());
//            calendar.setTime(operation.creationDate());
            String creationDate = String.valueOf(operation.creationDate().getTime());
//            timers[0][1] = System.nanoTime();
            
//            timers[1][0] = System.nanoTime();
            Map<Object, Object> props = new HashMap<>();
            props.put(T.id, new UInt128(Entity.PERSON.getNumber(), operation.personId()));
            props.put(T.label, "person");
            props.put("firstName", operation.personFirstName());
            props.put("lastName", operation.personLastName());
            props.put("gender", operation.gender());
            props.put("birthday", birthday);
            props.put("creationDate", creationDate);
            props.put("locationIP", operation.locationIp());
            props.put("browserUsed", operation.browserUsed());
            props.put("place", Long.toString(operation.cityId()));
//            timers[1][1] = System.nanoTime();
            
//            timers[2][0] = System.nanoTime();
            List<Object> keyValues = new ArrayList<>();
            props.forEach((key, val) -> {
                keyValues.add(key);
                keyValues.add(val);
            });
//            timers[2][1] = System.nanoTime();
            
            Object[] keyValArray = keyValues.toArray();
            
//            timers[3][0] = System.nanoTime();
            client.addVertex(keyValArray);
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

            
        }
    }
    
    public static class LdbcUpdate3AddCommentLikeHandler implements OperationHandler<LdbcUpdate3AddCommentLike, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate3AddCommentLikeHandler.class);

        @Override
        public void executeOperation(LdbcUpdate3AddCommentLike operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            
        }
    }
    
    public static class LdbcUpdate4AddForumHandler implements OperationHandler<LdbcUpdate4AddForum, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate4AddForum operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            
        }
    }
    
    public static class LdbcUpdate5AddForumMembershipHandler implements OperationHandler<LdbcUpdate5AddForumMembership, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate5AddForumMembership operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            
        }
    }
    
    public static class LdbcUpdate6AddPostHandler implements OperationHandler<LdbcUpdate6AddPost, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate6AddPost operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            
        }
    }
    
    public static class LdbcUpdate7AddCommentHandler implements OperationHandler<LdbcUpdate7AddComment, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate7AddComment operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            
        }
    }
    
    public static class LdbcUpdate8AddFriendshipHandler implements OperationHandler<LdbcUpdate8AddFriendship, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

        @Override
        public void executeOperation(LdbcUpdate8AddFriendship operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            
        }
    }
}
