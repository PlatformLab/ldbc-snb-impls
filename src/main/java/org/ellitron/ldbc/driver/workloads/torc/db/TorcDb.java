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
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
            config.setProperty(TorcGraph.CONFIG_COORD_LOC, properties.get("coordinatorLocator"));
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

            TorcVertex root = (TorcVertex) client.vertices(TorcHelper.makeVertexId(Entity.PERSON.getNumber(), person_id)).next();
            Iterator<VertexProperty<String>> props = root.properties();
            Map<String, String> propertyMap = new HashMap<>();
            props.forEachRemaining((prop) -> {
                propertyMap.put(prop.key(), prop.value());
            });

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            Calendar calendar = new GregorianCalendar();

            try {
                calendar.setTime(dateFormat.parse(propertyMap.get("birthday")));
            } catch (ParseException ex) {
                java.util.logging.Logger.getLogger(TorcDb.class.getName()).log(Level.SEVERE, null, ex);
            }

            long birthday = calendar.getTimeInMillis();

            dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            try {
                calendar.setTime(dateFormat.parse(propertyMap.get("creationDate")));
            } catch (ParseException ex) {
                java.util.logging.Logger.getLogger(TorcDb.class.getName()).log(Level.SEVERE, null, ex);
            }

            long creationDate = calendar.getTimeInMillis();

            LdbcShortQuery1PersonProfileResult res = new LdbcShortQuery1PersonProfileResult(
                    propertyMap.get("firstName"), propertyMap.get("lastName"),
                    birthday, propertyMap.get("locationIP"),
                    propertyMap.get("browserUsed"), Long.decode(propertyMap.get("place")), propertyMap.get("gender"),
                    creationDate);
            resultReporter.report(0, res, operation);
        }

    }

    public static class LdbcUpdate1AddPersonHandler implements OperationHandler<LdbcUpdate1AddPerson, BasicDbConnectionState> {

        final static Logger logger = LoggerFactory.getLogger(LdbcUpdate1AddPersonHandler.class);

        @Override
        public void executeOperation(LdbcUpdate1AddPerson operation, BasicDbConnectionState dbConnectionState, ResultReporter reporter) throws DbException {
            TorcGraph client = dbConnectionState.client();

            SimpleDateFormat birthdayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            SimpleDateFormat creationDateDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            
            Map<Object, Object> props = new HashMap<>();
            props.put(T.id, TorcHelper.makeVertexId(Entity.PERSON.getNumber(), operation.personId()));
            props.put(T.label, "person");
            props.put("firstName", operation.personFirstName());
            props.put("lastName", operation.personLastName());
            props.put("gender", operation.gender());
            props.put("birthday", birthdayDateFormat.format(operation.birthday()));
            props.put("creationDate", creationDateDateFormat.format(operation.creationDate()));
            props.put("locationIP", operation.locationIp());
            props.put("browserUsed", operation.browserUsed());
            props.put("place", Long.toString(operation.cityId()));
            
            List<Object> keyValues = new ArrayList<>();
            props.forEach((key, val) -> {
                keyValues.add(key);
                keyValues.add(val);
            });

            client.addVertex(keyValues.toArray());
            client.tx().commit();
            
            reporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
}
