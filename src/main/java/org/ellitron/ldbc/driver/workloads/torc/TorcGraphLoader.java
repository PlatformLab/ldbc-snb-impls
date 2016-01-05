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
package org.ellitron.ldbc.driver.workloads.torc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcGraph;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcVertex;
import org.ellitron.tinkerpop.gremlin.torc.structure.util.UInt128;
import org.ellitron.tinkerpop.gremlin.torc.structure.util.TorcHelper;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TorcGraphLoader {

    private static final Logger logger = Logger.getLogger(TorcGraphLoader.class.getName());

    public static void loadVertices(TorcGraph graph, Path filePath, boolean printLoadingDots) throws IOException {
        long count = 0;
        String[] colNames = null;
        boolean firstLine = true;
        Map<Object, Object> propertiesMap;
        String fileNameParts[] = filePath.getFileName().toString().split("_");
        String entityName = fileNameParts[0];
        for (String line : Files.readAllLines(filePath)) {
            if (firstLine) {
                colNames = line.split("\\|");
                firstLine = false;
                continue;
            }

            String[] colVals = line.split("\\|");
            propertiesMap = new HashMap<>();

            for (int i = 0; i < colVals.length; ++i) {
                if (colNames[i].equals("id")) {
                    propertiesMap.put(T.id, new UInt128(Entity.fromName(entityName).getNumber(), Long.decode(colVals[i])));
                } else {
                    propertiesMap.put(colNames[i], colVals[i]);
                }
            }

            propertiesMap.put(T.label, entityName);

            List<Object> keyValues = new ArrayList<>();
            propertiesMap.forEach((key, val) -> {
                keyValues.add(key);
                keyValues.add(val);
            });

            graph.addVertex(keyValues.toArray());

            count++;
            if (count % 100 == 0) {
                graph.tx().commit();
                if (printLoadingDots && (count % 100000 == 0)) {
                    System.out.print(". ");
                }
            }
        }
        graph.tx().commit();
    }
    
    public static void loadProperties(TorcGraph graph, Path filePath, boolean printLoadingDots) throws IOException {
        long count = 0;
        String[] colNames = null;
        boolean firstLine = true;
        String fileNameParts[] = filePath.getFileName().toString().split("_");
        String entityName = fileNameParts[0];
        for (String line : Files.readAllLines(filePath)) {
            if (firstLine) {
                colNames = line.split("\\|");
                firstLine = false;
                continue;
            }

            String[] colVals = line.split("\\|");
            TorcVertex vertex = (TorcVertex) graph.vertices(new UInt128(Entity.fromName(entityName).getNumber(), Long.decode(colVals[0]))).next();
            
            for (int i = 1; i < colVals.length; ++i) {
                vertex.property(VertexProperty.Cardinality.list, colNames[i], colVals[i]);
            }

            count++;
            if (count % 100 == 0) {
                graph.tx().commit();
                if (printLoadingDots && (count % 100000 == 0)) {
                    System.out.print(". ");
                }
            }
        }
        graph.tx().commit();
    }
    
    public static void loadEdges(TorcGraph graph, Path filePath, boolean undirected, boolean printLoadingDots) throws IOException {
        long count = 0;
        String[] colNames = null;
        boolean firstLine = true;
        Map<Object, Object> propertiesMap;
        String fileNameParts[] = filePath.getFileName().toString().split("_");
        String v1EntityName = fileNameParts[0];
        String edgeLabel = fileNameParts[1];
        String v2EntityName = fileNameParts[2];
        for (String line : Files.readAllLines(filePath)) {
            if (firstLine) {
                colNames = line.split("\\|");
                firstLine = false;
                continue;
            }
            
            String[] colVals = line.split("\\|");

            Long vertex1Id = Long.decode(colVals[0]);
            Long vertex2Id = Long.decode(colVals[1]);
            
            TorcVertex vertex1 = (TorcVertex) graph.vertices(new UInt128(Entity.fromName(v1EntityName).getNumber(), vertex1Id)).next();
            TorcVertex vertex2 = (TorcVertex) graph.vertices(new UInt128(Entity.fromName(v2EntityName).getNumber(), vertex2Id)).next();
            
            propertiesMap = new HashMap<>();
            for (int i = 2; i < colVals.length; ++i) {
                propertiesMap.put(colNames[i], colVals[i]);
            }

            List<Object> keyValues = new ArrayList<>();
            propertiesMap.forEach((key, val) -> {
                keyValues.add(key);
                keyValues.add(val);
            });

            if (undirected)
                vertex1.addUndirectedEdge(edgeLabel, vertex2, keyValues.toArray());
            else
                vertex1.addEdge(edgeLabel, vertex2, keyValues.toArray());
            
            count++;
            if (count % 100 == 0) {
                try {
                    graph.tx().commit();
                } catch (RuntimeException e) {
                    System.out.println("count="+count);
                    throw e;
                }
                if (printLoadingDots && (count % 100000 == 0)) {
                    System.out.print(". ");
                }
            }
        }
        graph.tx().commit();
    }
    
    public static void main(String[] args) throws IOException {
        Options options = new Options();
        options.addOption("C", "coordinator", true, "Service locator where the coordinator can be contacted.");
        options.addOption(null, "numMasters", true, "Total master servers.");
        options.addOption(null, "graphName", true, "Name for this graph.");
        options.addOption(null, "input", true, "Input file directory.");
        options.addOption("h", "help", false, "Print usage.");

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException ex) {
            logger.log(Level.SEVERE, null, ex);
            return;
        }

        if (cmd.hasOption("h")) {
            formatter.printHelp("TorcGraphLoader", options);
            return;
        }

        // Required parameters.
        String coordinatorLocator;
        if (cmd.hasOption("coordinator")) {
            coordinatorLocator = cmd.getOptionValue("coordinator");
        } else {
            logger.log(Level.SEVERE, "Missing required argument: coordinator");
            return;
        }

        int numMasters;
        if (cmd.hasOption("numMasters")) {
            numMasters = Integer.decode(cmd.getOptionValue("numMasters"));
        } else {
            logger.log(Level.SEVERE, "Missing required argument: numMasters");
            return;
        }

        String graphName;
        if (cmd.hasOption("graphName")) {
            graphName = cmd.getOptionValue("graphName");
        } else {
            logger.log(Level.SEVERE, "Missing required argument: graphName");
            return;
        }
        
        String inputBaseDir;
        if (cmd.hasOption("input")) {
            inputBaseDir = cmd.getOptionValue("input");
        } else {
            logger.log(Level.SEVERE, "Missing required argument: input");
            return;
        }

        /**
         * TODO: Use environment variables for the source of configuration
         * information.
         */
        BaseConfiguration config = new BaseConfiguration();
        config.setDelimiterParsingDisabled(true);
        config.setProperty(TorcGraph.CONFIG_GRAPH_NAME, graphName);
        config.setProperty(TorcGraph.CONFIG_COORD_LOCATOR, coordinatorLocator);
        config.setProperty(TorcGraph.CONFIG_NUM_MASTER_SERVERS, numMasters);

        TorcGraph graph = TorcGraph.open(config);
        
        // TODO: Make file list generation programmatic. This method of loading,
        // however, will be far too slow for anything other than the very 
        // smallest of SNB graphs, and is therefore quite transient. This will
        // do for now.
        String nodeFiles[] = {  "comment_0_0.csv",
                                "forum_0_0.csv",
                                "organisation_0_0.csv",
                                "person_0_0.csv",
                                "place_0_0.csv",
                                "post_0_0.csv",
                                "tag_0_0.csv",
                                "tagclass_0_0.csv" 
        };
        
        String propertiesFiles[] = {    "person_email_emailaddress_0_0.csv",
                                        "person_speaks_language_0_0.csv"
        };
        
        String edgeFiles[] = {  "comment_hasCreator_person_0_0.csv",
                                "comment_hasTag_tag_0_0.csv",
                                "comment_isLocatedIn_place_0_0.csv",
                                "comment_replyOf_comment_0_0.csv",
                                "comment_replyOf_post_0_0.csv",
                                "forum_containerOf_post_0_0.csv",
                                "forum_hasMember_person_0_0.csv",
                                "forum_hasModerator_person_0_0.csv",
                                "forum_hasTag_tag_0_0.csv",
                                "organisation_isLocatedIn_place_0_0.csv",
                                "person_hasInterest_tag_0_0.csv",
                                "person_isLocatedIn_place_0_0.csv",
                                "person_knows_person_0_0.csv",
                                "person_likes_comment_0_0.csv",
                                "person_likes_post_0_0.csv",
                                "person_studyAt_organisation_0_0.csv",
                                "person_workAt_organisation_0_0.csv",
                                "place_isPartOf_place_0_0.csv",
                                "post_hasCreator_person_0_0.csv",
                                "post_hasTag_tag_0_0.csv",
                                "post_isLocatedIn_place_0_0.csv",
                                "tag_hasType_tagclass_0_0.csv",
                                "tagclass_isSubclassOf_tagclass_0_0.csv"
        };
        
        try {
            for (String fileName : nodeFiles) {
                System.out.print("Loading node file " + fileName + " ");
                loadVertices(graph, Paths.get(inputBaseDir + "/" + fileName), true);
                System.out.println("Finished");
            }
            
            for (String fileName : propertiesFiles) {
                System.out.print("Loading properties file " + fileName + " ");
                loadProperties(graph, Paths.get(inputBaseDir + "/" + fileName), true);
                System.out.println("Finished");
            }
            
            for (String fileName : edgeFiles) {
                System.out.print("Loading edge file " + fileName + " ");
                
                if (fileName.contains("person_knows_person"))
                    loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), true, true);
                else
                    loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), false, true);
                
                System.out.println("Finished");
            }
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        } finally {
            //graph.deleteDatabaseAndCloseAllConnectionsAndTransactions();
            graph.close();
        }
    }
}
