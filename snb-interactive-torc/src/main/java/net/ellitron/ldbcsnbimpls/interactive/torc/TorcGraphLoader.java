/* 
 * Copyright (C) 2015-2016 Stanford University
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
package net.ellitron.ldbcsnbimpls.interactive.torc;

import net.ellitron.ldbcsnbimpls.snb.Entity;

import net.ellitron.torc.TorcGraph;
import net.ellitron.torc.TorcVertex;
import net.ellitron.torc.util.UInt128;
import net.ellitron.torc.util.TorcHelper;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TorcGraphLoader {

    private static final Logger logger = Logger.getLogger(TorcGraphLoader.class.getName());

    private static final long TX_MAX_RETRIES = 1000;
    
    public static void loadVertices(TorcGraph graph, Path filePath, boolean printLoadingDots, int batchSize) throws IOException, java.text.ParseException {
        String[] colNames = null;
        boolean firstLine = true;
        Map<Object, Object> propertiesMap;
        SimpleDateFormat birthdayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        SimpleDateFormat creationDateDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String fileNameParts[] = filePath.getFileName().toString().split("_");
        String entityName = fileNameParts[0];
        
        List<String> lines = Files.readAllLines(filePath);
        colNames = lines.get(0).split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;
        for (int startIndex = 1; startIndex < lines.size(); startIndex += batchSize) {
            int endIndex = Math.min(startIndex + batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            do {
                for (int i = startIndex; i < endIndex; i++) {
                    String line = lines.get(i);

                    String[] colVals = line.split("\\|");
                    propertiesMap = new HashMap<>();

                    for (int j = 0; j < colVals.length; ++j) {
                        if (colNames[j].equals("id")) {
                            propertiesMap.put(T.id, new UInt128(Entity.fromName(entityName).getNumber(), Long.decode(colVals[j])));
                        } else if (colNames[j].equals("birthday")) {
                            propertiesMap.put(colNames[j], String.valueOf(birthdayDateFormat.parse(colVals[j]).getTime()));
                        } else if (colNames[j].equals("creationDate")) {
                            propertiesMap.put(colNames[j], String.valueOf(creationDateDateFormat.parse(colVals[j]).getTime()));
                        } else {
                            propertiesMap.put(colNames[j], colVals[j]);
                        }
                    }

                    propertiesMap.put(T.label, entityName);

                    List<Object> keyValues = new ArrayList<>();
                    propertiesMap.forEach((key, val) -> {
                        keyValues.add(key);
                        keyValues.add(val);
                    });

                    graph.addVertex(keyValues.toArray());

                    lineCount++;
                }

                try {
                    graph.tx().commit();
                    txSucceeded = true;
                } catch (Exception e) {
                    txFailCount++;
                }
                
                if (txFailCount > TX_MAX_RETRIES) {
                    throw new RuntimeException(String.format("ERROR: Transaction failed %d times (file lines [%d,%d]), aborting...", txFailCount, startIndex, endIndex-1));
                }
            } while (!txSucceeded);
            
            if (printLoadingDots && (lineCount % 100000 == 0)) {
                System.out.print(". ");
            }
        }
    }
    
    public static void loadProperties(TorcGraph graph, Path filePath, boolean printLoadingDots, int batchSize) throws IOException {
        long count = 0;
        String[] colNames = null;
        boolean firstLine = true;
        String fileNameParts[] = filePath.getFileName().toString().split("_");
        String entityName = fileNameParts[0];
        
        List<String> lines = Files.readAllLines(filePath);
        colNames = lines.get(0).split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;
        for (int startIndex = 1; startIndex < lines.size(); startIndex += batchSize) {
            int endIndex = Math.min(startIndex + batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            do {
                for (int i = startIndex; i < endIndex; i++) {
                    String line = lines.get(i);

                    String[] colVals = line.split("\\|");
                    TorcVertex vertex = (TorcVertex) graph.vertices(new UInt128(Entity.fromName(entityName).getNumber(), Long.decode(colVals[0]))).next();

                    for (int j = 1; j < colVals.length; ++j) {
                        vertex.property(VertexProperty.Cardinality.list, colNames[j], colVals[j]);
                    }

                    lineCount++;
                }

                try {
                    graph.tx().commit();
                    txSucceeded = true;
                } catch (Exception e) {
                    txFailCount++;
                }
                
                if (txFailCount > TX_MAX_RETRIES) {
                    throw new RuntimeException(String.format("ERROR: Transaction failed %d times (file lines [%d,%d]), aborting...", txFailCount, startIndex, endIndex-1));
                }
            } while (!txSucceeded);
            
            if (printLoadingDots && (lineCount % 100000 == 0)) {
                System.out.print(". ");
            }
        }
    }
    
    public static void loadEdges(TorcGraph graph, Path filePath, boolean undirected, boolean printLoadingDots, int batchSize) throws IOException, java.text.ParseException {
        long count = 0;
        String[] colNames = null;
        boolean firstLine = true;
        Map<Object, Object> propertiesMap;
        SimpleDateFormat creationDateDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        SimpleDateFormat joinDateDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        joinDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String fileNameParts[] = filePath.getFileName().toString().split("_");
        String v1EntityName = fileNameParts[0];
        String edgeLabel = fileNameParts[1];
        String v2EntityName = fileNameParts[2];
        
        List<String> lines = Files.readAllLines(filePath);
        colNames = lines.get(0).split("\\|");
        long lineCount = 0;
        boolean txSucceeded;
        long txFailCount;
        for (int startIndex = 1; startIndex < lines.size(); startIndex += batchSize) {
            int endIndex = Math.min(startIndex + batchSize, lines.size());
            txSucceeded = false;
            txFailCount = 0;
            do {
                for (int i = startIndex; i < endIndex; i++) {
                    String line = lines.get(i);

                    String[] colVals = line.split("\\|");

                    Long vertex1Id = Long.decode(colVals[0]);
                    Long vertex2Id = Long.decode(colVals[1]);

                    TorcVertex vertex1 = (TorcVertex) graph.vertices(new UInt128(Entity.fromName(v1EntityName).getNumber(), vertex1Id)).next();
                    TorcVertex vertex2 = (TorcVertex) graph.vertices(new UInt128(Entity.fromName(v2EntityName).getNumber(), vertex2Id)).next();

                    propertiesMap = new HashMap<>();
                    for (int j = 2; j < colVals.length; ++j) {
                        if (colNames[j].equals("creationDate")) {
                            propertiesMap.put(colNames[j], String.valueOf(creationDateDateFormat.parse(colVals[j]).getTime()));
                        } else if (colNames[j].equals("joinDate")) {
                            propertiesMap.put(colNames[j], String.valueOf(joinDateDateFormat.parse(colVals[j]).getTime()));
                        } else {
                            propertiesMap.put(colNames[j], colVals[j]);
                        }
                    }

                    List<Object> keyValues = new ArrayList<>();
                    propertiesMap.forEach((key, val) -> {
                        keyValues.add(key);
                        keyValues.add(val);
                    });

                    vertex1.addEdge(edgeLabel, vertex2, keyValues.toArray());

                    if (undirected) {
                        vertex2.addEdge(edgeLabel, vertex1, keyValues.toArray());
                    }

                    lineCount++;
                }

                try {
                    graph.tx().commit();
                    txSucceeded = true;
                } catch (Exception e) {
                    txFailCount++;
                }
                
                if (txFailCount > TX_MAX_RETRIES) {
                    throw new RuntimeException(String.format("ERROR: Transaction failed %d times (file lines [%d,%d]), aborting...", txFailCount, startIndex, endIndex-1));
                }
            } while (!txSucceeded);
            
            if (printLoadingDots && (lineCount % 100000 == 0)) {
                System.out.print(". ");
            }
        }
    }
    
    public static void main(String[] args) throws IOException {
        Options options = new Options();
        options.addOption("C", "coordinator", true, "Service locator where the coordinator can be contacted.");
        options.addOption(null, "batchSize", true, "Number of nodes/edges to load in a single transaction.");
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

        int batchSize;
        if (cmd.hasOption("batchSize")) {
            batchSize = Integer.decode(cmd.getOptionValue("batchSize"));
        } else {
            logger.log(Level.SEVERE, "Missing required argument: batchSize");
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
        String nodeFiles[] = {  "person_0_0.csv",
                                "comment_0_0.csv",
                                "forum_0_0.csv",
                                "organisation_0_0.csv",
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
                try {
                    loadVertices(graph, Paths.get(inputBaseDir + "/" + fileName), true, batchSize);
                    System.out.println("Finished");
                } catch (NoSuchFileException e) {
                    System.out.println(" File not found.");
                }
            }
            
            for (String fileName : propertiesFiles) {
                System.out.print("Loading properties file " + fileName + " ");
                try {
                    loadProperties(graph, Paths.get(inputBaseDir + "/" + fileName), true, batchSize);
                    System.out.println("Finished");
                } catch (NoSuchFileException e) {
                    System.out.println(" File not found.");
                }
            }
            
            for (String fileName : edgeFiles) {
                System.out.print("Loading edge file " + fileName + " ");
                try {
                    if (fileName.contains("person_knows_person")) {
                        loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), true, true, batchSize);
                    } else {
                        loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), false, true, batchSize);
                    }

                    System.out.println("Finished");
                } catch (NoSuchFileException e) {
                    System.out.println(" File not found.");
                }
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
