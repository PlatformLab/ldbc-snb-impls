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
package net.ellitron.ldbcsnbimpls.interactive.titan;

import net.ellitron.torc.util.TorcHelper;
import net.ellitron.torc.util.UInt128;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.BaseConfiguration;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Map;
import java.util.TimeZone;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TitanGraphLoader {

  private static final Logger logger =
    Logger.getLogger(TitanGraphLoader.class.getName());

  private static final long TX_MAX_RETRIES = 1000;

  public static void loadVertices(Graph graph, Path filePath, 
      boolean printLoadingDots, int batchSize, long progReportPeriod) 
      throws IOException, java.text.ParseException {

    String[] colNames = null;
    boolean firstLine = true;
    Map<Object, Object> propertiesMap;
    SimpleDateFormat birthdayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    birthdayDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    SimpleDateFormat creationDateDateFormat = 
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    String fileNameParts[] = filePath.getFileName().toString().split("_");
    String entityName = fileNameParts[0];

    List<String> lines = Files.readAllLines(filePath);
    colNames = lines.get(0).split("\\|");
    long lineCount = 0;
    boolean txSucceeded;
    long txFailCount;

    // For progress reporting
    long startTime = System.currentTimeMillis();
    long nextProgReportTime = startTime + progReportPeriod*1000;
    long lastLineCount = 0;

    for (int startIndex = 1; startIndex < lines.size(); 
        startIndex += batchSize) {
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
              propertiesMap.put("iid", entityName + ":" + colVals[j]);
            } else if (colNames[j].equals("birthday")) {
              propertiesMap.put(colNames[j], String.valueOf(
                    birthdayDateFormat.parse(colVals[j]).getTime()));
            } else if (colNames[j].equals("creationDate")) {
              propertiesMap.put(colNames[j], String.valueOf(
                    creationDateDateFormat.parse(colVals[j]).getTime()));
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
          throw new RuntimeException(String.format(
                "ERROR: Transaction failed %d times (file lines [%d,%d]), " +  
                "aborting...", txFailCount, startIndex, endIndex-1));
        }
      } while (!txSucceeded);

      if (printLoadingDots && 
          (System.currentTimeMillis() > nextProgReportTime)) {
        long timeElapsed = System.currentTimeMillis() - startTime;
        long linesLoaded = lineCount - lastLineCount;
        System.out.println(String.format(
              "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
              (timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
        nextProgReportTime += progReportPeriod*1000;
        lastLineCount = lineCount;
      }
    }
  }

  public static void loadProperties(Graph graph, Path filePath, 
      boolean printLoadingDots, int batchSize, long progReportPeriod) 
      throws IOException {
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

    // For progress reporting
    long startTime = System.currentTimeMillis();
    long nextProgReportTime = startTime + progReportPeriod*1000;
    long lastLineCount = 0;

    for (int startIndex = 1; startIndex < lines.size(); 
        startIndex += batchSize) {
      int endIndex = Math.min(startIndex + batchSize, lines.size());
      txSucceeded = false;
      txFailCount = 0;
      do {
        for (int i = startIndex; i < endIndex; i++) {
          String line = lines.get(i);

          String[] colVals = line.split("\\|");

          GraphTraversalSource g = graph.traversal();
          Vertex vertex = 
            g.V().has("iid", entityName + ":" + colVals[0]).next();

          for (int j = 1; j < colVals.length; ++j) {
            vertex.property(VertexProperty.Cardinality.list, colNames[j],
                colVals[j]);
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
          throw new RuntimeException(String.format(
                "ERROR: Transaction failed %d times (file lines [%d,%d]), " + 
                "aborting...", txFailCount, startIndex, endIndex-1));
        }
      } while (!txSucceeded);

      if (printLoadingDots && 
          (System.currentTimeMillis() > nextProgReportTime)) {
        long timeElapsed = System.currentTimeMillis() - startTime;
        long linesLoaded = lineCount - lastLineCount;
        System.out.println(String.format(
              "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
              (timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
        nextProgReportTime += progReportPeriod*1000;
        lastLineCount = lineCount;
      }
    }
  }

  public static void loadEdges(Graph graph, Path filePath, boolean undirected,
      boolean printLoadingDots, int batchSize, long progReportPeriod) 
      throws IOException,  java.text.ParseException {
    long count = 0;
    String[] colNames = null;
    boolean firstLine = true;
    Map<Object, Object> propertiesMap;
    SimpleDateFormat creationDateDateFormat = 
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    creationDateDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    SimpleDateFormat joinDateDateFormat = 
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
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

    // For progress reporting
    long startTime = System.currentTimeMillis();
    long nextProgReportTime = startTime + progReportPeriod*1000;
    long lastLineCount = 0;

    for (int startIndex = 1; startIndex < lines.size(); 
        startIndex += batchSize) {
      int endIndex = Math.min(startIndex + batchSize, lines.size());
      txSucceeded = false;
      txFailCount = 0;
      do {
        for (int i = startIndex; i < endIndex; i++) {
          String line = lines.get(i);

          String[] colVals = line.split("\\|");

          GraphTraversalSource g = graph.traversal();
          Vertex vertex1 = 
            g.V().has("iid", v1EntityName + ":" + colVals[0]).next();
          Vertex vertex2 = 
            g.V().has("iid", v2EntityName + ":" + colVals[1]).next();

          propertiesMap = new HashMap<>();
          for (int j = 2; j < colVals.length; ++j) {
            if (colNames[j].equals("creationDate")) {
              propertiesMap.put(colNames[j], String.valueOf(
                    creationDateDateFormat.parse(colVals[j]).getTime()));
            } else if (colNames[j].equals("joinDate")) {
              propertiesMap.put(colNames[j], String.valueOf(
                    joinDateDateFormat.parse(colVals[j]).getTime()));
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
          throw new RuntimeException(String.format(
                "ERROR: Transaction failed %d times (file lines [%d,%d]), " + 
                "aborting...", txFailCount, startIndex, endIndex-1));
        }
      } while (!txSucceeded);

      if (printLoadingDots && 
          (System.currentTimeMillis() > nextProgReportTime)) {
        long timeElapsed = System.currentTimeMillis() - startTime;
        long linesLoaded = lineCount - lastLineCount;
        System.out.println(String.format(
              "Time Elapsed: %03dm.%02ds, Lines Loaded: +%d", 
              (timeElapsed/1000)/60, (timeElapsed/1000) % 60, linesLoaded));
        nextProgReportTime += progReportPeriod*1000;
        lastLineCount = lineCount;
      }
    }
  }

  public static void main(String[] args) throws IOException {
    Options options = new Options();
    options.addOption("C", "cassandraLocator", true, 
        "IP address of a cassandra server.");
    options.addOption(null, "batchSize", true, 
        "Number of nodes/edges to load in a single transaction.");
    options.addOption(null, "graphName", true, 
        "Name of the graph instance.");
    options.addOption(null, "input", true, 
        "Input file directory.");
    options.addOption(null, "progReportPeriod", true, 
        "How often, in seconds, to report loading progress (default 10s).");
    options.addOption("h", "help", false, 
        "Print usage.");

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
      formatter.printHelp("TitanGraphLoader", options);
      return;
    }

    // Required parameters.
    String cassandraLocator;
    if (cmd.hasOption("cassandraLocator")) {
      cassandraLocator = cmd.getOptionValue("cassandraLocator");
    } else {
      logger.log(Level.SEVERE, "Missing required argument: cassandraLocator");
      return;
    }

    int batchSize;
    if (cmd.hasOption("batchSize")) {
      batchSize = Integer.decode(cmd.getOptionValue("batchSize"));
    } else {
      logger.log(Level.SEVERE, "Missing required argument: batchSize");
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

    long progReportPeriod = 10;
    if (cmd.hasOption("progReportPeriod")) {
      progReportPeriod = Long.decode(cmd.getOptionValue("progReportPeriod"));
    } 

    // Create the Titan graph client instance with several configuration
    // parameters
    TitanGraph graph = TitanFactory.build()
      .set("storage.backend", "cassandra")
      .set("storage.hostname", cassandraLocator)
      .set("storage.cassandra.keyspace", graphName)
      .set("storage.batch-loading", true)
      .set("ids.block-size", 1000000)
      //                .set("schema.default", "none")
      .open();

    String vertexLabels[] = {  
      "person",
      "comment",
      "forum",
      "organisation",
      "place",
      "post",
      "tag",
      "tagclass" 
    };

    String edgeLabels[] = {  
      "containerOf",
      "hasCreator",
      "hasInterest",
      "hasMember",
      "hasModerator",
      "hasTag",
      "hasType",
      "isLocatedIn",
      "isPartOf",
      "isSubclassOf",
      "knows",
      "likes",
      "replyOf",
      "studyAt",
      "workAt"
    };

    // All property keys with Cardinality.SINGLE
    String singleCardPropKeys[] = {
      "birthday", // person
      "browserUsed", // comment person post
      "classYear", // studyAt
      "content", // comment post
      "creationDate", // comment forum person post knows likes
      "firstName", // person
      "gender", // person
      "imageFile", // post
      "joinDate", // hasMember
      //"language", // post
      "lastName", // person
      "length", // comment post
      "locationIP", // comment person post
      "name", // organisation place tag tagclass
      "title", // forum
      "type", // organisation place
      "url", // organisation place tag tagclass
      "workFrom", // workAt
    };

    // All property keys with Cardinality.LIST
    String listCardPropKeys[] = {
      "email", // person
      "language" // person, post
    };

    /*
     * Explicitly define the graph schema.
     *
     * Note: For unknown reasons, it seems that each modification to the
     * schema must be committed in its own transaction.
     */
    try {
      ManagementSystem mgmt;

      // Declare all vertex labels.
      for( String vLabel : vertexLabels ) {
        System.out.println(vLabel);
        mgmt = (ManagementSystem) graph.openManagement();
        mgmt.makeVertexLabel(vLabel).make();
        mgmt.commit();
      }

      // Declare all edge labels.
      for( String eLabel : edgeLabels ) {
        System.out.println(eLabel);
        mgmt = (ManagementSystem) graph.openManagement();
        mgmt.makeEdgeLabel(eLabel).multiplicity(Multiplicity.SIMPLE).make();
        mgmt.commit();
      }

      // Delcare all properties with Cardinality.SINGLE
      for ( String propKey : singleCardPropKeys ) {
        System.out.println(propKey);
        mgmt = (ManagementSystem) graph.openManagement();
        mgmt.makePropertyKey(propKey).dataType(String.class)
          .cardinality(Cardinality.SINGLE).make();     
        mgmt.commit();
      }

      // Delcare all properties with Cardinality.LIST
      for ( String propKey : listCardPropKeys ) {
        System.out.println(propKey);
        mgmt = (ManagementSystem) graph.openManagement();
        mgmt.makePropertyKey(propKey).dataType(String.class)
          .cardinality(Cardinality.LIST).make();     
        mgmt.commit();
      }

      /* 
       * Create a special ID property where we will store the IDs of
       * vertices in the SNB dataset, and a corresponding index. This is
       * necessary because TitanDB generates its own IDs for graph
       * vertices, but the benchmark references vertices by the ID they
       * were originally assigned during dataset generation.
       */
      mgmt = (ManagementSystem) graph.openManagement();
      mgmt.makePropertyKey("iid").dataType(String.class)
        .cardinality(Cardinality.SINGLE).make();     
      mgmt.commit();

      mgmt = (ManagementSystem) graph.openManagement();
      PropertyKey iid = mgmt.getPropertyKey("iid");
      mgmt.buildIndex("byIid", Vertex.class).addKey(iid).buildCompositeIndex();
      mgmt.commit();

      mgmt.awaitGraphIndexStatus(graph, "byIid").call();

      mgmt = (ManagementSystem) graph.openManagement();
      mgmt.updateIndex(mgmt.getGraphIndex("byIid"), SchemaAction.REINDEX)
        .get();
      mgmt.commit();

    } catch (Exception e) {
      logger.log(Level.SEVERE, e.toString());
      return;
    }

    // TODO: Make file list generation programmatic. This method of loading,
    // however, will be far too slow for anything other than the very 
    // smallest of SNB graphs, and is therefore quite transient. This will
    // do for now.
    String nodeFiles[] = {  
      "person_0_0.csv",
      "comment_0_0.csv",
      "forum_0_0.csv",
      "organisation_0_0.csv",
      "place_0_0.csv",
      "post_0_0.csv",
      "tag_0_0.csv",
      "tagclass_0_0.csv" 
    };

    String propertiesFiles[] = {    
      "person_email_emailaddress_0_0.csv",
      "person_speaks_language_0_0.csv"
    };

    String edgeFiles[] = {  
      "comment_hasCreator_person_0_0.csv",
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
          loadVertices(graph, Paths.get(inputBaseDir + "/" + fileName), 
              true, batchSize, progReportPeriod);
          System.out.println("Finished");
        } catch (NoSuchFileException e) {
          System.out.println(" File not found.");
        }
      }

      for (String fileName : propertiesFiles) {
        System.out.print("Loading properties file " + fileName + " ");
        try {
          loadProperties(graph, Paths.get(inputBaseDir + "/" + fileName), 
              true, batchSize, progReportPeriod);
          System.out.println("Finished");
        } catch (NoSuchFileException e) {
          System.out.println(" File not found.");
        }
      }

      for (String fileName : edgeFiles) {
        System.out.print("Loading edge file " + fileName + " ");
        try {
          if (fileName.contains("person_knows_person")) {
            loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), true, 
                true, batchSize, progReportPeriod);
          } else {
            loadEdges(graph, Paths.get(inputBaseDir + "/" + fileName), false, 
                true, batchSize, progReportPeriod);
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
      graph.close();
    }
  }
}
