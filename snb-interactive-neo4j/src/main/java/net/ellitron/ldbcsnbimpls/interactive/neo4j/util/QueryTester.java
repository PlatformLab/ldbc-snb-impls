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
package net.ellitron.ldbcsnbimpls.interactive.neo4j.util;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson.Organization;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import org.docopt.Docopt;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A utility for running individual queries for testing purposes. 
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class QueryTester {

  private static final String doc =
      "QueryTester: A utility for running individual queries for testing "
      + "purposes.\n"
      + "\n"
      + "Usage:\n"
      + "  QueryTester [options] query1 <personId> <firstName> <limit>\n"
      + "  QueryTester [options] query2 <personId> <maxDate> <limit>\n"
      + "  QueryTester [options] query3 <personId> <countryXName> <countryYName> <startDate> <durationDays> <limit>\n"
      + "  QueryTester [options] query4 <personId> <startDate> <durationDays> <limit>\n"
      + "  QueryTester [options] query5 <personId> <minDate> <limit>\n"
      + "  QueryTester [options] query6 <personId> <tagName> <limit>\n"
      + "  QueryTester [options] query7 <personId> <limit>\n"
      + "  QueryTester [options] query8 <personId> <limit>\n"
      + "  QueryTester [options] query9 <personId> <maxDate> <limit>\n"
      + "  QueryTester [options] query10 <personId> <month> <limit>\n"
      + "  QueryTester [options] query11 <personId> <countryName> <workFromYear> <limit>\n"
      + "  QueryTester [options] query12 <personId> <tagClassName> <limit>\n"
      + "  QueryTester [options] query13 <person1Id> <person2Id>\n"
      + "  QueryTester [options] query14 <person1Id> <person2Id>\n"
      + "  QueryTester [options] shortquery1 <personId>\n"
      + "  QueryTester [options] shortquery2 <personId> <limit>\n"
      + "  QueryTester [options] shortquery3 <personId>\n"
      + "  QueryTester [options] shortquery4 <messageId>\n"
      + "  QueryTester [options] shortquery5 <messageId>\n"
      + "  QueryTester [options] shortquery6 <messageId>\n"
      + "  QueryTester [options] shortquery7 <messageId>\n"
      + "  QueryTester [options] update1 <nth>\n"
      + "  QueryTester [options] update2 <nth>\n"
      + "  QueryTester [options] update3 <nth>\n"
      + "  QueryTester [options] update4 <nth>\n"
      + "  QueryTester [options] update5 <nth>\n"
      + "  QueryTester [options] update6 <nth>\n"
      + "  QueryTester [options] update7 <nth>\n"
      + "  QueryTester [options] update8 <nth>\n"
      + "  QueryTester (-h | --help)\n"
      + "  QueryTester --version\n"
      + "\n"
      + "Options:\n"
      + "  --config=<file>      QueryTester configuration file\n"
      + "                       [default: ./config/querytester.properties].\n"
      + "  --repeat=<n>         How many times to repeat the query. If n > 1\n"
      + "                       then normal query result output will be\n"
      + "                       surpressed to show only the query timing\n"
      + "                       information\n"
      + "                       [default: 1].\n"
      + "  --input=<input>      Directory of updateStream files to use as\n"
      + "                       input for update queries (the nth update of\n"
      + "                       its kind will be selected from the stream to\n"
      + "                       execute) [default: ./].\n"
      + "  --timeUnits=<unit>   Unit of time in which to report timings\n"
      + "                       (SECONDS, MILLISECONDS, MICROSECONDS,\n"
      + "                       NANOSECONDS) [default: MILLISECONDS].\n"
      + "  -h --help            Show this screen.\n"
      + "  --version            Show version.\n"
      + "\n";

  /**
   * Represents the set of query operations the user can execute. Co-locates
   * information about each query such as the QueryTester command that refers
   * to that query, the Operation class associated with that query, the
   * parameter types to the constructor of the Operation class, the QueryTester
   * arguments that supply the values to those constructor parameters, and the
   * configuration key used in the QueryTester configuration file to set the
   * specific handler implementation to use for this query type.
   *
   * Only complex read queries and short read queries are defined here. Update
   * queries are currently handled differently (update operation parameters are
   * taken from an update stream file).
   */
  private enum ComplexAndShortOp {

    QUERY1("query1",
        LdbcQuery1.class,
        //long personId, String firstName, int limit
        new Class<?>[]{Long.class, String.class, Integer.class},
        new String[]{"<personId>", "<firstName>", "<limit>"},
        "LdbcQuery1Handler"),
    QUERY2("query2",
        LdbcQuery2.class,
        //long personId, Date maxDate, int limit
        new Class<?>[]{Long.class, Date.class, Integer.class},
        new String[]{"<personId>", "<maxDate>", "<limit>"},
        "LdbcQuery2Handler"),
    QUERY3("query3",
        LdbcQuery3.class,
        // long personId, String countryXName, String countryYName, Date startDate, int durationDays, int limit 
        new Class<?>[]{Long.class, String.class, String.class, Date.class,
          Integer.class, Integer.class},
        new String[]{"<personId>", "<countryXName>", "<countryYName>",
          "<startDate>", "<durationDays>", "<limit>"},
        "LdbcQuery3Handler"),
    QUERY4("query4",
        LdbcQuery4.class,
        // long personId, Date startDate, int durationDays, int limit 
        new Class<?>[]{Long.class, Date.class, Integer.class, Integer.class},
        new String[]{"<personId>", "<startDate>", "<durationDays>", "<limit>"},
        "LdbcQuery4Handler"),
    QUERY5("query5",
        LdbcQuery5.class,
        // long personId, Date minDate, int limit 
        new Class<?>[]{Long.class, Date.class, Integer.class},
        new String[]{"<personId>", "<minDate>", "<limit>"},
        "LdbcQuery5Handler"),
    QUERY6("query6",
        LdbcQuery6.class,
        // long personId, String tagName, int limit 
        new Class<?>[]{Long.class, String.class, Integer.class},
        new String[]{"<personId>", "<tagName>", "<limit>"},
        "LdbcQuery6Handler"),
    QUERY7("query7",
        LdbcQuery7.class,
        // long personId, int limit 
        new Class<?>[]{Long.class, Integer.class},
        new String[]{"<personId>", "<limit>"},
        "LdbcQuery7Handler"),
    QUERY8("query8",
        LdbcQuery8.class,
        // long personId, int limit 
        new Class<?>[]{Long.class, Integer.class},
        new String[]{"<personId>", "<limit>"},
        "LdbcQuery8Handler"),
    QUERY9("query9",
        LdbcQuery9.class,
        // long personId, Date maxDate, int limit 
        new Class<?>[]{Long.class, Date.class, Integer.class},
        new String[]{"<personId>", "<maxDate>", "<limit>"},
        "LdbcQuery9Handler"),
    QUERY10("query10",
        LdbcQuery10.class,
        // long personId, int month, int limit 
        new Class<?>[]{Long.class, Integer.class, Integer.class},
        new String[]{"<personId>", "<month>", "<limit>"},
        "LdbcQuery10Handler"),
    QUERY11("query11",
        LdbcQuery11.class,
        // long personId, String countryName, int workFromYear, int limit 
        new Class<?>[]{Long.class, String.class, Integer.class, Integer.class},
        new String[]{"<personId>", "<countryName>", "<workFromYear>", "<limit>"},
        "LdbcQuery11Handler"),
    QUERY12("query12",
        LdbcQuery12.class,
        // long personId, String tagClassName, int limit 
        new Class<?>[]{Long.class, String.class, Integer.class},
        new String[]{"<personId>", "<tagClassName>", "<limit>"},
        "LdbcQuery12Handler"),
    QUERY13("query13",
        LdbcQuery13.class,
        // long person1Id, long person2Id 
        new Class<?>[]{Long.class, Long.class},
        new String[]{"<person1Id>", "<person2Id>"},
        "LdbcQuery13Handler"),
    QUERY14("query14",
        LdbcQuery14.class,
        // long person1Id, long person2Id 
        new Class<?>[]{Long.class, Long.class},
        new String[]{"<person1Id>", "<person2Id>"},
        "LdbcQuery14Handler"),
    SHORTQUERY1("shortquery1",
        LdbcShortQuery1PersonProfile.class,
        // long personId,
        new Class<?>[]{Long.class},
        new String[]{"<personId>"},
        "LdbcShortQuery1PersonProfileHandler"),
    SHORTQUERY2("shortquery2",
        LdbcShortQuery2PersonPosts.class,
        // long personId, int limit,
        new Class<?>[]{Long.class, Integer.class},
        new String[]{"<personId>", "<limit>"},
        "LdbcShortQuery2PersonPostsHandler"),
    SHORTQUERY3("shortquery3",
        LdbcShortQuery3PersonFriends.class,
        // long personId,
        new Class<?>[]{Long.class},
        new String[]{"<personId>"},
        "LdbcShortQuery3PersonFriendsHandler"),
    SHORTQUERY4("shortquery4",
        LdbcShortQuery4MessageContent.class,
        // long messageId,
        new Class<?>[]{Long.class},
        new String[]{"<messageId>"},
        "LdbcShortQuery4MessageContentHandler"),
    SHORTQUERY5("shortquery5",
        LdbcShortQuery5MessageCreator.class,
        // long messageId,
        new Class<?>[]{Long.class},
        new String[]{"<messageId>"},
        "LdbcShortQuery5MessageCreatorHandler"),
    SHORTQUERY6("shortquery6",
        LdbcShortQuery6MessageForum.class,
        // long messageId,
        new Class<?>[]{Long.class},
        new String[]{"<messageId>"},
        "LdbcShortQuery6MessageForumHandler"),
    SHORTQUERY7("shortquery7",
        LdbcShortQuery7MessageReplies.class,
        // long messageId,
        new Class<?>[]{Long.class},
        new String[]{"<messageId>"},
        "LdbcShortQuery7MessageRepliesHandler");

    /*
     * Command used to refer to this query at the command line.
     */
    public final String command;

    /*
     * Class that defines query operations of this type.
     */
    public final Class<?> opClass;

    /*
     * Parameter types for the opClass constructor.
     */
    public final Class<?>[] opCtorParamTypes;

    /*
     * The ith entry is the name of the command argument that feeds the ith
     * parameter to the opClass constructor.
     */
    public final String[] opCtorParamVals;

    /*
     * The key of the property that holds the full name of the class that
     * implements the OperationHandler for this query.
     */
    public final String opHandlerConfigKey;

    private ComplexAndShortOp(String command, Class<?> opClass,
        Class<?>[] opCtorParamTypes, String[] opCtorParamVals,
        String opHandlerConfigKey) {
      this.command = command;
      this.opClass = opClass;
      this.opCtorParamTypes = opCtorParamTypes;
      this.opCtorParamVals = opCtorParamVals;
      this.opHandlerConfigKey = opHandlerConfigKey;
    }
  }

  /**
   * Represents all the types of updates on the graph in the LDBC SNB
   * interactive workload and their parameters.
   */
  private enum UpdateOp {

    UPDATE1("update1",
        1,
        new String[]{"personId", "firstName", "lastName", "gender",
          "birthday", "creationDate", "locationIP", "browserUsed", "cityId",
          "speaks", "emails", "tagIds", "studyAt", "workAt"},
        LdbcUpdate1AddPerson.class,
        "LdbcUpdate1AddPersonHandler"),
    UPDATE2("update2",
        2,
        new String[]{"personId", "postId", "creationDate"},
        LdbcUpdate2AddPostLike.class,
        "LdbcUpdate2AddPostLikeHandler"),
    UPDATE3("update3",
        3,
        new String[]{"personId", "commentId", "creationDate"},
        LdbcUpdate3AddCommentLike.class,
        "LdbcUpdate3AddCommentLikeHandler"),
    UPDATE4("update4",
        4,
        new String[]{"forumId", "forumTitle", "creationDate",
          "moderatorPersonId", "tagIds"},
        LdbcUpdate4AddForum.class,
        "LdbcUpdate4AddForumHandler"),
    UPDATE5("update5",
        5,
        new String[]{"forumId", "personId", "joinDate"},
        LdbcUpdate5AddForumMembership.class,
        "LdbcUpdate5AddForumMembershipHandler"),
    UPDATE6("update6",
        6,
        new String[]{"postId", "imageFile", "creationDate",
          "locationIP", "browserUsed", "language", "content", "length",
          "authorPersonId", "forumId", "countryId", "tagIds"},
        LdbcUpdate6AddPost.class,
        "LdbcUpdate6AddPostHandler"),
    UPDATE7("update7",
        7,
        new String[]{"commentId", "creationDate", "locationIP",
          "browserUsed", "content", "length", "authorPersonId", "countryId",
          "replyToPostId", "replyToCommentId", "tagIds"},
        LdbcUpdate7AddComment.class,
        "LdbcUpdate7AddCommentHandler"),
    UPDATE8("update8",
        8,
        new String[]{"person1Id", "person2Id", "creationDate"},
        LdbcUpdate8AddFriendship.class,
        "LdbcUpdate8AddFriendshipHandler");

    /*
     * Command used to refer to this query at the command line.
     */
    public final String command;

    /*
     * The number of the update as it is defined in LDBC SNB and used in
     * dataset files.
     */
    public final int index;

    /*
     * Ordered array of the parameters for this update, in the order they
     * appear in the LDBC SNB Data Generator dataset files for this type of
     * update.
     */
    public final String[] params;

    /*
     * The class that represents operations of this type.
     */
    public final Class<?> opClass;

    /*
     * The key of the property that holds the full name of the class that
     * implements the OperationHandler for this query.
     */
    public final String opHandlerConfigKey;

    private UpdateOp(String command, int index, String[] params,
        Class<?> opClass, String opHandlerConfigKey) {
      this.command = command;
      this.index = index;
      this.params = params;
      this.opClass = opClass;
      this.opHandlerConfigKey = opHandlerConfigKey;
    }
  }

  /*
   * A mapping between the name of the operation parameter and the Java
   * datatype that it represents. This informs the parser how to interpret and
   * deserialize the value in this field in the file.
   */
  private static final Map<String, String> paramDataTypes;

  static {
    Map<String, String> dataTypeMap = new HashMap<>();
    dataTypeMap.put("authorPersonId", "Long");
    dataTypeMap.put("birthday", "Date");
    dataTypeMap.put("browserUsed", "String");
    dataTypeMap.put("cityId", "Long");
    dataTypeMap.put("commentId", "Long");
    dataTypeMap.put("content", "String");
    dataTypeMap.put("countryId", "Long");
    dataTypeMap.put("creationDate", "Date");
    dataTypeMap.put("emails", "List<String>");
    dataTypeMap.put("firstName", "String");
    dataTypeMap.put("forumId", "Long");
    dataTypeMap.put("forumTitle", "String");
    dataTypeMap.put("gender", "String");
    dataTypeMap.put("imageFile", "String");
    dataTypeMap.put("joinDate", "Date");
    dataTypeMap.put("language", "String");
    dataTypeMap.put("lastName", "String");
    dataTypeMap.put("length", "Integer");
    dataTypeMap.put("locationIP", "String");
    dataTypeMap.put("moderatorPersonId", "Long");
    dataTypeMap.put("person1Id", "Long");
    dataTypeMap.put("person2Id", "Long");
    dataTypeMap.put("personId", "Long");
    dataTypeMap.put("postId", "Long");
    dataTypeMap.put("replyToCommentId", "Long");
    dataTypeMap.put("replyToPostId", "Long");
    dataTypeMap.put("speaks", "List<String>");
    dataTypeMap.put("studyAt", "List<Organization>");
    dataTypeMap.put("tagIds", "List<Long>");
    dataTypeMap.put("workAt", "List<Organization>");

    paramDataTypes = Collections.unmodifiableMap(dataTypeMap);
  }

  /**
   * Parses a line of an LDBC SNB interactive workload update stream file into
   * an instance of the operation that represents it.
   *
   * @param line Line of update stream file.
   *
   * @return Instance of the update operation that represented by this line in
   * the update stream file. The returned object will be an instance of one
   * of:<br>
   * <ul>
   * <li>LdbcUpdate1AddPerson</li>
   * <li>LdbcUpdate2AddPostLike</li>
   * <li>LdbcUpdate3AddCommentLike</li>
   * <li>LdbcUpdate4AddForum</li>
   * <li>LdbcUpdate5AddForumMembership</li>
   * <li>LdbcUpdate6AddPost</li>
   * <li>LdbcUpdate7AddComment</li>
   * <li>LdbcUpdate8AddFriendship</li>
   * </ul>
   */
  private static Operation parseUpdate(UpdateOp update, String line)
      throws InstantiationException, IllegalAccessException,
      IllegalArgumentException, InvocationTargetException {

    List<String> colVals = new ArrayList<>(Arrays.asList(line.split("\\|")));

    // If there are any left-off fields at the end, fill them in with blanks.
    while (colVals.size() - 3 < update.params.length) {
      colVals.add("");
    }

    List<Object> argList = new ArrayList<>();
    for (int i = 0; i < update.params.length; i++) {
      String fieldValue = colVals.get(3 + i);
      switch (paramDataTypes.get(update.params[i])) {
        case "Date":
          argList.add(new Date(Long.decode(fieldValue)));
          break;
        case "Integer":
          argList.add(Integer.decode(fieldValue));
          break;
        case "List<Long>":
          List<Long> numList = new ArrayList<>();
          if (fieldValue.length() > 0) {
            for (String num : fieldValue.split(";")) {
              numList.add(Long.decode(num));
            }
          }
          argList.add(numList);
          break;
        case "List<Organization>":
          List<Organization> orgList = new ArrayList<>();
          if (fieldValue.length() > 0) {
            for (String org : fieldValue.split(";")) {
              String[] placeAndYear = org.split(",");
              long orgId = Long.decode(placeAndYear[0]);
              int year = Integer.decode(placeAndYear[1]);
              orgList.add(new Organization(orgId, year));
            }
          }
          argList.add(orgList);
          break;
        case "List<String>":
          argList.add(Arrays.asList(fieldValue.split(";")));
          break;
        case "Long":
          argList.add(Long.decode(fieldValue));
          break;
        case "String":
          argList.add(fieldValue);
          break;
        default:
          throw new RuntimeException(String.format("Don't know how to parse "
              + "field of type %s for update type %s",
              paramDataTypes.get(update.params[i]), update.name()));
      }
    }

    return (Operation) update.opClass.getDeclaredConstructors()[0]
        .newInstance(argList.toArray());
  }

  public static <R, T extends Operation<R>, S extends DbConnectionState> void
      execAndTimeQuery(OperationHandler<T, S> opHandler, T op,
          S connectionState, ResultReporter resultReporter, int repeatCount,
          String timeUnits)
      throws DbException {

    Long[] timings = new Long[repeatCount];

    long startTime, endTime;
    for (int i = 0; i < repeatCount; i++) {
      startTime = System.nanoTime();
      opHandler.executeOperation(op, connectionState, resultReporter);
      endTime = System.nanoTime();
      timings[i] = endTime - startTime;
    }

    Arrays.sort(timings);

    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = 0;
    for (int i = 0; i < timings.length; i++) {
      sum += timings[i];

      if (timings[i] < min) {
        min = timings[i];
      }

      if (timings[i] > max) {
        max = timings[i];
      }
    }

    long mean = sum / repeatCount;

    int p25 = (int) (0.25 * (float) repeatCount);
    int p50 = (int) (0.50 * (float) repeatCount);
    int p75 = (int) (0.75 * (float) repeatCount);
    int p90 = (int) (0.90 * (float) repeatCount);
    int p95 = (int) (0.95 * (float) repeatCount);
    int p99 = (int) (0.99 * (float) repeatCount);

    long nanosPerTimeUnit;

    switch (timeUnits) {
      case "NANOSECONDS":
        nanosPerTimeUnit = 1;
        break;
      case "MICROSECONDS":
        nanosPerTimeUnit = 1000;
        break;
      case "MILLISECONDS":
        nanosPerTimeUnit = 1000000;
        break;
      case "SECONDS":
        nanosPerTimeUnit = 1000000000;
        break;
      default:
        throw new RuntimeException("Unrecognized time unit: " + timeUnits);
    }

    System.out.println("Query:");
    System.out.println(op.toString());
    System.out.println();
    System.out.println(String.format(
        "Query Stats:\n"
        + "  Units:            %s\n"
        + "  Count:            %d\n"
        + "  Min:              %d\n"
        + "  Max:              %d\n"
        + "  Mean:             %d\n"
        + "  25th Percentile:  %d\n"
        + "  50th Percentile:  %d\n"
        + "  75th Percentile:  %d\n"
        + "  90th Percentile:  %d\n"
        + "  95th Percentile:  %d\n"
        + "  99th Percentile:  %d\n",
        timeUnits,
        repeatCount,
        min / nanosPerTimeUnit,
        max / nanosPerTimeUnit,
        mean / nanosPerTimeUnit,
        timings[p25] / nanosPerTimeUnit,
        timings[p50] / nanosPerTimeUnit,
        timings[p75] / nanosPerTimeUnit,
        timings[p90] / nanosPerTimeUnit,
        timings[p95] / nanosPerTimeUnit,
        timings[p99] / nanosPerTimeUnit));
  }

  public static OperationHandler<? extends Operation, DbConnectionState>
      getOpHandler(String className) throws Exception {
    return (OperationHandler<? extends Operation, DbConnectionState>) Class
        .forName(className).getConstructor().newInstance();
  }

  public static void main(String[] args) throws Exception {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("QueryTester 1.0").parse(args);

    // Get values of general options.
    String inputDir = (String) opts.get("--input");
    int repeatCount = Integer.decode((String) opts.get("--repeat"));
    String timeUnits = (String) opts.get("--timeUnits");

    // Load properties from the configuration file.
    String configFilename = (String) opts.get("--config");
    Properties prop = new Properties();
    prop.load(new FileInputStream(configFilename));

    // Find out which database we'll be testing this fine day.
    String dbName = prop.getProperty("db");

    /*
     * Construct the DbConnectionState object from the given properties in the
     * configuration file.
     */
    Map<String, String> config = new HashMap<>();
    String propKeyPrefix = dbName + ".DbConnectionState.";
    prop.stringPropertyNames().stream()
        .filter((propName) -> (propName.startsWith(propKeyPrefix)))
        .forEach((propName) -> {
          String configKey = propName.substring(propKeyPrefix.length());
          config.put(configKey, prop.getProperty(propName));
        });
    DbConnectionState dbConnectionState =
        (DbConnectionState) Class
        .forName(prop.getProperty(dbName + ".DbConnectionState"))
        .getDeclaredConstructors()[0]
        .newInstance(config);

    // Queries will dump their results into this result reporter object.
    ResultReporter resultReporter =
        new ResultReporter.SimpleResultReporter(
            new ConcurrentErrorReporter());

    // Figure out which query the user wants to run.
    ComplexAndShortOp csop = null;
    for (ComplexAndShortOp op : ComplexAndShortOp.values()) {
      if ((Boolean) opts.get(op.command)) {
        csop = op;
      }
    }

    if (csop != null) {
      /*
       * Use information in the QueryOp and some java reflection magic to
       * generically construct the Operation and configured OperationHandler
       * for this query. Here we go!
       */
      // First, gather up all the Operation constructor arguments and construct
      // Operation instance.
      List<Object> opCtorArgs = new ArrayList<>();
      for (int i = 0; i < csop.opCtorParamTypes.length; i++) {
        String argValue = (String) opts.get(csop.opCtorParamVals[i]);
        switch (csop.opCtorParamTypes[i].getSimpleName()) {
          case "Date": {
            opCtorArgs.add(new Date(Long.decode(argValue)));
            break;
          }
          case "Integer": {
            opCtorArgs.add(Integer.decode(argValue));
            break;
          }
          case "Long": {
            opCtorArgs.add(Long.decode(argValue));
            break;
          }
          case "String": {
            opCtorArgs.add(argValue);
            break;
          }
          default: {
            throw new RuntimeException(String.format("Unrecognized parameter "
                + "type for %s constructor: %s", csop.opClass,
                csop.opCtorParamTypes[i]));
          }
        }
      }
      Operation op = (Operation) csop.opClass
          .getDeclaredConstructors()[0]
          .newInstance(opCtorArgs.toArray());

      // Now construct an instance of the OperationHandler type specified in 
      // the configuration file.
      OperationHandler opHandler = (OperationHandler) Class
          .forName(prop.getProperty(dbName + "." + csop.opHandlerConfigKey))
          .getDeclaredConstructor().newInstance();

      // Let 'er rip!
      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);
    }

    UpdateOp uop = null;
    for (UpdateOp op : UpdateOp.values()) {
      if ((Boolean) opts.get(op.command)) {
        uop = op;
      }
    }

    if (uop != null) {
      // First construct an instance of the OperationHandler type specified in 
      // the configuration file.
      OperationHandler opHandler = (OperationHandler) Class
          .forName(prop.getProperty(dbName + "." + uop.opHandlerConfigKey))
          .getDeclaredConstructor().newInstance();

      String fileName;
      if (uop.index == 1) {
        fileName = "updateStream_0_0_person.csv";
      } else {
        fileName = "updateStream_0_0_forum.csv";
      }

      Path path = Paths.get(inputDir + "/" + fileName);
      BufferedReader inFile =
          Files.newBufferedReader(path, StandardCharsets.UTF_8);

      Long nth = Long.decode((String) opts.get("<nth>"));

      // Track how many update ops of this type we have read from the file.
      long readCount = 0;
      String line;
      while ((line = inFile.readLine()) != null) {
        if (Integer.decode(line.split("\\|")[2]) == uop.index) {
          readCount++;

          if (readCount == nth) {
            Operation op = parseUpdate(uop, line);

            execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
                repeatCount, timeUnits);

            break;
          }
        }
      }

      inFile.close();

      if (readCount < nth) {
        System.out.println(String.format("ERROR: File %s only contains %d"
            + " update%d ops, but user requested execution of update #%d.",
            path.toAbsolutePath(), readCount, uop.index, nth));
      }
    }

    if (repeatCount == 1) {
      printResult(resultReporter.result());
    }
  }

  /**
   * Print a result from executing one of the queries.
   */
  private static void printResult(Object result) {
    if (result == null) {
      System.out.println("Query returned no results.");
      return;
    }

    if (result instanceof List) {
      List<?> list = (List) result;
      for (Object o : list) {
        System.out.println(o.toString());
      }
    } else {
      System.out.println(result.toString());
    }
  }
}
