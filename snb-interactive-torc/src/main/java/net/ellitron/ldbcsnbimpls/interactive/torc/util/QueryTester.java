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

import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery10Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery11Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery12Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery13Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery14Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery1Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery2Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery3Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery4Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery5Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery6Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery7Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery8Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcQuery9Handler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery1PersonProfileHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery2PersonPostsHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery3PersonFriendsHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery4MessageContentHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery5MessageCreatorHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery6MessageForumHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery7MessageRepliesHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDbConnectionState;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
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
import java.io.IOException;
import java.lang.reflect.Constructor;
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
      + "  --host=<host>       Host IP address of Neo4j webserver\n"
      + "                      [default: 127.0.0.1].\n"
      + "  --port=<port>       Port of Neo4j webserver [default: 7474].\n"
      + "  --repeat=<n>        How many times to repeat the query. If n > 1\n"
      + "                      then normal query result output will be\n"
      + "                      surpressed to show only the query timing\n"
      + "                      information\n"
      + "                      [default: 1].\n"
      + "  --input=<input>     Directory of updateStream files to use as\n"
      + "                      input for update queries (the nth update of\n"
      + "                      its kind will be selected from the stream to\n"
      + "                      execute) [default: ./].\n"
      + "  --timeUnits=<unit>  Unit of time in which to report timings\n"
      + "                      (SECONDS, MILLISECONDS, MICROSECONDS,\n"
      + "                      NANOSECONDS) [default: MILLISECONDS].\n"
      + "  -h --help           Show this screen.\n"
      + "  --version           Show version.\n"
      + "\n";

  /**
   * Represents all the types of short read queries on the graph in the LDBC
   * SNB interactive workload and their parameters. The order in the enum
   * matches the numbering of the short read ops.
   */
  private enum ShortReadOp {

    PERSONPROFILE(new String[]{"personId", "Long"},
        LdbcShortQuery1PersonProfile.class,
        LdbcShortQuery1PersonProfileHandler.class),
    PERSONPOSTS(new String[]{"personId", "Long", "limit", "Integer"},
        LdbcShortQuery2PersonPosts.class,
        LdbcShortQuery2PersonPostsHandler.class),
    PERSONFRIENDS(new String[]{"personId", "Long"},
        LdbcShortQuery3PersonFriends.class,
        LdbcShortQuery3PersonFriendsHandler.class),
    MESSAGECONTENT(new String[]{"messageId", "Long"},
        LdbcShortQuery4MessageContent.class,
        LdbcShortQuery4MessageContentHandler.class),
    MESSAGECREATOR(new String[]{"messageId", "Long"},
        LdbcShortQuery5MessageCreator.class,
        LdbcShortQuery5MessageCreatorHandler.class),
    MESSAGEFORUM(new String[]{"messageId", "Long"},
        LdbcShortQuery6MessageForum.class,
        LdbcShortQuery6MessageForumHandler.class),
    MESSAGEREPLIES(new String[]{"messageId", "Long"},
        LdbcShortQuery7MessageReplies.class,
        LdbcShortQuery7MessageRepliesHandler.class);

    /*
     * Ordered array of the constructor arguments and their corresponding types
     * for the class that represents operations of this type.
     */
    private final String[] ctorArgsAndTypes;

    /*
     * The class that represents operations of this type.
     */
    private final Class<?> opClass;

    /*
     * The class that represents the handler for operations of this type.
     */
    private final Class<?> opHandlerClass;

    private ShortReadOp(String[] ctorArgsAndTypes, Class<?> opClass,
        Class<?> opHandlerClass) {
      this.ctorArgsAndTypes = ctorArgsAndTypes;
      this.opClass = opClass;
      this.opHandlerClass = opHandlerClass;
    }

    public String[] getOpCtorArgsAndTypes() {
      return this.ctorArgsAndTypes;
    }

    public Class<?> getOpClass() {
      return this.opClass;
    }

    public Class<?> getOpHandlerClass() {
      return this.opHandlerClass;
    }
  }

  /**
   * Represents all the types of updates on the graph in the LDBC SNB
   * interactive workload and their parameters.
   */
  private enum UpdateOp {

    ADDPERSON(1, new String[]{"personId", "firstName", "lastName", "gender",
      "birthday", "creationDate", "locationIP", "browserUsed", "cityId",
      "speaks", "emails", "tagIds", "studyAt", "workAt"},
        LdbcUpdate1AddPerson.class),
    ADDPOSTLIKE(2, new String[]{"personId", "postId", "creationDate"},
        LdbcUpdate2AddPostLike.class),
    ADDCOMMENTLIKE(3, new String[]{"personId", "commentId", "creationDate"},
        LdbcUpdate3AddCommentLike.class),
    ADDFORUM(4, new String[]{"forumId", "forumTitle", "creationDate",
      "moderatorPersonId", "tagIds"},
        LdbcUpdate4AddForum.class),
    ADDFORUMMEMBERSHIP(5, new String[]{"forumId", "personId", "joinDate"},
        LdbcUpdate5AddForumMembership.class),
    ADDPOST(6, new String[]{"postId", "imageFile", "creationDate",
      "locationIP", "browserUsed", "language", "content", "length",
      "authorPersonId", "forumId", "countryId", "tagIds"},
        LdbcUpdate6AddPost.class),
    ADDCOMMENT(7, new String[]{"commentId", "creationDate", "locationIP",
      "browserUsed", "content", "length", "authorPersonId", "countryId",
      "replyToPostId", "replyToCommentId", "tagIds"},
        LdbcUpdate7AddComment.class),
    ADDFRIENDSHIP(8, new String[]{"person1Id", "person2Id", "creationDate"},
        LdbcUpdate8AddFriendship.class);

    /*
     * The number of the update as it is defined in LDBC SNB.
     */
    private final int number;

    /*
     * Ordered array of the parameters for this update, in the order they
     * appear in the LDBC SNB Data Generator dataset files for this type of
     * update.
     */
    private final String[] params;

    /*
     * The class that represents operations of this type.
     */
    private final Class<?> opClass;

    private UpdateOp(int number, String[] params, Class<?> opClass) {
      this.number = number;
      this.params = params;
      this.opClass = opClass;
    }

    public int getNumber() {
      return number;
    }

    public String[] getParams() {
      return params;
    }

    public Class<?> getOpClass() {
      return opClass;
    }

    /**
     * Return the update that has the given number.
     *
     * @return Update that has the given number.
     */
    public static UpdateOp getValueByNumber(int number) {
      UpdateOp update = null;
      for (UpdateOp u : UpdateOp.values()) {
        if (u.getNumber() == number) {
          update = u;
        }
      }

      return update;
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
  private static Operation<LdbcNoResult> parseUpdate(String line)
      throws InstantiationException, IllegalAccessException,
      IllegalArgumentException, InvocationTargetException {
    List<String> colVals = new ArrayList<>(Arrays.asList(line.split("\\|")));

    UpdateOp update = UpdateOp.getValueByNumber(Integer.decode(colVals.get(2)));
    String[] params = update.getParams();

    // If there are any left-off fields at the end, fill them in with blanks.
    while (colVals.size() - 3 < params.length) {
      colVals.add("");
    }

    List<Object> argList = new ArrayList<>();
    for (int i = 0; i < params.length; i++) {
      String fieldValue = colVals.get(3 + i);
      switch (paramDataTypes.get(params[i])) {
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
              paramDataTypes.get(params[i]), update.name()));
      }
    }

    Constructor ctor = update.getOpClass().getDeclaredConstructors()[0];
    return (Operation<LdbcNoResult>) ctor.newInstance(argList.toArray());
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

  public static void main(String[] args) throws DbException, IOException,
      InstantiationException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("QueryTester 1.0").parse(args);

    String host = (String) opts.get("--host");

    String port = (String) opts.get("--port");

    String inputDir = (String) opts.get("--input");

    int repeatCount = Integer.decode((String) opts.get("--repeat"));

    String timeUnits = (String) opts.get("--timeUnits");

    Neo4jDbConnectionState dbConnectionState =
        new Neo4jDbConnectionState(host, port);

    ResultReporter resultReporter =
        new ResultReporter.SimpleResultReporter(
            new ConcurrentErrorReporter());

    if ((Boolean) opts.get("query1")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      String firstName = (String) opts.get("<firstName>");
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery1Handler opHandler = new LdbcQuery1Handler();
      LdbcQuery1 op = new LdbcQuery1(personId, firstName, limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query2")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      Date maxDate = new Date(Long.decode((String) opts.get("<maxDate>")));
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery2Handler opHandler = new LdbcQuery2Handler();
      LdbcQuery2 op = new LdbcQuery2(personId, maxDate, limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query3")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      String countryXName = (String) opts.get("<countryXName>");
      String countryYName = (String) opts.get("<countryYName>");
      Date startDate = new Date(Long.decode((String) opts.get("<startDate>")));
      int durationDays = Integer.decode((String) opts.get("<durationDays>"));
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery3Handler opHandler = new LdbcQuery3Handler();
      LdbcQuery3 op = new LdbcQuery3(
          personId,
          countryXName,
          countryYName,
          startDate,
          durationDays,
          limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query4")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      Date startDate = new Date(Long.decode((String) opts.get("<startDate>")));
      int durationDays = Integer.decode((String) opts.get("<durationDays>"));
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery4Handler opHandler = new LdbcQuery4Handler();
      LdbcQuery4 op = new LdbcQuery4(
          personId,
          startDate,
          durationDays,
          limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query5")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      Date minDate = new Date(Long.decode((String) opts.get("<minDate>")));
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery5Handler opHandler = new LdbcQuery5Handler();
      LdbcQuery5 op = new LdbcQuery5(
          personId,
          minDate,
          limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query6")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      String tagName = (String) opts.get("<tagName>");
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery6Handler opHandler = new LdbcQuery6Handler();
      LdbcQuery6 op = new LdbcQuery6(
          personId,
          tagName,
          limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query7")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery7Handler opHandler = new LdbcQuery7Handler();
      LdbcQuery7 op = new LdbcQuery7(
          personId,
          limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query8")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery8Handler opHandler = new LdbcQuery8Handler();
      LdbcQuery8 op = new LdbcQuery8(
          personId,
          limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query9")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      Date maxDate = new Date(Long.decode((String) opts.get("<maxDate>")));
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery9Handler opHandler = new LdbcQuery9Handler();
      LdbcQuery9 op = new LdbcQuery9(personId, maxDate, limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query10")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      int month = Integer.decode((String) opts.get("<month>"));
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery10Handler opHandler = new LdbcQuery10Handler();
      LdbcQuery10 op = new LdbcQuery10(personId, month, limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query11")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      String countryName = (String) opts.get("<countryName>");
      int workFromYear = Integer.decode((String) opts.get("<workFromYear>"));
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery11Handler opHandler = new LdbcQuery11Handler();
      LdbcQuery11 op = new LdbcQuery11(
          personId,
          countryName,
          workFromYear,
          limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query12")) {
      long personId = Long.decode((String) opts.get("<personId>"));
      String tagClassName = (String) opts.get("<tagClassName>");
      int limit = Integer.decode((String) opts.get("<limit>"));

      LdbcQuery12Handler opHandler = new LdbcQuery12Handler();
      LdbcQuery12 op = new LdbcQuery12(
          personId,
          tagClassName,
          limit);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query13")) {
      long person1Id = Long.decode((String) opts.get("<person1Id>"));
      long person2Id = Long.decode((String) opts.get("<person2Id>"));

      LdbcQuery13Handler opHandler = new LdbcQuery13Handler();
      LdbcQuery13 op = new LdbcQuery13(person1Id, person2Id);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("query14")) {
      long person1Id = Long.decode((String) opts.get("<person1Id>"));
      long person2Id = Long.decode((String) opts.get("<person2Id>"));

      LdbcQuery14Handler opHandler = new LdbcQuery14Handler();
      LdbcQuery14 op = new LdbcQuery14(person1Id, person2Id);

      execAndTimeQuery(opHandler, op, dbConnectionState, resultReporter,
          repeatCount, timeUnits);

    } else if ((Boolean) opts.get("shortquery1")
        || (Boolean) opts.get("shortquery2")
        || (Boolean) opts.get("shortquery3")
        || (Boolean) opts.get("shortquery4")
        || (Boolean) opts.get("shortquery5")
        || (Boolean) opts.get("shortquery6")
        || (Boolean) opts.get("shortquery7")) {
      // Extract the number of the short query operation we want to perform.
      int opNumber = 0;
      for (int i = 1; i <= 7; i++) {
        if ((Boolean) opts.get("shortquery" + i)) {
          opNumber = i;
          break;
        }
      }

      ShortReadOp srOp = ShortReadOp.values()[opNumber - 1];

      String[] opCtorArgsAndTypes = srOp.getOpCtorArgsAndTypes();
      List<Object> argList = new ArrayList<>();
      for (int i = 0; i < opCtorArgsAndTypes.length; i += 2) {
        String argOpt = "<" + opCtorArgsAndTypes[i] + ">";
        String type = opCtorArgsAndTypes[i + 1];

        String argValue = (String) opts.get(argOpt);
        switch (type) {
          case "Long":
            argList.add(Long.decode(argValue));
            break;
          case "Integer":
            argList.add(Integer.decode(argValue));
            break;
          default:
            throw new RuntimeException(String.format("Don't know how to parse "
                + "arg of type %s for short read type %s",
                type, srOp.name()));
        }
      }

      Object opHandler = srOp.getOpHandlerClass().newInstance();
      Object op = srOp.getOpClass().getDeclaredConstructors()[0]
          .newInstance(argList.toArray());

      long startTime = 0;
      long endTime = 0;
      switch (opNumber) {
        case 1: {
          execAndTimeQuery((LdbcShortQuery1PersonProfileHandler) opHandler,
              (LdbcShortQuery1PersonProfile) op, dbConnectionState,
              resultReporter, repeatCount, timeUnits);
          break;
        }
        case 2: {
          execAndTimeQuery((LdbcShortQuery2PersonPostsHandler) opHandler,
              (LdbcShortQuery2PersonPosts) op, dbConnectionState,
              resultReporter, repeatCount, timeUnits);
          break;
        }
        case 3: {
          execAndTimeQuery((LdbcShortQuery3PersonFriendsHandler) opHandler,
              (LdbcShortQuery3PersonFriends) op, dbConnectionState,
              resultReporter, repeatCount, timeUnits);
          break;
        }
        case 4: {
          execAndTimeQuery((LdbcShortQuery4MessageContentHandler) opHandler,
              (LdbcShortQuery4MessageContent) op, dbConnectionState,
              resultReporter, repeatCount, timeUnits);
          break;
        }
        case 5: {
          execAndTimeQuery((LdbcShortQuery5MessageCreatorHandler) opHandler,
              (LdbcShortQuery5MessageCreator) op, dbConnectionState,
              resultReporter, repeatCount, timeUnits);
          break;
        }
        case 6: {
          execAndTimeQuery((LdbcShortQuery6MessageForumHandler) opHandler,
              (LdbcShortQuery6MessageForum) op, dbConnectionState,
              resultReporter, repeatCount, timeUnits);
          break;
        }
        case 7: {
          execAndTimeQuery((LdbcShortQuery7MessageRepliesHandler) opHandler,
              (LdbcShortQuery7MessageReplies) op, dbConnectionState,
              resultReporter, repeatCount, timeUnits);
          break;
        }
        default:
          throw new RuntimeException("ERROR: Encountered unknown short read "
              + "operation number " + opNumber + "!");
      }

    } else if ((Boolean) opts.get("update1") || (Boolean) opts.get("update2")
        || (Boolean) opts.get("update3") || (Boolean) opts.get("update4")
        || (Boolean) opts.get("update5") || (Boolean) opts.get("update6")
        || (Boolean) opts.get("update7") || (Boolean) opts.get("update8")) {
      // Extract the number of the update operation we want to perform.
      int opNumber = 0;
      for (int i = 1; i <= 8; i++) {
        if ((Boolean) opts.get("update" + i)) {
          opNumber = i;
          break;
        }
      }

      String fileName;
      if (opNumber == 1) {
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
        if (Integer.decode(line.split("\\|")[2]) == opNumber) {
          readCount++;

          if (readCount == nth) {
            Operation<LdbcNoResult> op = parseUpdate(line);

            long startTime, endTime;
            switch (opNumber) {
              case 1: {
                OperationHandler<LdbcUpdate1AddPerson, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate1AddPersonHandler();
                execAndTimeQuery(opHandler, (LdbcUpdate1AddPerson) op,
                    dbConnectionState, resultReporter, repeatCount, timeUnits);
                break;
              }
              case 2: {
                OperationHandler<LdbcUpdate2AddPostLike, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate2AddPostLikeHandler();
                execAndTimeQuery(opHandler, (LdbcUpdate2AddPostLike) op,
                    dbConnectionState, resultReporter, repeatCount, timeUnits);
                break;
              }
              case 3: {
                OperationHandler<LdbcUpdate3AddCommentLike, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate3AddCommentLikeHandler();
                execAndTimeQuery(opHandler, (LdbcUpdate3AddCommentLike) op,
                    dbConnectionState, resultReporter, repeatCount, timeUnits);
                break;
              }
              case 4: {
                OperationHandler<LdbcUpdate4AddForum, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate4AddForumHandler();
                execAndTimeQuery(opHandler, (LdbcUpdate4AddForum) op,
                    dbConnectionState, resultReporter, repeatCount, timeUnits);
                break;
              }
              case 5: {
                OperationHandler<LdbcUpdate5AddForumMembership, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate5AddForumMembershipHandler();
                execAndTimeQuery(opHandler, (LdbcUpdate5AddForumMembership) op,
                    dbConnectionState, resultReporter, repeatCount, timeUnits);
                break;
              }
              case 6: {
                OperationHandler<LdbcUpdate6AddPost, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate6AddPostHandler();
                execAndTimeQuery(opHandler, (LdbcUpdate6AddPost) op,
                    dbConnectionState, resultReporter, repeatCount, timeUnits);
                break;
              }
              case 7: {
                OperationHandler<LdbcUpdate7AddComment, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate7AddCommentHandler();
                execAndTimeQuery(opHandler, (LdbcUpdate7AddComment) op,
                    dbConnectionState, resultReporter, repeatCount, timeUnits);
                break;
              }
              case 8: {
                OperationHandler<LdbcUpdate8AddFriendship, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate8AddFriendshipHandler();
                execAndTimeQuery(opHandler, (LdbcUpdate8AddFriendship) op,
                    dbConnectionState, resultReporter, repeatCount, timeUnits);
                break;
              }
              default:
                throw new RuntimeException("ERROR: Encountered unknown update "
                    + "operation number " + opNumber + "!");
            }

            break;
          }
        }
      }

      inFile.close();

      if (readCount < nth) {
        System.out.println(String.format("ERROR: File %s only contains %d"
            + " update%d ops, but user requested execution of update #%d.",
            path.toAbsolutePath(), readCount, opNumber, nth));
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
