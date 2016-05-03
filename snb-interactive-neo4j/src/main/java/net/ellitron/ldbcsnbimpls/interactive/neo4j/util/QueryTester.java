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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
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
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery1PersonProfileHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery2PersonPostsHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery3PersonFriendsHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery4MessageContentHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery5MessageCreatorHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery6MessageForumHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDb.LdbcShortQuery7MessageRepliesHandler;
import net.ellitron.ldbcsnbimpls.interactive.neo4j.Neo4jDbConnectionState;
import org.docopt.Docopt;

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
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery1 <personId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery2 <personId> <limit>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery3 <personId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery4 <messageId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery5 <messageId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery6 <messageId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] shortquery7 <messageId>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] [--input=<input>] update1 <nth>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] [--input=<input>] update2 <nth>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] [--input=<input>] update3 <nth>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] [--input=<input>] update4 <nth>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] [--input=<input>] update5 <nth>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] [--input=<input>] update6 <nth>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] [--input=<input>] update7 <nth>\n"
      + "  QueryTester [--host=<host>] [--port=<port>] [--input=<input>] update8 <nth>\n"
      + "  QueryTester (-h | --help)\n"
      + "  QueryTester --version\n"
      + "\n"
      + "Options:\n"
      + "  --host=<host>     Host IP address of Neo4j webserver [default: 127.0.0.1].\n"
      + "  --port=<port>     Port of Neo4j webserver [default: 7474].\n"
      + "  --input=<input>   Directory of updateStream files to use as input [default: ./].\n"
      + "  -h --help         Show this screen.\n"
      + "  --version         Show version.\n"
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

  public static void main(String[] args) throws DbException, IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("QueryTester 1.0").parse(args);

    String host;
    if (opts.containsKey("--host")) {
      host = (String) opts.get("--host");
    } else {
      host = "127.0.0.1";
    }

    String port;
    if (opts.containsKey("--port")) {
      port = (String) opts.get("--port");
    } else {
      port = "7474";
    }

    String inputDir;
    if (opts.containsKey("--input")) {
      inputDir = (String) opts.get("--input");
    } else {
      inputDir = "./";
    }

    Neo4jDbConnectionState dbConnectionState =
        new Neo4jDbConnectionState(host, port);

    ResultReporter resultReporter =
        new ResultReporter.SimpleResultReporter(
            new ConcurrentErrorReporter());

    if ((Boolean) opts.get("shortquery1")
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

      System.out.println(op.toString());

      long startTime = 0;
      long endTime = 0;
      switch (opNumber) {
        case 1: {
          startTime = System.nanoTime();
          ((LdbcShortQuery1PersonProfileHandler) opHandler).executeOperation(
              (LdbcShortQuery1PersonProfile) op, dbConnectionState,
              resultReporter);
          endTime = System.nanoTime();
          break;
        }
        case 2: {
          startTime = System.nanoTime();
          ((LdbcShortQuery2PersonPostsHandler) opHandler).executeOperation(
              (LdbcShortQuery2PersonPosts) op, dbConnectionState,
              resultReporter);
          endTime = System.nanoTime();
          break;
        }
        case 3: {
          startTime = System.nanoTime();
          ((LdbcShortQuery3PersonFriendsHandler) opHandler).executeOperation(
              (LdbcShortQuery3PersonFriends) op, dbConnectionState,
              resultReporter);
          endTime = System.nanoTime();
          break;
        }
        case 4: {
          startTime = System.nanoTime();
          ((LdbcShortQuery4MessageContentHandler) opHandler).executeOperation(
              (LdbcShortQuery4MessageContent) op, dbConnectionState,
              resultReporter);
          endTime = System.nanoTime();
          break;
        }
        case 5: {
          startTime = System.nanoTime();
          ((LdbcShortQuery5MessageCreatorHandler) opHandler).executeOperation(
              (LdbcShortQuery5MessageCreator) op, dbConnectionState,
              resultReporter);
          endTime = System.nanoTime();
          break;
        }
        case 6: {
          startTime = System.nanoTime();
          ((LdbcShortQuery6MessageForumHandler) opHandler).executeOperation(
              (LdbcShortQuery6MessageForum) op, dbConnectionState,
              resultReporter);
          endTime = System.nanoTime();
          break;
        }
        case 7: {
          startTime = System.nanoTime();
          ((LdbcShortQuery7MessageRepliesHandler) opHandler).executeOperation(
              (LdbcShortQuery7MessageReplies) op, dbConnectionState,
              resultReporter);
          endTime = System.nanoTime();
          break;
        }
        default:
          throw new RuntimeException("ERROR: Encountered unknown short read "
              + "operation number " + opNumber + "!");
      }

      printResult(resultReporter.result());

      System.out.println(String.format("Query time: %dus",
          (endTime - startTime) / 1000l));

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

            System.out.println(op.toString());

            long startTime, endTime;
            switch (opNumber) {
              case 1: {
                OperationHandler<LdbcUpdate1AddPerson, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate1AddPersonHandler();
                startTime = System.nanoTime();
                opHandler.executeOperation((LdbcUpdate1AddPerson) op,
                    dbConnectionState, resultReporter);
                endTime = System.nanoTime();
                break;
              }
              case 2: {
                OperationHandler<LdbcUpdate2AddPostLike, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate2AddPostLikeHandler();
                startTime = System.nanoTime();
                opHandler.executeOperation((LdbcUpdate2AddPostLike) op,
                    dbConnectionState, resultReporter);
                endTime = System.nanoTime();
                break;
              }
              case 3: {
                OperationHandler<LdbcUpdate3AddCommentLike, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate3AddCommentLikeHandler();
                startTime = System.nanoTime();
                opHandler.executeOperation((LdbcUpdate3AddCommentLike) op,
                    dbConnectionState, resultReporter);
                endTime = System.nanoTime();
                break;
              }
              case 4: {
                OperationHandler<LdbcUpdate4AddForum, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate4AddForumHandler();
                startTime = System.nanoTime();
                opHandler.executeOperation((LdbcUpdate4AddForum) op,
                    dbConnectionState, resultReporter);
                endTime = System.nanoTime();
                break;
              }
              case 5: {
                OperationHandler<LdbcUpdate5AddForumMembership, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate5AddForumMembershipHandler();
                startTime = System.nanoTime();
                opHandler.executeOperation((LdbcUpdate5AddForumMembership) op,
                    dbConnectionState, resultReporter);
                endTime = System.nanoTime();
                break;
              }
              case 6: {
                OperationHandler<LdbcUpdate6AddPost, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate6AddPostHandler();
                startTime = System.nanoTime();
                opHandler.executeOperation((LdbcUpdate6AddPost) op,
                    dbConnectionState, resultReporter);
                endTime = System.nanoTime();
                break;
              }
              case 7: {
                OperationHandler<LdbcUpdate7AddComment, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate7AddCommentHandler();
                startTime = System.nanoTime();
                opHandler.executeOperation((LdbcUpdate7AddComment) op,
                    dbConnectionState, resultReporter);
                endTime = System.nanoTime();
                break;
              }
              case 8: {
                OperationHandler<LdbcUpdate8AddFriendship, Neo4jDbConnectionState> opHandler =
                    new Neo4jDb.LdbcUpdate8AddFriendshipHandler();
                startTime = System.nanoTime();
                opHandler.executeOperation((LdbcUpdate8AddFriendship) op,
                    dbConnectionState, resultReporter);
                endTime = System.nanoTime();
                break;
              }
              default:
                throw new RuntimeException("ERROR: Encountered unknown update "
                    + "operation number " + opNumber + "!");
            }

            System.out.println(String.format("Query time: %dus",
                (endTime - startTime) / 1000l));

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
