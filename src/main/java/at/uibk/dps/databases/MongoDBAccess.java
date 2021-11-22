package at.uibk.dps.databases;


import at.uibk.dps.util.Event;
import at.uibk.dps.util.Type;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static com.mongodb.client.model.Filters.*;

/**
 * Class to handle communication with the mongo database.
 */
public class MongoDBAccess {
    private static final long workflowExecutionId = System.currentTimeMillis();
    private static final String PATH_TO_PROPERTIES = "mongoDatabase.properties";
    private static MongoClient mongoClient;
    private static MongoDBAccess mongoDBAccess;
    private static List<Document> entries = Collections.synchronizedList(new ArrayList<>());
    // used to determine when the end date of a log entry was already used as a new start date if loopCounter > maxConcurrency
    private static List<Document> usedForMaxConcurrency = Collections.synchronizedList(new ArrayList<>());
    private static String DATABASE;
    private static String COLLECTION;

    private MongoDBAccess() throws IOException {
        // disable the logging for mongoDB on stdout
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger mongoLogger = loggerContext.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.OFF);

        // read the required properties from file
        Properties databaseFile = new Properties();
        databaseFile.load(new FileInputStream(PATH_TO_PROPERTIES));

        final String host = databaseFile.getProperty("host");
        final int port = Integer.parseInt(databaseFile.getProperty("port"));
        final String username = databaseFile.getProperty("username");
        final String password = databaseFile.getProperty("password");
        DATABASE = databaseFile.getProperty("database");
        COLLECTION = databaseFile.getProperty("collection");

        MongoCredential sim = MongoCredential.createCredential(username, DATABASE, password.toCharArray());
        mongoClient = MongoClients.create
                (MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Arrays.asList(new ServerAddress(host, port))))
                        .credential(sim)
                        .build());
    }

    public static MongoClient getConnection() throws IOException {
        if (mongoClient == null) {
            mongoDBAccess = new MongoDBAccess();
        }
        return mongoClient;
    }

    /**
     * Method to save a workflow-start event and its workflow-content and input.
     */
    public static void saveLogWorkflowStart(Type type, String workflowContent, String workflowInput, long start) {
        saveLog(Event.WORKFLOW_START, null, null, null, null, null, 0L, -1,
                true, -1, -1, start, type, workflowContent, workflowInput);
    }

    /**
     * Method to save a log entry to a list of entries, sets the 'cost' field to -1.
     */
    public static void saveLog(Event event, String functionId, String deployment, String functionName, String functionType, String output,
                               Long RTT, boolean success, int loopCounter, int maxLoopCounter, long startTime, Type type) {
        saveLog(event, functionId, deployment, functionName, functionType, output, RTT, -1, success, loopCounter, maxLoopCounter,
                startTime, type);
    }

    /**
     * Method to save a log entry to a list of entries, sets the workflowContent and workflowInput to null.
     */
    public static void saveLog(Event event, String functionId, String deployment, String functionName, String functionType, String output,
                               Long RTT, double cost, boolean success, int loopCounter, int maxLoopCounter, long startTime, Type type) {
        saveLog(event, functionId, deployment, functionName, functionType, output, RTT, cost, success, loopCounter, maxLoopCounter, startTime, type,
                null, null);
    }

    /**
     * Method to save a log entry to a list of entries.
     */
    public static void saveLog(Event event, String functionId, String deployment, String functionName, String functionType, String output,
                               Long RTT, double cost, boolean success, int loopCounter, int maxLoopCounter, long startTime, Type type,
                               String workflowContent, String workflowInput) {
        Long done = null;
        if (event.toString().equals("FUNCTION_CANCELED")) {
            done = 2L;
        } else if (event.toString().toLowerCase().contains("function")) {
            done = 0L;
        }
        Document log = new Document("workflow_id", workflowExecutionId)
                .append("workflowContent", workflowContent)
                .append("workflowInput", workflowInput)
                .append("function_id", functionId)
                .append("deployment", deployment)
                .append("functionName", functionName)
                .append("functionType", functionType)
                .append("Event", event.toString())
                .append("output", output)
                .append("RTT", RTT)
                .append("cost", cost)
                .append("success", success)
                .append("loopCounter", loopCounter)
                .append("maxLoopCounter", maxLoopCounter)
                .append("startTime", new Date(startTime))
                .append("endTime", new Date(startTime + RTT))
                .append("type", type.toString())
                .append("done", done); // flag used to update metadataDB
        entries.add(log);
    }

    /**
     * Gets the latest end date in the list of log entries regardless of whether the function was executed within a
     * parallelFor loop or not.
     *
     * @return the latest end date of the current workflow execution
     */
    public static long getLastEndDateOverall() {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        // create a temporary list to prevent a ConcurrentModificationException
        ArrayList<Document> tmp = new ArrayList<>(entries);
        return tmp.stream()
                .filter(d -> d.getLong("workflow_id") == workflowExecutionId
                        && d.containsKey("function_id"))
                .max(Comparator.comparing(d -> d.getDate("endTime")))
                .get()
                .getDate("endTime")
                .getTime();
    }

    /**
     * Gets the latest end date in the list of log entries that belongs to a function that was not executed within a
     * parallelFor loop.
     *
     * @return the latest end date of a function outside of a parallelFor of the current workflow execution
     */
    public static long getLastEndDateOutOfLoop() {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        // create a temporary list to prevent a ConcurrentModificationException
        ArrayList<Document> tmp = new ArrayList<>(entries);
        return tmp.stream()
                .filter(d -> d.getLong("workflow_id") == workflowExecutionId
                        && d.getInteger("loopCounter") == -1
                        && d.containsKey("function_id"))
                .max(Comparator.comparing(d -> d.getDate("endTime")))
                .get()
                .getDate("endTime")
                .getTime();

//        long endParallelFor = tmp.stream()
//                .filter(d -> d.getLong("workflow_id") == workflowExecutionId
//                        && d.getString("Event").equals("PARALLEL_FOR_END"))
//                .max(Comparator.comparing(d -> d.getDate("endTime")))
//                .orElse(new Document("endTime", new Date(0)))
//                .getDate("endTime")
//                .getTime();
//
//        return Math.max(outOfLoop, endParallelFor);
    }

    /**
     * Gets the latest end date in the list of log entries that belongs to a function that was executed within a
     * parallelFor loop.
     *
     * @return the latest end date of a function inside of a parallelFor of the current workflow execution
     */
    public static long getLastEndDateInLoop() {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        // create a temporary list to prevent a ConcurrentModificationException
        ArrayList<Document> tmp = new ArrayList<>(entries);
        return tmp.stream()
                .filter(d -> d.getLong("workflow_id") == workflowExecutionId
                        && d.getInteger("loopCounter") != -1
                        && d.containsKey("function_id"))
                .max(Comparator.comparing(d -> d.getDate("endTime")))
                .get()
                .getDate("endTime")
                .getTime();
    }

    /**
     * Checks in the list of logs for the smallest end time of the current parallelFor construct. Is used to determine
     * the next starting time for a function within a parallelFor that is over the maximum concurrency limit.
     *
     * @param functionId to check the log entries for
     *
     * @return the earliest finishing time in a parallelFor that has not been used yet for this function
     */
    public static synchronized long getFirstAvailableStartTime(String functionId) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        boolean flag = true;
        // create a temporary list to prevent a ConcurrentModificationException
        ArrayList<Document> tmp = null;
        ArrayList<Document> loopLogs = null;
        Optional<Document> doc = Optional.empty();
        // check for entry until one is found
        while (flag) {
            tmp = new ArrayList<>(entries);
            loopLogs = new ArrayList<>();
            Collections.reverse(tmp);
            /* only get the documents of the current parallelFor
             * if there was a parallelFor previously, the PARALLEL_FOR_END event signals the end of that parallelFor */
            for (Document d : tmp) {
                if (d.getString("Event").equals("PARALLEL_FOR_END")) {
                    break;
                }
                if (d.containsKey("function_id") && functionId.equals(d.getString("function_id")) &&
                        d.getInteger("loopCounter") != -1 && d.getLong("workflow_id") == workflowExecutionId &&
                        !usedForMaxConcurrency.contains(d)) {
                    loopLogs.add(d);
                }
            }
            // get the entry with the smallest end time
            doc = loopLogs.stream().min(Comparator.comparing(d -> d.getDate("endTime")));
            flag = !doc.isPresent();
        }
        usedForMaxConcurrency.add(doc.get());
        return doc.get().getDate("endTime").getTime();
    }

    /**
     * Returns all entries from the logs that were executions, have a function_id field and have not been updated in the
     * metadata DB already.
     *
     * @return a FindIterable containing documents
     */
    public static FindIterable<Document> findNewEntries() {
        try {
            MongoClient client = getConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        MongoDatabase mongoDatabase = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> dbCollection = mongoDatabase.getCollection(COLLECTION);
        return dbCollection.find(and(eq("done", 0L), not(eq("function_id", null)),
                eq("type", "EXEC")));
    }

    /**
     * Adds all documents stored in the list of entries to the mongo database.
     */
    public static void addAllEntries() throws IOException {
        MongoClient client = getConnection();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> dbCollection = mongoDatabase.getCollection(COLLECTION);
        if (!entries.isEmpty()) {
            dbCollection.insertMany(entries);
        }
    }

    /**
     * Get the list of all log entries.
     *
     * @return a list of documents containing all logs.
     */
    public static List<Document> getAllEntries() {
        return entries;
    }

    /**
     * Sets the 'done' field in the mongoDB document to the given value.
     *
     * @param document to set the field
     * @param value    the value to set the field to, 1 means "done", 2 means "ignored"
     */
    public static void setAsDone(Document document, Long value) {
        try {
            MongoClient client = getConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        MongoDatabase mongoDatabase = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> dbCollection = mongoDatabase.getCollection(COLLECTION);
        ObjectId id = (ObjectId) document.get("_id");
        dbCollection.updateOne(eq("_id", id), Updates.set("done", value));
    }


    /**
     * Closes the mongoDB connection.
     */
    public static void close() {
        mongoClient.close();
    }

}
