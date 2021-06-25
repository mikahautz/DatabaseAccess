package at.uibk.dps.databases;

import at.uibk.dps.util.Provider;
import at.uibk.dps.util.Utils;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Class to handle communication with the mongo database.
 */
public class MariaDBAccess {
    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String PATH_TO_PROPERTIES = "mariaDatabase.properties";
    private static MariaDBAccess mariaDBAccess;
    private static Connection mariaConnection = null;
    /**
     * Counts the amount of skipped logs while updating.
     */
    private static long skipped = 0;
    /**
     * Counts the amount of updated logs while updating.
     */
    private static long updated = 0;

    /**
     * Updates the metadata DB with the given document and sets its 'done'-field to true.
     */
    public static Consumer<Document> updateMD = new Consumer<Document>() {
        @Override
        public void accept(final Document document) {
            if (document.getString("function_id") != null) {
                if (functionIdEntryExists(document)) {
                    System.out.println("Updating entries for function with id '" + document.getString("function_id") + "'.");
                    updateMetadata(document);
                    updated++;
                    // set the log entry as done
                    MongoDBAccess.setAsDone(document, 1L);
                } else {
                    System.out.println("No entry for function with id '" + document.getString("function_id") +
                            "' found. Skipped.");
                    // set the log entry as ignored
                    MongoDBAccess.setAsDone(document, 2L);
                    skipped++;
                }
            }
        }
    };

    private MariaDBAccess() {
        try {
            Class.forName(JDBC_DRIVER);
            Properties databaseFile = new Properties();
            databaseFile.load(new FileInputStream(PATH_TO_PROPERTIES));

            final String host = databaseFile.getProperty("host");
            final int port = Integer.parseInt(databaseFile.getProperty("port"));
            final String username = databaseFile.getProperty("username");
            final String password = databaseFile.getProperty("password");
            final String database = databaseFile.getProperty("database");
            final String db_url = "jdbc:mariadb://" + host + ":" + port + "/" + database;

            mariaConnection = DriverManager.getConnection(db_url, username, password);
        } catch (ClassNotFoundException | SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        if (mariaConnection == null) {
            mariaDBAccess = new MariaDBAccess();
        }
        return mariaConnection;
    }

    /**
     * Checks if an entry with the function id (e.g. ARN) of the document exists in the functiondeployment table of the
     * metadata DB.
     *
     * @param document to get the function id
     *
     * @return true if it exists, false otherwise
     */
    private static boolean functionIdEntryExists(Document document) {
        try {
            return getFunctionIdEntry(document.getString("function_id")).next();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return false;
    }

    /**
     * Get the entry with the function id (e.g. ARN) of the document in the functiondeployment table of the metadata
     * DB.
     *
     * @param functionId to get the entry
     *
     * @return the ResultSet
     */
    public static ResultSet getFunctionIdEntry(String functionId) {
        Connection connection = getConnection();
        String query = "SELECT * FROM functiondeployment WHERE KMS_Arn = ?";

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, functionId);
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return resultSet;
    }

    /**
     * Gets the id from the metadata DB for the functiontype with the given name and type.
     *
     * @param functionImplementationId to get the entry
     *
     * @return the id from the functiontype in the DB
     */
    private static int getFunctionTypeId(int functionImplementationId) {
        Connection connection = getConnection();
        PreparedStatement preparedStatement;
        String query = "SELECT functionType_id FROM functionimplementation WHERE id = ?";
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1, functionImplementationId);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("functionType_id");
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return -1;
    }

    /**
     * Gets the entry from the metadata DB for the given provider.
     *
     * @param provider to get the entry from
     *
     * @return the entry from the provider in the DB
     */
    public static ResultSet getProviderEntry(Provider provider) {
        Connection connection = getConnection();
        PreparedStatement preparedStatement;
        String query = "SELECT * FROM provider WHERE name = ?";
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, provider.name());
            return preparedStatement.executeQuery();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    /**
     * Gets the entry from the metadata DB for the given region and provider.
     *
     * @param region   to get the entry from
     * @param provider to get the entry from
     *
     * @return the entry from the region in the DB
     */
    public static ResultSet getRegionEntry(String region, Provider provider) {
        Connection connection = getConnection();
        PreparedStatement preparedStatement;
        String query = "SELECT * FROM region WHERE region = ? AND provider = ?";
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, region);
            preparedStatement.setString(2, provider.name());
            return preparedStatement.executeQuery();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }


    private static double calculateCost(int memorySize, double RTT, Provider provider) {
        // TODO instead of RTT get function runtime
        ResultSet resultSet = getProviderEntry(provider);
        double result = -1;
        if (resultSet != null) {
            try {
                resultSet.next();
                double invocationCost = resultSet.getDouble("invocationCost");
                double durationGBpsCost = resultSet.getDouble("durationGBpsCost");

                // fixed invocationCost + allocated memory size in GB * function runtime in sec * GBps cost
                result = invocationCost + (((memorySize / 1000.0) * (RTT / 1000)) * durationGBpsCost);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        return result;
    }

    private static int getFunctionMemory(Document document) {
        return getFieldFromOutput(document, "functionMemory");
    }

    private static int getRuntime(Document document) {
        // TODO calculate if not present in output
        return getFieldFromOutput(document, "runtime");
    }

    /**
     * Checks the output field of the document for a given key.
     *
     * @param document to check the field
     * @param key      to check
     *
     * @return the element for the given key as int, if it doesn't exist -1
     */
    private static int getFieldFromOutput(Document document, String key) {
        String output = document.getString("output");
        JsonObject json = (JsonObject) JsonParser.parseString(output);
        JsonElement element = json.get(key);
        if (element == null) {
            return -1;
        } else {
            return element.getAsInt();
        }
    }

    /**
     * Updates the functiontype table in the metadataDB for the given document.
     *
     * @param document       to get the values
     * @param functionTypeId to get the entry
     */
    private static void updateFunctionType(Document document, int functionTypeId, double cost) {
        // TODO update cost
        Connection connection = getConnection();

        // get the fields from the document
        Long RTT = document.getLong("RTT");
        boolean success = document.getBoolean("success");

        // query to check if there is already an entry in the functiontype table
        String query = "SELECT * FROM functiontype WHERE id = ?";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1, functionTypeId);
            resultSet = preparedStatement.executeQuery();
            resultSet.next();

            // get the fields from the entry
            int invocations = resultSet.getInt("invocations");
            double avgRTT = resultSet.getDouble("avgRTT");
            double avgCost = resultSet.getDouble("avgCost");
            double successRate = resultSet.getDouble("successRate");
            int successfulInvocations = (int) Math.round(successRate * invocations);

            // if the cost is -1 or 0, set it to the average cost to prevent wrong values
            if (cost == -1 || cost == 0) {
                cost = avgCost;
            }

            // update the fields
            double newAvgRTT = ((avgRTT * invocations) + RTT) / (invocations + 1);
            double newCost = ((avgCost * invocations) + cost) / (invocations + 1);
            if (success) {
                successfulInvocations++;
            }
            double newSuccessRate = (double) successfulInvocations / (double) (invocations + 1);

            // update the functiontype table
            String update = "UPDATE functiontype SET avgRTT = ?, avgCost = ?, successRate = ?, invocations = ? WHERE "
                    + "id = ?";
            preparedStatement = connection.prepareStatement(update);
            preparedStatement.setDouble(1, newAvgRTT);
            preparedStatement.setDouble(2, newCost);
            preparedStatement.setDouble(3, newSuccessRate);
            preparedStatement.setInt(4, (invocations + 1));
            preparedStatement.setInt(5, functionTypeId);
            preparedStatement.executeUpdate();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /**
     * Updates the functionimplementation table in the metadataDB for the given document.
     *
     * @param document                 to get the values
     * @param functionImplementationId to get the entry
     */
    private static void updateFunctionImplementation(Document document, int functionImplementationId, double cost) {
        Connection connection = getConnection();
        // get the fields from the document
        Long RTT = document.getLong("RTT");
        Boolean success = document.getBoolean("success");

        // query to check if there is already an entry in the functionimplementation table
        String query = "SELECT * FROM functionimplementation WHERE id = ?";

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1, functionImplementationId);
            resultSet = preparedStatement.executeQuery();
            // get the first entry
            resultSet.next();

            // get the fields from the entry
            int invocations = resultSet.getInt("invocations");
            double avgRTT = resultSet.getDouble("avgRTT");
            double avgCost = resultSet.getDouble("avgCost");
            double successRate = resultSet.getDouble("successRate");
            int successfulInvocations = (int) Math.round(successRate * invocations);

            // if the cost is -1 or 0, set it to the average cost to prevent wrong values
            if (cost == -1 || cost == 0) {
                cost = avgCost;
            }

            // update the fields
            double newAvgRTT = ((avgRTT * invocations) + RTT) / (invocations + 1);
            double newCost = ((avgCost * invocations) + cost) / (invocations + 1);
            if (success) {
                successfulInvocations++;
            }
            double newSuccessRate = (double) successfulInvocations / (double) (invocations + 1);

            // TODO update cost
            // update the functionimplementation table
            String updateFunctionImplementation = "UPDATE functionimplementation SET avgRTT = ?, avgCost = ?, successRate = ?, invocations = ? WHERE "
                    + "id = ?";
            preparedStatement = connection.prepareStatement(updateFunctionImplementation);
            preparedStatement.setDouble(1, newAvgRTT);
            preparedStatement.setDouble(2, newCost);
            preparedStatement.setDouble(3, newSuccessRate);
            preparedStatement.setInt(4, (invocations + 1));
            preparedStatement.setInt(5, functionImplementationId);
            preparedStatement.executeUpdate();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /**
     * Updates the functiondeployment table in the metadataDB for the given document.
     *
     * @param document to get the values
     * @param entry    the entry to update
     */
    private static void updateFunctionDeployment(Document document, ResultSet entry, double cost) {
        Connection connection = getConnection();
        PreparedStatement preparedStatement = null;

        String function_id = document.getString("function_id");
        Long RTT = document.getLong("RTT");
        Boolean success = document.getBoolean("success");
        Integer maxLoopCounter = document.getInteger("maxLoopCounter");

        try {
            // get the required fields
            int invocations = entry.getInt("invocations");
            double avgRTT = entry.getDouble("avgRTT");
            double avgCost = entry.getDouble("avgCost");
            double successRate = entry.getDouble("successRate");
            int avgLoopCounter = entry.getInt("avgLoopCounter");
            int successfulInvocations = (int) Math.round(successRate * invocations);

            // if the cost is -1 or 0, set it to the average cost to prevent wrong values
            if (cost == -1 || cost == 0) {
                cost = avgCost;
            }

            // update the fields
            double newAvgRTT = ((avgRTT * invocations) + RTT) / (invocations + 1);
            double newCost = ((avgCost * invocations) + cost) / (invocations + 1);
            if (success) {
                successfulInvocations++;
            }
            double newSuccessRate = (double) successfulInvocations / (double) (invocations + 1);
            // if the function was not executed in a loop
            if (maxLoopCounter == null || maxLoopCounter == -1) {
                maxLoopCounter = 0;
            }
            // TODO change from ceil to round?
            int newAvgLoopCounter = (int) Math.ceil(((avgLoopCounter * invocations) + maxLoopCounter) / (double) (invocations + 1));

            // update the functiondeployment table
            String updateFunctionDeployment = "UPDATE functiondeployment SET avgRTT = ?, avgCost = ?, successRate = ?, "
                    + "avgLoopCounter = ?, invocations = ? WHERE KMS_Arn = ?";
            preparedStatement = connection.prepareStatement(updateFunctionDeployment);
            preparedStatement.setDouble(1, newAvgRTT);
            preparedStatement.setDouble(2, newCost);
            preparedStatement.setDouble(3, newSuccessRate);
            preparedStatement.setInt(4, newAvgLoopCounter);
            preparedStatement.setInt(5, (invocations + 1));
            preparedStatement.setString(6, function_id);
            preparedStatement.executeUpdate();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    /**
     * Update the metadata DB for the given document.
     *
     * @param document to update
     */
    private static void updateMetadata(Document document) {
        // TODO update cost
        //  TODO check if computationalSpeed, memorySpeed, ioSpeed is entered, if not insert
        Connection connection = getConnection();
        PreparedStatement preparedStatement = null;
        String functionId = document.getString("function_id");
        Long RTT = document.getLong("RTT");
        Provider provider = Utils.detectProvider(functionId);
        double cost = -1;

        // get the functiondeployment table entry
        ResultSet entry = getFunctionIdEntry(functionId);

        try {
            // get the first entry
            entry.next();
            // get the required fields
            int memorySize = entry.getInt("memorySize");
            int functionImplementationId = entry.getInt("functionImplementation_id");
            int functionTypeId = getFunctionTypeId(functionImplementationId);

            // TODO all providers or only AWS?
            if (memorySize != -1 && provider != Provider.FAIL) {
                cost = calculateCost(memorySize, RTT, provider);
            }

            updateFunctionDeployment(document, entry, cost);
            updateFunctionImplementation(document, functionImplementationId, cost);
            updateFunctionType(document, functionTypeId, cost);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static long getUpdated() {
        return updated;
    }

    public static long getSkipped() {
        return skipped;
    }

    public static void resetCounters() {
        updated = 0;
        skipped = 0;
    }
}
