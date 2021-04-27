package at.uibk.dps.databases;

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
    private static ResultSet getFunctionIdEntry(String functionId) {
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
     * Updates the functiontype table in the metadataDB for the given document.
     *
     * @param document       to get the values
     * @param functionTypeId to get the entry
     */
    private static void updateFunctionType(Document document, int functionTypeId) {
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
            double successRate = resultSet.getDouble("successRate");
            int successfulInvocations = (int) Math.round(successRate * invocations);

            // update the fields
            double newAvgRTT = ((avgRTT * invocations) + RTT) / (invocations + 1);
            if (success) {
                successfulInvocations++;
            }
            double newSuccessRate = (double) successfulInvocations / (double) (invocations + 1);

            // TODO update cost
            // update the functiontype table
            String update = "UPDATE functiontype SET avgRTT = ?, successRate = ?, invocations = ? WHERE "
                    + "id = ?";
            preparedStatement = connection.prepareStatement(update);
            preparedStatement.setDouble(1, newAvgRTT);
            preparedStatement.setDouble(2, newSuccessRate);
            preparedStatement.setInt(3, (invocations + 1));
            preparedStatement.setInt(4, functionTypeId);
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
    private static void updateFunctionImplementation(Document document, int functionImplementationId) {
        // TODO update cost
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
            double successRate = resultSet.getDouble("successRate");
            int successfulInvocations = (int) Math.round(successRate * invocations);

            // update the fields
            double newAvgRTT = ((avgRTT * invocations) + RTT) / (invocations + 1);
            if (success) {
                successfulInvocations++;
            }
            double newSuccessRate = (double) successfulInvocations / (double) (invocations + 1);

            // TODO update cost
            // update the functionimplementation table
            String updateFunctionImplementation = "UPDATE functionimplementation SET avgRTT = ?, successRate = ?, invocations = ? WHERE "
                    + "id = ?";
            preparedStatement = connection.prepareStatement(updateFunctionImplementation);
            preparedStatement.setDouble(1, newAvgRTT);
            preparedStatement.setDouble(2, newSuccessRate);
            preparedStatement.setInt(3, (invocations + 1));
            preparedStatement.setInt(4, functionImplementationId);
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
    private static void updateFunctionDeployment(Document document, ResultSet entry) {
        Connection connection = getConnection();
        PreparedStatement preparedStatement = null;

        String function_id = document.getString("function_id");
        Long RTT = document.getLong("RTT");
        Boolean success = document.getBoolean("success");

        try {
            // get the required fields
            int invocations = entry.getInt("invocations");
            double avgRTT = entry.getDouble("avgRTT");
            double successRate = entry.getDouble("successRate");
            int successfulInvocations = (int) Math.round(successRate * invocations);

            // update the fields // TODO update cost
            double newAvgRTT = ((avgRTT * invocations) + RTT) / (invocations + 1);
            if (success) {
                successfulInvocations++;
            }
            double newSuccessRate = (double) successfulInvocations / (double) (invocations + 1);

            // update the functiondeployment table
            String updateFunctionDeployment = "UPDATE functiondeployment SET avgRTT = ?, successRate = ?, invocations = ? WHERE "
                    + "KMS_Arn = ?";
            preparedStatement = connection.prepareStatement(updateFunctionDeployment);
            preparedStatement.setDouble(1, newAvgRTT);
            preparedStatement.setDouble(2, newSuccessRate);
            preparedStatement.setInt(3, (invocations + 1));
            preparedStatement.setString(4, function_id);
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

        // get the functiondeployment table entry
        ResultSet entry = getFunctionIdEntry(functionId);

        try {
            // get the first entry
            entry.next();
            // get the required fields
            int functionImplementationId = entry.getInt("functionImplementation_id");
            int functionTypeId = getFunctionTypeId(functionImplementationId);

            updateFunctionDeployment(document, entry);
            updateFunctionImplementation(document, functionImplementationId);
            updateFunctionType(document, functionTypeId);

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
