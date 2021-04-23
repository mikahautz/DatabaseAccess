package at.uibk.dps.cronjob;

import at.uibk.dps.mongoLogger.MongoDBAccess;
import at.uibk.dps.util.Provider;
import at.uibk.dps.util.Utils;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.function.Consumer;

public class MariaDBAccess {

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String PATH_TO_PROPERTIES = "mariaDatabase.properties";
    private static MariaDBAccess mariaDBAccess;
    private static Connection mariaConnection = null;
    public static Consumer<Document> updateMD = new Consumer<Document>() {
        @Override
        public void accept(final Document document) {
            if (document.getString("function_id") != null) {
                updateFunctionType(document);
                updateFunctionImplementation(document);
                updateFunctionDeployment(document);
                MongoDBAccess.setAsDone(document);
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

    public static void doSth() {
        Connection connection = getConnection();
        Statement statement = null;

        MongoDBAccess.findNewEntries().forEach(updateMD);

        try {
            statement = connection.createStatement();
//            String query = "SELECT * FROM ?";
//            PreparedStatement preparedStatement = connection.prepareStatement(query);
//            preparedStatement.setString(1, "fcdeployment");
//            ResultSet resultSet = preparedStatement.executeQuery();
            String query = "SELECT * FROM fcdeployment";
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                int FCteam = resultSet.getInt("FCteam");
                int fd = resultSet.getInt("fd");
                String description = resultSet.getString("description");
//                Date dateCreated = rs.getDate("date_created");
//                boolean isAdmin = rs.getBoolean("is_admin");

                // print the results
                System.out.format("%s, %s, %s, %s\n", id, FCteam, fd, description);
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static int getProviderId(Provider provider) {
        Connection connection = getConnection();
        PreparedStatement preparedStatement;
        String query = "SELECT id FROM provider WHERE name = ?";
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, provider.name());
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("id");
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return -1;
    }

    private static int getFunctionTypeId(String functionName, String functionType) {
        Connection connection = getConnection();
        PreparedStatement preparedStatement;
        String query = "SELECT id FROM functiontype WHERE name = ? AND type = ?";
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, functionName);
            preparedStatement.setString(2, functionType);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("id");
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return -1;
    }

    private static int getFunctionImplementationId(String functionImplementationName) {
        Connection connection = getConnection();
        PreparedStatement preparedStatement;
        String query = "SELECT id FROM functionimplementation WHERE name = ?";
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, functionImplementationName);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("id");
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return -1;
    }

    public static void updateFunctionType(Document document) {
//        if (document.getString("functionName") == null ||
//                document.getString("functionType") == null) {
//            return;
//        }
        // TODO update cost
        Connection connection = getConnection();
        String functionName = document.getString("functionName");
        String functionType = document.getString("functionType");
        Long RTT = document.getLong("RTT");
        boolean success = document.getBoolean("success");
        String query = "SELECT * FROM functiontype WHERE name = ? AND type = ?";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, functionName);
            preparedStatement.setString(2, functionType);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                // update
                int invocations = resultSet.getInt("invocations");
                double avgRTT = resultSet.getDouble("avgRTT");
                double successRate = resultSet.getDouble("successRate");
                int successfulInvocations = (int) Math.round(successRate * invocations);

                double newAvgRTT = ((avgRTT * invocations) + RTT) / (invocations + 1);
                if (success) {
                    successfulInvocations++;
                }
                double newSuccessRate = (double) successfulInvocations / (double) (invocations + 1);
                // TODO update cost
                String update = "UPDATE functiontype SET avgRTT = ?, successRate = ?, invocations = ? WHERE "
                        + "name = ? AND type = ?";
                preparedStatement = connection.prepareStatement(update);
                preparedStatement.setDouble(1, newAvgRTT);
                preparedStatement.setDouble(2, newSuccessRate);
                preparedStatement.setInt(3, (invocations + 1));
                preparedStatement.setString(4, functionName);
                preparedStatement.setString(5, functionType);
                preparedStatement.executeUpdate();
            } else {
                // insert new entry
                String insert = "INSERT INTO functiontype (name, type, avgRTT, avgCost, successRate, invocations) "
                        + "values (?, ?, ?, ?, ?, ?)";
                preparedStatement = connection.prepareStatement(insert);
                preparedStatement.setString(1, functionName);
                preparedStatement.setString(2, functionType);
                preparedStatement.setDouble(3, RTT);
                preparedStatement.setDouble(4, 0);   // TODO
                preparedStatement.setDouble(5, (success) ? 1 : 0);
                preparedStatement.setInt(6, 1);
                preparedStatement.execute();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void updateFunctionImplementation(Document document) {
//        if (document.getString("functionName") == null ||
//                document.getString("functionType") == null) {
//            return;
//        }
        // TODO algorithm, implementationFilePath, language_id, computationWork, memoryWork, ioWOrk
        // TODO which value has 'name'?
        // TODO update cost
        Connection connection = getConnection();
        String functionName = document.getString("functionName");
        String functionType = document.getString("functionType");
        Long RTT = document.getLong("RTT");
        Boolean success = document.getBoolean("success");
        Provider provider = Utils.detectProvider(document.getString("function_id"));
        int providerId = getProviderId(provider);
        int functionTypeId = getFunctionTypeId(functionName, functionType);
        String name = provider.name() + "." + functionName + "." + functionType;

        String query = "SELECT * FROM functionimplementation WHERE name = ? AND functionType_id = ? AND provider = ?";

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, name);
            preparedStatement.setInt(2, functionTypeId);
            preparedStatement.setInt(3, providerId);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                // update
                int invocations = resultSet.getInt("invocations");
                double avgRTT = resultSet.getDouble("avgRTT");
                double successRate = resultSet.getDouble("successRate");
                int successfulInvocations = (int) Math.round(successRate * invocations);

                double newAvgRTT = ((avgRTT * invocations) + RTT) / (invocations + 1);
                if (success) {
                    successfulInvocations++;
                }
                double newSuccessRate = (double) successfulInvocations / (double) (invocations + 1);
                // TODO update cost
                String update = "UPDATE functionimplementation SET avgRTT = ?, successRate = ?, invocations = ? WHERE "
                        + "functionType_id = ? AND provider = ?";
                preparedStatement = connection.prepareStatement(update);
                preparedStatement.setDouble(1, newAvgRTT);
                preparedStatement.setDouble(2, newSuccessRate);
                preparedStatement.setInt(3, (invocations + 1));
                preparedStatement.setInt(4, functionTypeId);
                preparedStatement.setInt(5, providerId);
                preparedStatement.executeUpdate();
            } else {
                // insert new entry
                String insert = "INSERT INTO functionimplementation (name, functionType_id, provider, avgRTT, avgCost, successRate, invocations) "
                        + "values (?, ?, ?, ?, ?, ?, ?)";
                preparedStatement = connection.prepareStatement(insert);
                preparedStatement.setString(1, name);
                preparedStatement.setInt(2, functionTypeId);
                preparedStatement.setInt(3, providerId);
                preparedStatement.setDouble(4, RTT);
                preparedStatement.setDouble(5, 0);   // TODO
                preparedStatement.setDouble(6, (success) ? 1 : 0);
                preparedStatement.setInt(7, 1);
                preparedStatement.execute();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void updateFunctionDeployment(Document document) {
        // TODO get functionImplementationId from DB
        // TODO get execution amount of function deployment
        // TODO either insert or update

//        if (document.getString("functionName") == null ||
//                document.getString("functionType") == null) {
//            return;
//        }
        // TODO description, handlerName, input, isDeployed, timeout, computationalSpeed, memorySpeed, ioSpeed
        // TODO which value has 'name'?
        // TODO update cost
        Connection connection = getConnection();
        String functionName = document.getString("functionName");
        String functionType = document.getString("functionType");
        String function_id = document.getString("function_id");
        Integer memorySize = document.getInteger("memorySize");
        Long RTT = document.getLong("RTT");
        Boolean success = document.getBoolean("success");
        Provider provider = Utils.detectProvider(function_id);
        String functionImplementationName = provider.name() + "." + functionName + "." + functionType;
        int functionImplementationId = getFunctionImplementationId(functionImplementationName);
        String name = functionName + "." + functionType + "." + function_id;

        // TODO add memorySize
        String query = "SELECT * FROM functiondeployment WHERE functionImplementation_id = ? AND KMS_Arn = ?";

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1, functionImplementationId);
            preparedStatement.setString(2, function_id);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                // update
                int invocations = resultSet.getInt("invocations");
                double avgRTT = resultSet.getDouble("avgRTT");
                double successRate = resultSet.getDouble("successRate");
                int successfulInvocations = (int) Math.round(successRate * invocations);

                double newAvgRTT = ((avgRTT * invocations) + RTT) / (invocations + 1);
                if (success) {
                    successfulInvocations++;
                }
                double newSuccessRate = (double) successfulInvocations / (double) (invocations + 1);
                // TODO update cost
                String update = "UPDATE functiondeployment SET avgRTT = ?, successRate = ?, invocations = ? WHERE "
                        + "functionImplementation_id = ? AND KMS_Arn = ?";
                preparedStatement = connection.prepareStatement(update);
                preparedStatement.setDouble(1, newAvgRTT);
                preparedStatement.setDouble(2, newSuccessRate);
                preparedStatement.setInt(3, (invocations + 1));
                preparedStatement.setInt(4, functionImplementationId);
                preparedStatement.setString(5, function_id);
                preparedStatement.executeUpdate();
            } else {
                // insert new entry
                // TODO change:
                if (memorySize == null) {
                    memorySize = -1;
                }
                String insert = "INSERT INTO functiondeployment (name, functionImplementation_id, KMS_Arn, description, "
                        + "handlerName, isDeployed, memorySize, timeout, avgRTT, avgCost, successRate, invocations) "
                        + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                preparedStatement = connection.prepareStatement(insert);
                preparedStatement.setString(1, name);
                preparedStatement.setInt(2, functionImplementationId);
                preparedStatement.setString(3, function_id);
                preparedStatement.setString(4, "");
                preparedStatement.setString(5, "");
                preparedStatement.setBoolean(6, true);
                preparedStatement.setInt(7, memorySize);
                preparedStatement.setInt(8, 0);
                preparedStatement.setDouble(9, RTT);
                preparedStatement.setDouble(10, 0);   // TODO
                preparedStatement.setDouble(11, (success) ? 1 : 0);
                preparedStatement.setInt(12, 1);
                preparedStatement.execute();
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

}
