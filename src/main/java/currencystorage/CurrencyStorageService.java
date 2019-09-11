package currencystorage;

import java.io.*;

import java.math.BigDecimal;

import java.nio.file.Paths;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import java.time.LocalDateTime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.jetbrains.annotations.NotNull;


public class CurrencyStorageService {
    private Connection connection;

    private static final Logger LOG = LogManager.getLogger(CurrencyStorageService.class);

    private static final String CURRENCY = "currency";
    private static final String SPOT = "spot";
    private static final String DATE = "date";

    private static final String TABLE_NAME = "exchange_rates";
    private static final String CONFIG_FILE_NAME = "mysql.properties";
    private static final String TABLE_INIT_QUERY = "CREATE TABLE IF NOT EXISTS %s" +
        "(currency VARCHAR(3) NOT NULL," +
        "spot DECIMAL(20,5) NOT NULL," +
        "date DATETIME NOT NULL," +
        "PRIMARY KEY (currency, date))";
    private static final String SELECT_LATEST_POINTS = "SELECT currency, spot, date FROM %s WHERE date IN "
        + "(SELECT MAX(date) FROM exchange_rates GROUP BY currency)";
    private static final String RATES_INSERT_QUERY = "INSERT INTO %s(currency,spot,date) VALUES(?,?,?)" +
        "ON DUPLICATE KEY UPDATE spot = VALUES(spot)";
    //private static final
    private static final String SELECT_WITH_CURRENCY = "SELECT date, currency, spot FROM %s WHERE date "
        + "BETWEEN ? AND ? AND currency = ?";
    private static final String SELECT_WITHOUT_CURRENCY = "SELECT date, currency, spot FROM %s WHERE date " +
            "BETWEEN ? AND ?";

    public CurrencyStorageService() throws IOException, SQLException {
        LOG.info("Establishing database connection...");
        this.openConnection();
        LOG.info("Initializing table...");
        this.initTable();
    }

    private void openConnection() throws IOException, SQLException{
        String currentDirectoryPath = Paths.get(System.getProperty("user.dir")).toString();
        try {
            CurrencyStorageClientConfiguration storageClientConfig = new CurrencyStorageClientConfiguration(Paths.get(currentDirectoryPath, CONFIG_FILE_NAME));
            connection = DriverManager.getConnection(storageClientConfig.getJdbcConnectString(),
                storageClientConfig.getUser(), storageClientConfig.getPassword());
        } catch (IOException e) {
            LOG.error("Could not initialize database client configuration: ", e);
            throw e;
        } catch (SQLException e) {
            LOG.error("RDBMS error while opening connection: ", e);
            throw e;
        }
    }

    private void initTable() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            String query = String.format(TABLE_INIT_QUERY, TABLE_NAME);
            int result = statement.executeUpdate(query);
            LOG.debug(String.format("CREATE TABLE: %d", result));
        } catch (SQLException e) {
            LOG.error("RDBMS error while initializing table: ",e);
            throw e;
        }
    }

    public void saveData(@NotNull HashMap<String, BigDecimal> ratesMap) throws SQLException {
         String query;

         try(Statement statement = connection.createStatement()) {
             query = String.format(SELECT_LATEST_POINTS, TABLE_NAME);
             ResultSet latestRecordsInDB = statement.executeQuery(query);
             while (latestRecordsInDB.next()) {
                 filterDuplicateValues(ratesMap, latestRecordsInDB);
             }
         } catch (SQLException e) {
             LOG.error("RDBMS error while retrieving data for comparison: ", e);
             throw e;
         }

         query = String.format(RATES_INSERT_QUERY, TABLE_NAME);
         try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
             Timestamp today = new Timestamp(System.currentTimeMillis());
             for(Map.Entry<String, BigDecimal> entry: ratesMap.entrySet()) {
                 preparedStatement.setString(1, entry.getKey());
                 preparedStatement.setBigDecimal(2, entry.getValue());
                 preparedStatement.setTimestamp(3, today);
                 preparedStatement.addBatch();
             }

             int[] result = preparedStatement.executeBatch();
             LOG.debug(String.format( "INSERT: %s", Arrays.toString(result)));

         } catch (SQLException e) {
             LOG.error("RDBMS error when saving data: ", e);
             throw e;
         }
    }

    public HashMap<String,Map<LocalDateTime,Float>> retrieveData(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        return retrieveData(startDate, endDate, null);
    }

    public HashMap<String,Map<LocalDateTime,Float>> retrieveData(LocalDateTime startDate, LocalDateTime endDate, String currency) throws SQLException {
        if (endDate == null) {
            endDate = LocalDateTime.now();
        }

        ResultSet resultSet;
        String query;

        if (currency == null) {
            query = String.format(SELECT_WITHOUT_CURRENCY, TABLE_NAME);
        } else {
            query = String.format(SELECT_WITH_CURRENCY, TABLE_NAME);
        }

        try(PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setTimestamp(1, Timestamp.valueOf(startDate));
            preparedStatement.setTimestamp(2, Timestamp.valueOf(endDate));
            if (currency != null) {
                preparedStatement.setString(3, currency);
            }

            resultSet = preparedStatement.executeQuery();

            HashMap<String,Map<LocalDateTime,Float>> result = new HashMap<>();

            while (resultSet.next()) {
                fetchRowFromResultSet(resultSet, result);
            }

            return result;
        } catch (SQLException e) {
            LOG.error("RDBMS error while retrieving data: ",e);
            throw e;
        }
    }

    private void fetchRowFromResultSet(@NotNull ResultSet resultSet, @NotNull HashMap<String,Map<LocalDateTime,Float>> result) throws SQLException {
        try {
            if (!result.containsKey(resultSet.getString(CURRENCY))) {
                HashMap<LocalDateTime, Float> timeValue = new HashMap<>();

                timeValue.put(resultSet.getTimestamp(DATE).toLocalDateTime(), resultSet.getFloat(SPOT));
                result.put(resultSet.getString(CURRENCY),timeValue);
            } else {
                result.get(resultSet.getString(CURRENCY)).put(resultSet.getTimestamp(DATE).toLocalDateTime(), resultSet.getFloat(SPOT));
            }
        } catch (SQLException e) {
            LOG.error("Unexpected error while fetching data from the result set: ", e);
            throw e;
        }
    }

    private void filterDuplicateValues(@NotNull HashMap<String, BigDecimal> ratesMap, @NotNull ResultSet latestRecords) throws SQLException {

        try {
            String currency = latestRecords.getString(CURRENCY);
            BigDecimal oldValue = latestRecords.getBigDecimal(SPOT).stripTrailingZeros();
            BigDecimal newValue = ratesMap.get(currency); // potentially null

            LOG.debug(String.format("Old: %s, new: %s", oldValue.toString(), newValue.toString()));

            if (oldValue.equals(newValue)) {
                ratesMap.remove(currency);
            }
        } catch (SQLException e) {
            LOG.error("Unexpected error while fetching data from the result set: ", e);
            throw e;
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        connection.close();
    }
}
