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
    private static final String TABLE_NAME = "exchange_rates";
    private static final String CONFIG_FILE_NAME = "mysql.properties";
    private static final String TABLE_INIT_QUERY = "CREATE TABLE IF NOT EXISTS %s" +
        "(currency VARCHAR(3) NOT NULL," +
        "spot DECIMAL(10,2)," +
        "date DATETIME NOT NULL," +
        "PRIMARY KEY (currency, date))";
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
         String query = String.format(RATES_INSERT_QUERY, TABLE_NAME);

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

    //  TODO: make this readable
    public HashMap<String,Map<LocalDateTime,Float>> retrieveData(LocalDateTime startDate, LocalDateTime endDate, String currency) throws SQLException {
        String query;
        PreparedStatement preparedStatement;
        ResultSet resultSet;

        try {
            if (currency == null) {
                query = String.format(SELECT_WITHOUT_CURRENCY, TABLE_NAME);
                preparedStatement = this.prepareStatementHelper(query, startDate, endDate);
            } else {
                query = String.format(SELECT_WITH_CURRENCY, TABLE_NAME);
                preparedStatement = this.prepareStatementHelper(query, startDate, endDate);
                preparedStatement.setString(3, currency);
            }
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            LOG.error("RDBMS error while retrieving data: ", e);
            throw e;
        }

        HashMap<String,Map<LocalDateTime,Float>> result = new HashMap<>();

        while (resultSet.next()) {
            fillResult(resultSet, result);
        }

        return result;

    }

    private PreparedStatement prepareStatementHelper(String query,
                                        LocalDateTime startDate,
                                        LocalDateTime endDate) throws SQLException {
        if (endDate == null) {
            endDate = LocalDateTime.now();
        }

        PreparedStatement preparedStatement = connection.prepareStatement(query);
        preparedStatement.setTimestamp(1, Timestamp.valueOf(startDate));
        preparedStatement.setTimestamp(2, Timestamp.valueOf(endDate));

        return preparedStatement;
    }

    private void fillResult(ResultSet resultSet, HashMap<String,Map<LocalDateTime,Float>> result) throws SQLException {
        if (!result.containsKey(resultSet.getString("currency"))) {
            HashMap<LocalDateTime, Float> timeValue = new HashMap<>();

            timeValue.put(resultSet.getTimestamp("date").toLocalDateTime(), resultSet.getFloat("spot"));
            result.put(resultSet.getString("currency"),timeValue);
        } else {
            result.get(resultSet.getString("currency")).put(resultSet.getTimestamp("date").toLocalDateTime(), resultSet.getFloat("spot"));
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        connection.close();
    }
}
