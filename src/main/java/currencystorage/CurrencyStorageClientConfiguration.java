package currencystorage;

import java.io.*;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

class CurrencyStorageClientConfiguration {
    private static final Logger LOG = LogManager.getLogger(CurrencyStorageClientConfiguration.class);

    private static final String JDBC_STRING = "jdbcConnectString";
    private static final String USER = "user";
    private static final String PASS = "pass";

    private String jdbcConnectString;
    private String user;
    private String password;


    CurrencyStorageClientConfiguration(@NotNull Path configFilePath) throws IOException {
        Properties config = new Properties();
        try (FileInputStream configFile = new FileInputStream(configFilePath.toString())) {
            config.load(configFile);
            jdbcConnectString = config.getProperty(JDBC_STRING);
            user = config.getProperty(USER);
            password = config.getProperty(PASS);

        } catch (IOException e) {
            LOG.error(String.format("Unable to read MySQL client config file at %s:", configFilePath.toString()), e);
            throw e;
        }
    }

    String getJdbcConnectString() {
        return this.jdbcConnectString;
    }

    String getUser() {
        return this.user;
    }

    String getPassword() {
        return this.password;
    }
}
