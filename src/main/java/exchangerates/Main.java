package exchangerates;

import java.io.*;

import java.math.BigDecimal;

import java.sql.SQLException;

import java.time.LocalDate;
import java.time.LocalDateTime;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import currencystorage.CurrencyStorageService;

public class Main {
    private static final Logger LOG = LogManager.getLogger(ExchangeRatesService.class);

    public static void main(String[] args) throws IOException, SQLException {
        LOG.info("Initializing...");

        HashMap<String, BigDecimal> rates = ExchangeRatesService.fetchRates();

        LOG.debug(String.format("Rates: %s", rates.toString()));
        CurrencyStorageService dbHandle = new CurrencyStorageService();
        LOG.info("Saving data to database...");
        dbHandle.saveData(rates);

        // test retrieve
        LOG.info("Getting info from database...");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime today = LocalDate.now().atStartOfDay();
        HashMap<String, Map<LocalDateTime,Float>> dataFromDB = dbHandle.retrieveData(today, now);

        Gson gson = new Gson();
        LOG.info(gson.toJson(dataFromDB));
    }
}
