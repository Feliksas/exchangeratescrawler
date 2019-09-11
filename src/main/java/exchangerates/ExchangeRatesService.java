package exchangerates;

import java.io.*;

import java.math.BigDecimal;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.jetbrains.annotations.NotNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;


public class ExchangeRatesService {
    private static final String RATES_URL = "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html";
    private static final String TABLE_CSS = "#ecb-content-col > main > div.forextable > table > tbody";
    private static final String CURRENCY_NAME_CLASS = "currency";
    private static final String CURRENCY_VALUE_CLASS =  "spot number";
    private static final String TAG_A = "a";

    private static final Logger LOG = LogManager.getLogger(ExchangeRatesService.class);

    @NotNull
    static HashMap<String, BigDecimal> fetchRates() throws IOException {
        HashMap<String,BigDecimal> result = new HashMap<>();

        LOG.info("Retrieving rates page...");
        Document doc = Jsoup.connect(RATES_URL).get();
        Element ratesTable = doc.select(TABLE_CSS).get(0);

        for (Element currencyRate: ratesTable.children()) {
            String currency = currencyRate.getElementsByClass(CURRENCY_NAME_CLASS).get(0).getElementsByTag(TAG_A).get(0).text();
            BigDecimal rate = new BigDecimal(currencyRate.getElementsByClass(CURRENCY_VALUE_CLASS).get(0).getElementsByTag(TAG_A).get(0).text()).stripTrailingZeros();

            result.put(currency, rate);
        }

        return result;
    }
}
