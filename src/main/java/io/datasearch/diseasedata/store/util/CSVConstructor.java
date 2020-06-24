package io.datasearch.diseasedata.store.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Read and Constructs CSV files.
 */
public class CSVConstructor {

    public static final CSVConstructor INSTANCE = new CSVConstructor();
    private static final Logger logger = LoggerFactory.getLogger(CSVConstructor.class);

    private CSVConstructor() {
    }

    public void readCSV() {
        Map<String, Map<String, String>> stationID = new HashMap<>();
        URL stationList = getClass().getClassLoader().getResource("weather-rainfall/stations.csv");
        URL station;
        try {
            CSVParser parser1 = CSVParser.parse(stationList, StandardCharsets.UTF_8, CSVFormat.EXCEL);
            for (CSVRecord record : parser1) {
                Map<String, String> details = new HashMap<>();
                details.put("Station_ID", record.get(2));
                details.put("Station_Name", record.get(1));
                details.put("Latitude", record.get(3));
                details.put("Longitude", record.get(4));
                stationID.put(record.get(2), details);
            }
            logger.info("Entering");
            Writer output = createOutput();
            for (Map.Entry entry : stationID.entrySet()) {
                int index = 0;
                station = getClass().getClassLoader().getResource("weather-rainfall/".concat((String) entry.getKey())
                        .concat(".csv"));
                Map<String, String> value = (HashMap) entry.getValue();
                CSVParser parser2 = CSVParser.parse(station, StandardCharsets.UTF_8, CSVFormat.EXCEL);
                for (CSVRecord record : parser2) {
                    if (index != 0) {
                        output.append((String) entry.getKey());
                        output.append(",");
                        output.append(value.get("Station_Name"));
                        output.append(",");
                        output.append(value.get("Latitude"));
                        output.append(",");
                        output.append(value.get("Longitude"));
                        String date = createDate(record.get(4), record.get(5), record.get(6));
                        output.append(",");
                        output.append(date);
                        output.append(",");
                        output.append(record.get(7));
                        output.append("\n");
                    } else {
                        index = 1;
                    }
                }
            }
            output.flush();
            output.close();
            logger.info("Exiting");
        } catch (IOException e) {
            throw new RuntimeException("Error reading weather-data:", e);
        }
    }

    public Writer createOutput() {
        try {
            //URL input = getClass().getClassLoader().getResource("weather-precipitation-data.csv");
            FileWriter csvWriter = new FileWriter("weather-precipitation-data.csv");
            csvWriter.append("StationID");
            csvWriter.append(",");
            csvWriter.append("StationName");
            csvWriter.append(",");
            csvWriter.append("Date");
            csvWriter.append(",");
            csvWriter.append("Observed_val");
            csvWriter.append("\n");
            return csvWriter;
        } catch (IOException e) {
            logger.info(e.getMessage());
            throw new RuntimeException("Error reading weather-rainfall-data:", e);
        }
    }

    public String createDate(String year, String month, String day) {
        String date = year;
        if (month.length() == 1) {
            date = date.concat("0".concat(month));
        } else {
            date = date.concat(month);
        }
        if (day.length() == 1) {
            date = date.concat("0".concat(day));
        } else {
            date = date.concat(day);
        }
        return date;
    }
}
