package io.datasearch.diseasedata.store.dengdipipeline.datastore.ingestion.util;

import io.datasearch.diseasedata.store.DiseaseDataStore;
import io.datasearch.diseasedata.store.dengdipipeline.datastore.schema.SimpleFeatureTypeSchema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.factory.Hints;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Template class for generating features.
 */
public class FeatureData {

    private List<SimpleFeature> features = null;
    private String typeName;
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataStore.class);

    public FeatureData(Map<String, Object> parameters, Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas) {
        this.typeName = (String) parameters.get("feature_name");
        //buildSimpleFeature((List<Map<String, String>>) parameters.get("attributes"));
        //buildQueries((List<Map<String, String>>) (parameters.get("queries")));
        buildSimpleFeatureData((String) parameters.get("data_source"),
                (Map<String, Integer>) parameters.get("records"),
                simpleFeatureTypeSchemas.get(this.typeName).getConfigurations(),
                simpleFeatureTypeSchemas.get(this.typeName).getAttributes(),
                simpleFeatureTypeSchemas.get(this.typeName).getSimpleFeatureType());
    }

    public List<SimpleFeature> getFeatureData() {
        return features;
    }

    public void buildSimpleFeatureData(String dataSource, Map<String, Integer> records,
                                       Map<String, String> configurations,
                                       List<Map<String, String>> attributes, SimpleFeatureType simpleFeatureType) {
        if (features == null) {

            DateTimeFormatter dateFormat = null;
            List<SimpleFeature> features = new ArrayList<>();
            URL input = getClass().getClassLoader().getResource(dataSource);
            if (input == null) {
                throw new RuntimeException("Couldn't load resource " + dataSource);
            }

            // date parser corresponding to the CSV format
            if (configurations.get("DateTimeFormat") != null) {
                dateFormat = DateTimeFormatter.ofPattern(configurations.get("DateTimeFormat"), Locale.US);
            }

            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(simpleFeatureType);

            // parser corresponding to the CSV format
            try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.EXCEL)) {
                for (CSVRecord record : parser) {
                    try {
                        // pull out the fields corresponding to our simple feature attributes
                        for (Map<String, String> attribute : attributes) {
                            if ("Date".equalsIgnoreCase(attribute.get("attributeType"))) {
                                builder.set(attribute.get("attributeName"),
                                        Date.from(LocalDate.parse(
                                                record.get(
                                                        records.get(attribute.get("attributeName"))),
                                                dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant()));
                            } else if ("Point".equals(attribute.get("attributeType"))) {
                                double latitude = Double.parseDouble(
                                        record.get(records.get("Lat")));
                                double longitude = Double.parseDouble(
                                        record.get(records.get("Long")));
                                builder.set(attribute.get("attributeName"),
                                        "POINT (" + longitude + " " + latitude + ")");
                            } else {
                                builder.set(attribute.get("attributeName"),
                                        record.get(records.get(attribute.get("attributeName"))));
                            }
                        }
                        builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);
                        SimpleFeature feature = builder.buildFeature(
                                record.get(records.get(configurations.get("FeatureID"))));
                        features.add(feature);
                    } catch (Throwable e) {
                        logger.debug("Invalid" + typeName + "record: " + e.toString() + " " + record.toString());
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading" + typeName + ":", e);
            }
            this.features = Collections.unmodifiableList(features);
        }
    }
}
