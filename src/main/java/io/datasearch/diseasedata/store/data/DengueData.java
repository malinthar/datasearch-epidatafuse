package io.datasearch.diseasedata.store.data;

import io.datasearch.diseasedata.store.DiseaseDataStore;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.Query;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *  Dengue Data Feature calss.
 */
public class DengueData implements DiseaseData {

    private SimpleFeatureType sft = null;
    private List<SimpleFeature> features = null;
    // private List<Query> queries = null;
    // private Filter subsetFilter = null;
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataStore.class);

    @Override
    public String getTypeName() {
        return "dengue-data";
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        if (sft == null) {
            logger.info("creating sft");
            StringBuilder attributes = new StringBuilder();
            attributes.append("moh_id:String:index=true,"); // marks this attribute for indexing
            attributes.append("moh_name:String,");
            attributes.append("Latitude:String,");
            attributes.append("Longitude:String,");

            for (int i = 1; i < 53; i++) {
                attributes.append("week" + String.valueOf(i) + ":String,");
            }
            attributes.append("total:String,");
            attributes.append("*geom:Point:srid=4326");

            sft = SimpleFeatureTypes.createType(getTypeName(), attributes.toString());
        }
        return sft;
    }

    @Override
    public List<SimpleFeature> getTestData() {
        if (features == null) {
            List<SimpleFeature> features = new ArrayList<>();

            URL input = getClass().getClassLoader().getResource("dengue-moh.csv");
            if (input == null) {
                throw new RuntimeException("Couldn't load resource dengue-moh.CSV");
            }

            // parser corresponding to the CSV format
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());

            // use a geotools SimpleFeatureBuilder to create our features
            // use apache commons-csv to parse the GDELT file
            try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.EXCEL)) {
                for (CSVRecord record : parser) {
                    try {
                        builder.set("moh_id", record.get(0));
                        builder.set("moh_name", record.get(1));
                        builder.set("Latitude", record.get(2));
                        builder.set("Longitude", record.get(3));
                        double latitude = Double.parseDouble(record.get(2));
                        double longitude = Double.parseDouble(record.get(3));
                        builder.set("geom", "POINT (" + longitude + " " + latitude + ")");

                        for (int i = 4; i < 56; i++) {
                            builder.set("week" + String.valueOf(i - 3), record.get(i));
                        }
                        builder.set("total", record.get(56));
                        builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

                        SimpleFeature feature = builder.buildFeature(record.get(0));
                        features.add(feature);
                    } catch (Throwable e) {
                        logger.debug("Invalid dengue-data record: " + e.toString() + " " + record.toString());
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading dengue-data:", e);
            }
            this.features = Collections.unmodifiableList(features);
        }
        return features;
    }

    @Override
    public List<Query> getTestQueries() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Filter getSubsetFilter() {
        // TODO Auto-generated method stub
        return null;
    }

}
