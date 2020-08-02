package io.datasearch.diseasedata.store.dengdipipeline.ingestion;

import io.datasearch.diseasedata.store.dengdipipeline.ingestion.util.FeatureData;
import io.datasearch.diseasedata.store.schema.SimpleFeatureTypeSchema;
import io.datasearch.diseasedata.store.util.ConfigurationLoader;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * For ingesting data from csv files.
 */
public class DataIngester {
    private static final Logger logger = LoggerFactory.getLogger(DataIngester.class);

    public List<SimpleFeature> createFetures(Map<String, Object> parameters,
                                             Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas) {
        FeatureData fd = new FeatureData(parameters, simpleFeatureTypeSchemas);
        return fd.getFeatureData();
    }

    public Map<String, List<SimpleFeature>> getFeatureData(Map<String, Object> parameters,
                                                           Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas) {
        Map<String, List<SimpleFeature>> featureList = new HashMap<>();
        try {
            List<Map<String, Object>> simpleFeatureTypeList =
                    (List) parameters.get("simple_feature_types");
            for (Map<String, Object> sft : simpleFeatureTypeList) {
                List<SimpleFeature> features = createFetures(sft, simpleFeatureTypeSchemas);
                logger.info((String) sft.get("feature_name"));
                featureList.put((String) sft.get("feature_name"), features);
            }

            return featureList;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Configuration error in data ingester", e);
        }
    }

    public void insertData(DataStore datastore,
                           Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas) throws Exception {

        Map<String, Object> configurations = ConfigurationLoader.getIngestConfigurations();
        Map<String, List<SimpleFeature>> featureList = getFeatureData(configurations, simpleFeatureTypeSchemas);
        for (Map.Entry<String, List<SimpleFeature>> entry : featureList.entrySet()) {
            List<SimpleFeature> features = entry.getValue();
            String featureName = entry.getKey();
            DataStore dataStore = datastore;
            SimpleFeatureSource featureSource = dataStore.getFeatureSource(featureName);
            SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
            logger.info("featureStore from datastore " + featureSource);
            for (SimpleFeature feature : features) {
                SimpleFeatureCollection collection = DataUtilities.collection(feature);
                featureStore.addFeatures(collection);
            }
            logger.info("Add " + features.size() + " new data points into " + entry.getKey());
        }


    }
}
