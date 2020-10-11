package io.datasearch.epidatafuse.core.dengdipipeline.datastore.ingestion;

import io.datasearch.epidatafuse.core.dengdipipeline.datastore.ingestion.util.EventTransformer;
import io.datasearch.epidatafuse.core.dengdipipeline.datastore.ingestion.util.SimpleFeatureTransformer;
import io.datasearch.epidatafuse.core.dengdipipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.datasearch.epidatafuse.core.util.ConfigurationLoader;
import io.siddhi.core.event.Event;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * For ingesting data into a datastore.
 */
public class DataIngester {
    private static final Logger logger = LoggerFactory.getLogger(DataIngester.class.getName());
    private SimpleFeatureTransformer simpleFeatureTransformer = new SimpleFeatureTransformer();
    private EventTransformer eventTransfomer = new EventTransformer();

    //todo: Handling exception?
    public List<SimpleFeature> generateSimpleFeatures(Map<String, Object> configurations,
                                                      SimpleFeatureTypeSchema simpleFeatureTypeSchema) {
        try {
            List<SimpleFeature> features = this.simpleFeatureTransformer.transform(
                    configurations, simpleFeatureTypeSchema);
            return features;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Configuration error in data ingester:", e);
        }
    }

    public void insertData(DataStore dataStore,
                           Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas) throws IOException {

        //todo:get ingestion configurations (use class)
        Map<String, Object> configurations = ConfigurationLoader.getIngestConfigurations();
        List<Map<String, Object>> ingestionConfigList =
                (List) configurations.get("simple_feature_types");

        //todo:remove hard coded things "feature_names and such"
        for (Map<String, Object> ingestionConfig : ingestionConfigList) {
            insertData(dataStore, ingestionConfig, simpleFeatureTypeSchemas.get(
                    ingestionConfig.get("feature_name")));
        }
    }

    public void insertData(DataStore dataStore, Map<String, Object> ingestionConfig
            , SimpleFeatureTypeSchema simpleFeatureTypeSchema) throws IOException {
        List<SimpleFeature> simpleFeatures = generateSimpleFeatures(
                ingestionConfig, simpleFeatureTypeSchema);

        // Find the 'featureSource(GeoMesa)' of this particular feature type from data source
        SimpleFeatureSource featureSource = dataStore.getFeatureSource(
                (String) ingestionConfig.get("feature_name"));
        SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;

        for (SimpleFeature feature : simpleFeatures) {
            SimpleFeatureCollection collection = DataUtilities.collection(feature);
            featureStore.addFeatures(collection);
        }

        logger.info("Added " + simpleFeatures.size() + " new data points into " +
                ingestionConfig.get("feature_name"));
    }

    public void insertStreamingData(DataStore dataStore,
                                    SimpleFeatureTypeSchema simpleFeatureTypeSchema, Event[] events) {
        try {
            List<SimpleFeature> simpleFeatures = this.eventTransfomer.transform(
                    events, simpleFeatureTypeSchema);
            SimpleFeatureSource featureSource = dataStore.getFeatureSource(simpleFeatureTypeSchema.getTypeName());
            SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;

            for (SimpleFeature feature : simpleFeatures) {
                SimpleFeatureCollection collection = DataUtilities.collection(feature);
                featureStore.addFeatures(collection);
            }

            logger.info("Added " + simpleFeatures.size() + " new data points into " +
                    simpleFeatureTypeSchema.getTypeName());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
