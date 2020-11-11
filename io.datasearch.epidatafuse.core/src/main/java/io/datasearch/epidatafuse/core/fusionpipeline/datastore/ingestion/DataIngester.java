package io.datasearch.epidatafuse.core.fusionpipeline.datastore.ingestion;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.datasearch.epidatafuse.core.util.IngestConfig;
import io.datasearch.epidatafuse.core.util.IngestionConfig;
import io.siddhi.core.event.Event;
import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


/**
 * DataIngester for ingesting data into a datastore.
 */
public class DataIngester {

    private static final Logger logger = LoggerFactory.getLogger(DataIngester.class.getName());
    private SimpleFeatureTransformer simpleFeatureTransformer;
    private EventTransformer eventTransformer;
    private static final String SIMPLE_FEATURE_GENERATION_ERROR = "Error generating simple feature";

    public DataIngester() {
        this.simpleFeatureTransformer = new SimpleFeatureTransformer();
        this.eventTransformer = new EventTransformer();
    }

    public void ingestBulk(DataStore dataStore,
                           Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas,
                           IngestionConfig ingestConfiguration) {

        List<IngestConfig> ingestionConfigList = ingestConfiguration.getFeatureConfigs();
        for (IngestConfig ingestionConfig : ingestionConfigList) {
            ingestBulk(dataStore, ingestionConfig,
                    simpleFeatureTypeSchemas.get(ingestionConfig.getFeatureName()));
        }
    }

    public Boolean ingestBulk(DataStore dataStore, IngestConfig ingestConfig,
                              SimpleFeatureTypeSchema simpleFeatureTypeSchema) {
        try {
            int counter = writeSimpleFeatures(dataStore, simpleFeatureTypeSchema, ingestConfig);
            logger.info("Added " + counter + " new records into " + ingestConfig.getFeatureName());
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    public int writeSimpleFeatures(DataStore dataStore, SimpleFeatureTypeSchema simpleFeatureTypeSchema,
                                   IngestConfig ingestConfig) {
        try {
            int counter = this.simpleFeatureTransformer.transformAndWrite(dataStore,
                    ingestConfig, simpleFeatureTypeSchema);
            return counter;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException(SIMPLE_FEATURE_GENERATION_ERROR, e);
        }
    }


    public void ingestStreamingData(DataStore dataStore,
                                    SimpleFeatureTypeSchema simpleFeatureTypeSchema, Event[] events) {
        try {
            this.eventTransformer.transformAndWriteEvent(events, simpleFeatureTypeSchema, dataStore);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
