package io.datasearch.epidatafuse.core.fusionpipeline.datastore;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.ingestion.DataIngester;
import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.datasearch.epidatafuse.core.util.IngestConfig;
import io.datasearch.epidatafuse.core.util.IngestionConfig;
import io.siddhi.core.event.Event;
import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Pipeline DataStore.
 */
public class PipelineDataStore {
    private DataStore dataStore;
    private Map<String, SimpleFeatureTypeSchema> featureSFTSchemas;
    private Map<String, SimpleFeatureTypeSchema> granularitySFTSchemas;
    private DataIngester dataIngester;

    private static final Logger logger = LoggerFactory.getLogger(PipelineDataStore.class);

    public PipelineDataStore(DataStore dataStore, Map<String,
            SimpleFeatureTypeSchema> featureSFTSchemas, Map<String,
            SimpleFeatureTypeSchema> granularitySFTSchemas) {

        this.dataStore = dataStore;
        this.featureSFTSchemas = featureSFTSchemas;
        this.granularitySFTSchemas = granularitySFTSchemas;
        dataIngester = new DataIngester();
    }

    public DataStore getDataStore() {
        return this.dataStore;
    }

    public void addFeatureSchema(SimpleFeatureTypeSchema schema) {
        this.featureSFTSchemas.put(schema.getSimpleFeatureTypeName(), schema);
    }

    public void addGranularitySchema(SimpleFeatureTypeSchema schema) {
        this.granularitySFTSchemas.put(schema.getSimpleFeatureTypeName(), schema);
    }

    public SimpleFeatureTypeSchema getSchema(String featureTypeName) {
        return featureSFTSchemas.get(featureTypeName);
    }

    public SimpleFeatureTypeSchema getGranularitySchema(String featureTypeName) {
        return granularitySFTSchemas.get(featureTypeName);
    }


    public Map<String, SimpleFeatureTypeSchema> getSchemas() {
        return this.featureSFTSchemas;
    }

    public void bulkIngest(IngestionConfig ingestionConfig) {
        try {
            this.dataIngester.ingestBulk(this.getDataStore(),
                    this.getSchemas(), ingestionConfig);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public Boolean bulkIngest(IngestConfig ingestConfig, String featureType) {
        Boolean status = false;
        try {
            if (getSchema(ingestConfig.getFeatureName()) != null) {
                SimpleFeatureTypeSchema schema = getSchema(ingestConfig.getFeatureName());
                status = this.dataIngester.ingestBulk(this.getDataStore(), ingestConfig, schema);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            status = false;
        }
        return status;
    }

    public Boolean ingestGranules(IngestConfig ingestConfig) {
        Boolean status = false;
        try {
            if (getGranularitySchema(ingestConfig.getFeatureName()) != null) {
                SimpleFeatureTypeSchema schema = getGranularitySchema(ingestConfig.getFeatureName());
                status = this.dataIngester.ingestBulk(this.getDataStore(), ingestConfig, schema);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            status = false;
        }
        return status;
    }

    public void streamingIngest(Event[] events, String featureType) {
        try {
            this.dataIngester.ingestStreamingData(this.getDataStore(), this.getSchema(featureType), events);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
