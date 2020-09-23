package io.datasearch.epidatafuse.core.dengdipipeline.datastore;

import io.datasearch.epidatafuse.core.dengdipipeline.datastore.ingestion.DataIngester;
import io.datasearch.epidatafuse.core.dengdipipeline.datastore.schema.SimpleFeatureTypeSchema;
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
    //Stream handler for handling data
    private Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemaMap;

    private DataIngester dataIngester;

    private static final Logger logger = LoggerFactory.getLogger(PipelineDataStore.class);

    public PipelineDataStore(DataStore dataStore, Map<String, SimpleFeatureTypeSchema> schemaMap) {
        this.dataStore = dataStore;
        simpleFeatureTypeSchemaMap = schemaMap;
        dataIngester = new DataIngester();
    }

    public DataStore getDataStore() {
        return this.dataStore;
    }

    public SimpleFeatureTypeSchema getSchema(String featureTypeName) {
        return simpleFeatureTypeSchemaMap.get(featureTypeName);
    }

    public Map<String, SimpleFeatureTypeSchema> getSchemas() {
        return this.simpleFeatureTypeSchemaMap;
    }

    public void bulkIngest() {
        try {
            this.dataIngester.insertData(this.getDataStore(),
                    this.getSchemas());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void streamingIngest(Event[] events, String featureType) {
        try {
            this.dataIngester.insertStreamingData(this.getDataStore(), this.getSchema(featureType), events);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
