package io.datasearch.diseasedata.store.dengdipipeline.datastore;

import io.datasearch.diseasedata.store.dengdipipeline.datastore.schema.SimpleFeatureTypeSchema;
import org.geotools.data.DataStore;

import java.util.Map;

/**
 * Pipeline DataStore.
 */
public class PipelineDataStore {
    private DataStore dataStore;
    //Stream handler for handling data
    private Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas;

    public PipelineDataStore(DataStore dataStore, Map<String, SimpleFeatureTypeSchema> schemas) {
        this.dataStore = dataStore;
        simpleFeatureTypeSchemas = schemas;
    }

    public DataStore getDataStore() {
        return this.dataStore;
    }

    public SimpleFeatureTypeSchema getSchema(String featureTypeName) {
        return simpleFeatureTypeSchemas.get(featureTypeName);
    }

    public Map<String, SimpleFeatureTypeSchema> getSchemas() {
        return this.simpleFeatureTypeSchemas;
    }

}
